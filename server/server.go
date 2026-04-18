// Package server implements the Valkyr TCP server, peer management,
// and command routing.
package server

import (
	"bufio"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kartik/valkyr/config"
	"github.com/kartik/valkyr/resp"
	"github.com/kartik/valkyr/store"
)

// Server is the main Valkyr TCP server. It manages the TCP listener,
// connected peers, command routing, and graceful shutdown.
type Server struct {
	cfg        *config.Config
	ln         net.Listener
	store      *store.Store
	router     *Router
	peers      map[*Peer]bool
	peersMu    sync.Mutex
	startTime  time.Time
	cmdCount   int64
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
	aofWriter    AOFWriter // optional, set after construction

	// Pub/Sub fields
	pubsubMu       sync.RWMutex
	pubsubChannels map[string]map[*Peer]bool
	pubsubPatterns map[string]map[*Peer]bool
}

// AOFWriter is the interface that the AOF persistence layer must satisfy.
type AOFWriter interface {
	Log(args []resp.Value) error
	StartRewrite()
	FinalizeRewrite(tempPath string) error
}

// NewServer creates a new Server with the given configuration.
func NewServer(cfg *config.Config) *Server {
	s := &Server{
		cfg:            cfg,
		store:          store.NewStore(),
		peers:          make(map[*Peer]bool),
		startTime:      time.Now(),
		shutdownCh:     make(chan struct{}),
		pubsubChannels: make(map[string]map[*Peer]bool),
		pubsubPatterns: make(map[string]map[*Peer]bool),
	}
	s.router = NewRouter(s)
	return s
}

// Store returns the server's data store.
func (s *Server) Store() *store.Store {
	return s.store
}

// DispatchCommand dispatches a command through the router.
// Used by AOF replay to re-execute persisted commands on startup.
func (s *Server) DispatchCommand(args []resp.Value) resp.Value {
	return s.router.Dispatch(nil, args)
}

// SetAOFWriter sets the AOF persistence writer. Must be called before Start.
func (s *Server) SetAOFWriter(w AOFWriter) {
	s.aofWriter = w
}

// LogToAOF writes a command to the AOF file if persistence is enabled.
func (s *Server) LogToAOF(args []resp.Value) {
	if s.aofWriter != nil {
		if err := s.aofWriter.Log(args); err != nil {
			slog.Error("AOF write failed", "err", err)
		}
	}
}

// SyncAOF flushes the AOF buffer and fsyncs to disk.
// Returns an error if persistence is not enabled or the sync fails.
func (s *Server) SyncAOF() error {
	if s.aofWriter == nil {
		return fmt.Errorf("persistence is disabled")
	}
	if syncer, ok := s.aofWriter.(interface{ Sync() error }); ok {
		return syncer.Sync()
	}
	return fmt.Errorf("AOF writer does not support sync")
}

// Start begins listening for TCP connections and starts the TTL sweeper.
// It blocks until the server is shut down.
func (s *Server) Start() error {
	// Start TTL sweeper
	s.store.TTL.StartSweeper()

	ln, err := net.Listen("tcp", s.cfg.ListenAddr())
	if err != nil {
		return fmt.Errorf("server: failed to listen on %s: %w", s.cfg.ListenAddr(), err)
	}
	s.ln = ln

	slog.Info("Valkyr server started",
		"addr", s.cfg.ListenAddr(),
		"pid", os.Getpid(),
	)

	return s.acceptLoop()
}

// acceptLoop continuously accepts new TCP connections until shutdown.
func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			select {
			case <-s.shutdownCh:
				return nil // graceful shutdown
			default:
				slog.Error("Accept error", "err", err)
				continue
			}
		}
		go s.handleConn(conn)
	}
}

// handleConn creates a new Peer for the connection and starts its read loop.
func (s *Server) handleConn(conn net.Conn) {
	peer := NewPeer(conn, s)
	s.addPeer(peer)
	slog.Info("Client connected",
		"addr", conn.RemoteAddr(),
		"clients", s.ConnectedClients(),
	)

	peer.ReadLoop()

	s.removePeer(peer)
	slog.Info("Client disconnected",
		"addr", conn.RemoteAddr(),
		"clients", s.ConnectedClients(),
	)
}

// addPeer registers a peer in the server's peer set.
func (s *Server) addPeer(p *Peer) {
	s.peersMu.Lock()
	s.peers[p] = true
	s.peersMu.Unlock()
}

// removePeer removes a peer from the server's peer set.
func (s *Server) removePeer(p *Peer) {
	s.peersMu.Lock()
	delete(s.peers, p)
	s.peersMu.Unlock()
}

// IncrCmdCount atomically increments the total commands processed counter.
func (s *Server) IncrCmdCount() {
	atomic.AddInt64(&s.cmdCount, 1)
}

// ConnectedClients returns the current number of connected clients.
func (s *Server) ConnectedClients() int {
	s.peersMu.Lock()
	n := len(s.peers)
	s.peersMu.Unlock()
	return n
}

// Shutdown gracefully shuts down the server: closes the listener,
// disconnects all clients, stops the TTL sweeper.
func (s *Server) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.shutdownCh)
		if s.ln != nil {
			s.ln.Close()
		}
		s.store.TTL.StopSweeper()

		// Close all peer connections
		s.peersMu.Lock()
		for peer := range s.peers {
			peer.Close()
		}
		s.peersMu.Unlock()

		slog.Info("Valkyr server shut down")
	})
}

// CheckAndEvictMemory inspects memory limits and performs eviction if necessary.
func (s *Server) CheckAndEvictMemory() resp.Value {
	if s.cfg.MaxMemory <= 0 {
		return resp.Value{}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	used := int64(m.Alloc)

	if used < s.cfg.MaxMemory {
		return resp.Value{}
	}

	if s.cfg.MaxMemoryPolicy == "noeviction" {
		return resp.ErrorValue("OOM command not allowed when used memory > 'maxmemory'")
	}

	// Try to evict keys
	for i := 0; i < 50; i++ {
		evicted := s.store.Evict(s.cfg.MaxMemoryPolicy, 1)
		if evicted == 0 {
			break // Nothing left to evict
		}
		runtime.ReadMemStats(&m)
		if int64(m.Alloc) < s.cfg.MaxMemory {
			return resp.Value{} // Successfully cleared enough memory!
		}
	}

	// Double check memory
	runtime.ReadMemStats(&m)
	if int64(m.Alloc) >= s.cfg.MaxMemory {
		return resp.ErrorValue("OOM command not allowed when used memory > 'maxmemory'")
	}

	return resp.Value{}
}

// GenerateSnapshotCommands returns a point-in-time list of commands representing the full database state.
func (s *Server) GenerateSnapshotCommands() [][]resp.Value {
	keys := s.store.AllKeys()
	var cmds [][]resp.Value

	for _, key := range keys {
		t := s.store.KeyType(key)
		switch t {
		case "string":
			val, ok := s.store.Strings.Get(key)
			if ok {
				cmds = append(cmds, []resp.Value{
					resp.BulkStringValue("SET"),
					resp.BulkStringValue(key),
					resp.BulkStringValue(val),
				})
			}
		case "hash":
			h := s.store.Hashes.HGetAll(key)
			if len(h) > 0 {
				args := []resp.Value{
					resp.BulkStringValue("HSET"),
					resp.BulkStringValue(key),
				}
				for f, v := range h {
					args = append(args, resp.BulkStringValue(f), resp.BulkStringValue(v))
				}
				cmds = append(cmds, args)
			}
		case "list":
			elems := s.store.Lists.LRange(key, 0, -1)
			if len(elems) > 0 {
				args := []resp.Value{
					resp.BulkStringValue("RPUSH"),
					resp.BulkStringValue(key),
				}
				for _, e := range elems {
					args = append(args, resp.BulkStringValue(e))
				}
				cmds = append(cmds, args)
			}
		case "set":
			mems := s.store.Sets.SMembers(key)
			if len(mems) > 0 {
				args := []resp.Value{
					resp.BulkStringValue("SADD"),
					resp.BulkStringValue(key),
				}
				for _, m := range mems {
					args = append(args, resp.BulkStringValue(m))
				}
				cmds = append(cmds, args)
			}
		case "zset":
			mems := s.store.ZSets.ZRange(key, 0, -1)
			if len(mems) > 0 {
				args := []resp.Value{
					resp.BulkStringValue("ZADD"),
					resp.BulkStringValue(key),
				}
				for _, m := range mems {
					scoreStr := fmt.Sprintf("%g", m.Score)
					args = append(args, resp.BulkStringValue(scoreStr), resp.BulkStringValue(m.Member))
				}
				cmds = append(cmds, args)
			}
		}

		// TTL check
		if deadline, ok := s.store.TTL.GetDeadline(key); ok {
			cmds = append(cmds, []resp.Value{
				resp.BulkStringValue("PEXPIREAT"),
				resp.BulkStringValue(key),
				resp.BulkStringValue(strconv.FormatInt(deadline, 10)),
			})
		}
	}
	return cmds
}

// BGRewriteAOF starts a background AOF rewrite/compaction.
func (s *Server) BGRewriteAOF() error {
	if s.aofWriter == nil {
		return fmt.Errorf("persistence is disabled")
	}

	s.aofWriter.StartRewrite()

	// Generate snapshot
	cmds := s.GenerateSnapshotCommands()

	// Spawn background goroutine to write snapshot to temp file
	go func() {
		tempPath := s.cfg.AOFPath + ".tmp"
		f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			slog.Error("BGRewriteAOF: failed to create temp file", "err", err)
			return
		}
		buf := bufio.NewWriter(f)

		for _, args := range cmds {
			fmt.Fprintf(buf, "*%d\r\n", len(args))
			for _, arg := range args {
				s := arg.Str
				fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(s), s)
			}
		}

		if err := buf.Flush(); err != nil {
			f.Close()
			slog.Error("BGRewriteAOF: failed to flush temp file", "err", err)
			return
		}
		f.Close()

		// Finalize
		if err := s.aofWriter.FinalizeRewrite(tempPath); err != nil {
			slog.Error("BGRewriteAOF: failed to finalize rewrite", "err", err)
			return
		}

		slog.Info("AOF rewrite/compaction complete", "path", s.cfg.AOFPath)
	}()

	return nil
}

// Info returns the formatted INFO string for the INFO command.
func (s *Server) Info() string {
	var sb strings.Builder

	uptime := int64(time.Since(s.startTime).Seconds())
	cmdCount := atomic.LoadInt64(&s.cmdCount)

	// Memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	usedMB := float64(m.Alloc) / 1024 / 1024

	sb.WriteString("# Server\r\n")
	sb.WriteString(fmt.Sprintf("valkyr_version:1.0.0\r\n"))
	sb.WriteString(fmt.Sprintf("tcp_port:%d\r\n", s.cfg.Port))
	sb.WriteString(fmt.Sprintf("uptime_in_seconds:%d\r\n", uptime))
	sb.WriteString(fmt.Sprintf("executable:%s\r\n", os.Args[0]))
	sb.WriteString(fmt.Sprintf("config_file:%s\r\n", filepath.Base(s.cfg.AOFPath)))

	sb.WriteString("\r\n# Clients\r\n")
	sb.WriteString(fmt.Sprintf("connected_clients:%d\r\n", s.ConnectedClients()))

	sb.WriteString("\r\n# Stats\r\n")
	sb.WriteString(fmt.Sprintf("total_commands_processed:%d\r\n", cmdCount))

	sb.WriteString("\r\n# Memory\r\n")
	sb.WriteString(fmt.Sprintf("used_memory_human:%.2fM\r\n", usedMB))

	sb.WriteString("\r\n# Keyspace\r\n")
	dbSize := s.store.DBSize()
	expires := s.store.TTL.ExpiresCount()
	if dbSize > 0 {
		sb.WriteString(fmt.Sprintf("db0:keys=%d,expires=%d,avg_ttl=0\r\n", dbSize, expires))
	}

	return sb.String()
}

// RandomKey returns a random key from the store, or empty string if empty.
func (s *Server) RandomKey() string {
	keys := s.store.AllKeys()
	if len(keys) == 0 {
		return ""
	}
	return keys[rand.Intn(len(keys))]
}

// Subscribe subscribes the peer to the specified channels.
func (s *Server) Subscribe(p *Peer, channels []string) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	p.subMu.Lock()
	defer p.subMu.Unlock()

	for _, ch := range channels {
		chLower := strings.ToLower(ch)
		peers, ok := s.pubsubChannels[chLower]
		if !ok {
			peers = make(map[*Peer]bool)
			s.pubsubChannels[chLower] = peers
		}
		peers[p] = true

		p.subscribedChannels[chLower] = true

		subCount := len(p.subscribedChannels) + len(p.subscribedPatterns)
		p.WriteAndFlush(resp.ArrayValue([]resp.Value{
			resp.BulkStringValue("subscribe"),
			resp.BulkStringValue(ch),
			resp.IntegerValue(int64(subCount)),
		}))
	}
}

// Unsubscribe unsubscribes the peer from specified channels.
// If channels list is empty, unsubscribes from all channels.
func (s *Server) Unsubscribe(p *Peer, channels []string) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	p.subMu.Lock()
	defer p.subMu.Unlock()

	if len(channels) == 0 {
		for ch := range p.subscribedChannels {
			s.unsubscribePeerFromChannel(p, ch)
		}
		if len(p.subscribedChannels) == 0 && len(p.subscribedPatterns) == 0 {
			p.WriteAndFlush(resp.ArrayValue([]resp.Value{
				resp.BulkStringValue("unsubscribe"),
				resp.NullValue(),
				resp.IntegerValue(0),
			}))
		}
		return
	}

	for _, ch := range channels {
		chLower := strings.ToLower(ch)
		s.unsubscribePeerFromChannel(p, chLower)
	}
}

func (s *Server) unsubscribePeerFromChannel(p *Peer, ch string) {
	if _, exists := p.subscribedChannels[ch]; exists {
		delete(p.subscribedChannels, ch)
		if peers, ok := s.pubsubChannels[ch]; ok {
			delete(peers, p)
			if len(peers) == 0 {
				delete(s.pubsubChannels, ch)
			}
		}
	}
	subCount := len(p.subscribedChannels) + len(p.subscribedPatterns)
	p.WriteAndFlush(resp.ArrayValue([]resp.Value{
		resp.BulkStringValue("unsubscribe"),
		resp.BulkStringValue(ch),
		resp.IntegerValue(int64(subCount)),
	}))
}

// PSubscribe subscribes the peer to specified glob patterns.
func (s *Server) PSubscribe(p *Peer, patterns []string) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	p.subMu.Lock()
	defer p.subMu.Unlock()

	for _, pattern := range patterns {
		patLower := strings.ToLower(pattern)
		peers, ok := s.pubsubPatterns[patLower]
		if !ok {
			peers = make(map[*Peer]bool)
			s.pubsubPatterns[patLower] = peers
		}
		peers[p] = true

		p.subscribedPatterns[patLower] = true

		subCount := len(p.subscribedChannels) + len(p.subscribedPatterns)
		p.WriteAndFlush(resp.ArrayValue([]resp.Value{
			resp.BulkStringValue("psubscribe"),
			resp.BulkStringValue(pattern),
			resp.IntegerValue(int64(subCount)),
		}))
	}
}

// PUnsubscribe unsubscribes the peer from specified patterns.
// If patterns is empty, unsubscribes from all patterns.
func (s *Server) PUnsubscribe(p *Peer, patterns []string) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	p.subMu.Lock()
	defer p.subMu.Unlock()

	if len(patterns) == 0 {
		for pat := range p.subscribedPatterns {
			s.punsubscribePeerFromPattern(p, pat)
		}
		if len(p.subscribedChannels) == 0 && len(p.subscribedPatterns) == 0 {
			p.WriteAndFlush(resp.ArrayValue([]resp.Value{
				resp.BulkStringValue("punsubscribe"),
				resp.NullValue(),
				resp.IntegerValue(0),
			}))
		}
		return
	}

	for _, pat := range patterns {
		patLower := strings.ToLower(pat)
		s.punsubscribePeerFromPattern(p, patLower)
	}
}

func (s *Server) punsubscribePeerFromPattern(p *Peer, pat string) {
	if _, exists := p.subscribedPatterns[pat]; exists {
		delete(p.subscribedPatterns, pat)
		if peers, ok := s.pubsubPatterns[pat]; ok {
			delete(peers, p)
			if len(peers) == 0 {
				delete(s.pubsubPatterns, pat)
			}
		}
	}
	subCount := len(p.subscribedChannels) + len(p.subscribedPatterns)
	p.WriteAndFlush(resp.ArrayValue([]resp.Value{
		resp.BulkStringValue("punsubscribe"),
		resp.BulkStringValue(pat),
		resp.IntegerValue(int64(subCount)),
	}))
}

// UnsubscribeAll is called on client disconnect to clean up all registrations.
func (s *Server) UnsubscribeAll(p *Peer) {
	s.pubsubMu.Lock()
	defer s.pubsubMu.Unlock()

	p.subMu.Lock()
	defer p.subMu.Unlock()

	for ch := range p.subscribedChannels {
		if peers, ok := s.pubsubChannels[ch]; ok {
			delete(peers, p)
			if len(peers) == 0 {
				delete(s.pubsubChannels, ch)
			}
		}
	}
	for pat := range p.subscribedPatterns {
		if peers, ok := s.pubsubPatterns[pat]; ok {
			delete(peers, p)
			if len(peers) == 0 {
				delete(s.pubsubPatterns, pat)
			}
		}
	}
}

// Publish sends message to all subscribers of a channel and matching patterns.
func (s *Server) Publish(channel string, message string) int {
	s.pubsubMu.RLock()
	defer s.pubsubMu.RUnlock()

	chLower := strings.ToLower(channel)
	receivers := make(map[*Peer]bool)

	// Direct subscribers
	if peers, ok := s.pubsubChannels[chLower]; ok {
		for p := range peers {
			receivers[p] = true
			p.WriteAndFlush(resp.ArrayValue([]resp.Value{
				resp.BulkStringValue("message"),
				resp.BulkStringValue(channel),
				resp.BulkStringValue(message),
			}))
		}
	}

	// Pattern subscribers
	for pattern, peers := range s.pubsubPatterns {
		if matchGlob(pattern, chLower) {
			for p := range peers {
				if !receivers[p] {
					receivers[p] = true
					p.WriteAndFlush(resp.ArrayValue([]resp.Value{
						resp.BulkStringValue("pmessage"),
						resp.BulkStringValue(pattern),
						resp.BulkStringValue(channel),
						resp.BulkStringValue(message),
					}))
				}
			}
		}
	}

	return len(receivers)
}
