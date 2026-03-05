// Package server implements the Valkyr TCP server, peer management,
// and command routing.
package server

import (
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
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
	shutdownCh chan struct{}
	aofWriter  AOFWriter // optional, set after construction
}

// AOFWriter is the interface that the AOF persistence layer must satisfy.
// This allows the server to log write commands without importing the aof package.
type AOFWriter interface {
	Log(args []resp.Value) error
}

// NewServer creates a new Server with the given configuration.
func NewServer(cfg *config.Config) *Server {
	s := &Server{
		cfg:        cfg,
		store:      store.NewStore(),
		peers:      make(map[*Peer]bool),
		startTime:  time.Now(),
		shutdownCh: make(chan struct{}),
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
	return s.router.Dispatch(args)
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
