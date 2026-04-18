package server

import (
	"bufio"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/kartik/valkyr/resp"
)

// Peer represents a single connected client.
// Each peer runs its own goroutine for reading commands and writing responses.
type Peer struct {
	conn               net.Conn
	reader             *resp.Reader
	writer             *resp.Writer
	server             *Server
	writeMu            sync.Mutex
	subMu              sync.Mutex
	subscribedChannels map[string]bool
	subscribedPatterns map[string]bool
	inTx               bool
	txQueue            [][]resp.Value
}

// NewPeer creates a new Peer for the given connection and server.
func NewPeer(conn net.Conn, server *Server) *Peer {
	return &Peer{
		conn:               conn,
		reader:             resp.NewReader(bufio.NewReader(conn)),
		writer:             resp.NewWriter(bufio.NewWriter(conn)),
		server:             server,
		subscribedChannels: make(map[string]bool),
		subscribedPatterns: make(map[string]bool),
	}
}

// ReadLoop continuously reads RESP commands from the client, dispatches them
// through the router, writes responses, and flushes. Returns when the client
// disconnects or an unrecoverable error occurs.
func (p *Peer) ReadLoop() {
	defer func() {
		// Clean up subscriptions upon client disconnect to avoid memory leaks
		p.server.UnsubscribeAll(p)
		p.conn.Close()
	}()

	for {
		value, err := p.reader.ReadValue()
		if err != nil {
			if err != io.EOF {
				slog.Error("Read error", "err", err, "addr", p.conn.RemoteAddr())
			}
			return
		}

		// Ensure we have an array of arguments
		var args []resp.Value
		switch value.Typ {
		case resp.Array:
			args = value.Array
		default:
			// Single value — wrap as array
			args = []resp.Value{value}
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0].Str)

		// 1. Pub/Sub check
		p.subMu.Lock()
		isSubscribed := len(p.subscribedChannels) > 0 || len(p.subscribedPatterns) > 0
		p.subMu.Unlock()

		if isSubscribed {
			switch cmd {
			case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT":
				// allowed
			default:
				errVal := resp.ErrorValue("ERR only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT are allowed in this context")
				if err := p.WriteAndFlush(errVal); err != nil {
					return
				}
				continue
			}
		}

		// 2. Transaction Check
		if p.inTx && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" {
			// Check if command exists
			if !p.server.router.HasHandler(cmd) {
				errVal := resp.ErrorValue("ERR unknown command '" + cmd + "'")
				if err := p.WriteAndFlush(errVal); err != nil {
					return
				}
				continue
			}
			// Queue command
			p.txQueue = append(p.txQueue, args)
			queuedVal := resp.SimpleStringValue("QUEUED")
			if err := p.WriteAndFlush(queuedVal); err != nil {
				return
			}
			continue
		}

		// Dispatch command
		p.server.IncrCmdCount()
		result := p.server.router.Dispatch(p, args)

		// Write response
		if err := p.WriteAndFlush(result); err != nil {
			return
		}
	}
}

// WriteAndFlush writes a value to the peer's connection safely using writeMu and flushes.
func (p *Peer) WriteAndFlush(val resp.Value) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	if err := p.writer.WriteValue(val); err != nil {
		slog.Error("Write error", "err", err, "addr", p.conn.RemoteAddr())
		return err
	}
	if err := p.writer.Flush(); err != nil {
		slog.Error("Flush error", "err", err, "addr", p.conn.RemoteAddr())
		return err
	}
	return nil
}

// Close closes the underlying TCP connection.
func (p *Peer) Close() {
	p.conn.Close()
}

// RemoteAddr returns the remote address of the peer's connection.
func (p *Peer) RemoteAddr() net.Addr {
	return p.conn.RemoteAddr()
}

// Writer returns the peer's RESP writer for direct writes (used by pub/sub).
func (p *Peer) Writer() *resp.Writer {
	return p.writer
}
