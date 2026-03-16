package server

import (
	"bufio"
	"io"
	"log/slog"
	"net"

	"github.com/kartik/valkyr/resp"
)

// Peer represents a single connected client.
// Each peer runs its own goroutine for reading commands and writing responses.
type Peer struct {
	conn   net.Conn
	reader *resp.Reader
	writer *resp.Writer
	server *Server
}

// NewPeer creates a new Peer for the given connection and server.
func NewPeer(conn net.Conn, server *Server) *Peer {
	return &Peer{
		conn:   conn,
		reader: resp.NewReader(bufio.NewReader(conn)),
		writer: resp.NewWriter(bufio.NewWriter(conn)),
		server: server,
	}
}

// ReadLoop continuously reads RESP commands from the client, dispatches them
// through the router, writes responses, and flushes. Returns when the client
// disconnects or an unrecoverable error occurs.
func (p *Peer) ReadLoop() {
	defer p.conn.Close()

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

		// Dispatch command
		p.server.IncrCmdCount()
		result := p.server.router.Dispatch(args)

		// Write response
		if err := p.writer.WriteValue(result); err != nil {
			slog.Error("Write error", "err", err, "addr", p.conn.RemoteAddr())
			return
		}
		if err := p.writer.Flush(); err != nil {
			slog.Error("Flush error", "err", err, "addr", p.conn.RemoteAddr())
			return
		}
	}
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
