// Package aof implements Append-Only File persistence for Valkyr.
// Every write command is logged as raw RESP bytes. On startup, the file
// is replayed to restore state. Buffered writes are flushed on BGSAVE
// or graceful shutdown.
package aof

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/kartik/valkyr/resp"
)

// AOF manages the append-only persistence file.
type AOF struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
}

// New opens (or creates) the AOF file at the given path in append mode
// and returns an AOF instance with a buffered writer.
func New(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("aof: failed to open %s: %w", path, err)
	}
	return &AOF{
		file: f,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Log serializes a command (as a RESP array of bulk strings) and appends
// it to the AOF buffer. This satisfies the server.AOFWriter interface.
func (a *AOF) Log(args []resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Write array header: *<count>\r\n
	fmt.Fprintf(a.buf, "*%d\r\n", len(args))

	// Write each argument as a bulk string: $<len>\r\n<data>\r\n
	for _, arg := range args {
		s := arg.Str
		fmt.Fprintf(a.buf, "$%d\r\n%s\r\n", len(s), s)
	}

	return nil
}

// Replay reads the AOF file from the beginning and re-executes every
// command through the provided dispatch function to restore state.
// The dispatchFn should be the router's Dispatch method — it receives
// the full argument array (including the command name) and returns
// the result (which is discarded during replay).
func (a *AOF) Replay(dispatchFn func(args []resp.Value) resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Seek to beginning for reading
	if _, err := a.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("aof: seek failed: %w", err)
	}

	reader := resp.NewReader(bufio.NewReader(a.file))
	count := 0

	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("aof: replay parse error at command %d: %w", count+1, err)
		}

		// Each entry should be an array of bulk strings
		if val.Typ != resp.Array || len(val.Array) == 0 {
			continue
		}

		// Dispatch the command (result is discarded during replay)
		result := dispatchFn(val.Array)
		if result.Typ == resp.Error {
			slog.Warn("AOF replay command failed",
				"cmd", strings.ToUpper(val.Array[0].Str),
				"err", result.Str,
			)
		}
		count++
	}

	// Seek back to end for future appends
	if _, err := a.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("aof: seek-to-end failed: %w", err)
	}
	// Reset the buffered writer to point at the new file position
	a.buf.Reset(a.file)

	slog.Info("AOF replay complete", "commands", count)
	return nil
}

// Sync flushes the buffered writer and fsyncs the file to disk.
// Called by BGSAVE command and during graceful shutdown.
func (a *AOF) Sync() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.buf.Flush(); err != nil {
		return fmt.Errorf("aof: flush failed: %w", err)
	}
	if err := a.file.Sync(); err != nil {
		return fmt.Errorf("aof: fsync failed: %w", err)
	}
	return nil
}

// Close flushes all pending writes, fsyncs, and closes the AOF file.
func (a *AOF) Close() error {
	if err := a.Sync(); err != nil {
		return err
	}
	return a.file.Close()
}
