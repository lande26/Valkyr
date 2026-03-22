// Valkyr — A production-grade Redis clone in Go.
// This is the entry point that loads configuration, creates all dependencies,
// starts the TCP server, and handles graceful shutdown via OS signals.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	valkyrAOF "github.com/kartik/valkyr/aof"
	"github.com/kartik/valkyr/config"
	"github.com/kartik/valkyr/server"
)

func main() {
	// Load configuration from file and CLI flags
	cfg, err := config.Load("valkyr.conf")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Configure logging
	var logLevel slog.Level
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	slog.SetDefault(slog.New(handler))

	// Create server
	srv := server.NewServer(cfg)

	// Initialize AOF persistence (if enabled)
	var aofFile *valkyrAOF.AOF
	if !cfg.NoPersist {
		aofFile, err = valkyrAOF.New(cfg.AOFPath)
		if err != nil {
			slog.Error("Failed to open AOF file", "err", err)
			os.Exit(1)
		}

		// Replay AOF to restore state (AOF writer is NOT set yet,
		// so replayed commands won't be re-logged to the file)
		if err := aofFile.Replay(srv.DispatchCommand); err != nil {
			slog.Error("AOF replay failed", "err", err)
			os.Exit(1)
		}

		// NOW set the AOF writer so future commands get logged
		srv.SetAOFWriter(aofFile)
		slog.Info("AOF persistence enabled", "path", cfg.AOFPath)
	}

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		slog.Info("Received signal, shutting down...", "signal", sig)

		// Flush AOF before shutting down
		if aofFile != nil {
			if err := aofFile.Close(); err != nil {
				slog.Error("AOF close failed", "err", err)
			}
			slog.Info("AOF flushed to disk")
		}

		srv.Shutdown()
		os.Exit(0)
	}()

	// Start accepting connections (blocks)
	slog.Info("Starting Valkyr",
		"port", cfg.Port,
		"persist", !cfg.NoPersist,
	)
	if err := srv.Start(); err != nil {
		slog.Error("Server error", "err", err)
		os.Exit(1)
	}
}
