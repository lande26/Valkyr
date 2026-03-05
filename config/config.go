// Package config handles loading and merging configuration for the Valkyr server
// from config files and CLI flags.
package config

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config holds all Valkyr server configuration options.
type Config struct {
	Port      int    // TCP port to listen on (default: 6379)
	Bind      string // Address to bind to (default: "0.0.0.0")
	AOFPath   string // Path to the AOF persistence file (default: "valkyr.aof")
	LogLevel  string // Logging level: debug, info, warn, error (default: "info")
	NoPersist bool   // If true, disable AOF persistence entirely
}

// DefaultConfig returns a Config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		Port:      6379,
		Bind:      "0.0.0.0",
		AOFPath:   "valkyr.aof",
		LogLevel:  "info",
		NoPersist: false,
	}
}

// Load reads configuration from the given file path (if it exists),
// then overrides with CLI flags. CLI flags take precedence over file values.
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	// Try to load from config file
	if configPath != "" {
		if err := cfg.loadFromFile(configPath); err != nil {
			// Only fail if the file was explicitly provided and doesn't exist
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("config: failed to load %s: %w", configPath, err)
			}
		}
	}

	// Override with CLI flags
	cfg.loadFromFlags()

	return cfg, nil
}

// loadFromFile parses a key-value config file (Redis-style format).
// Lines starting with # are comments. Format: key value
func (c *Config) loadFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		key := strings.ToLower(parts[0])
		value := parts[1]

		switch key {
		case "port":
			if p, err := strconv.Atoi(value); err == nil {
				c.Port = p
			}
		case "bind":
			c.Bind = value
		case "aof-path":
			c.AOFPath = value
		case "loglevel":
			c.LogLevel = strings.ToLower(value)
		case "no-persist":
			c.NoPersist = strings.ToLower(value) == "yes" || value == "1" || strings.ToLower(value) == "true"
		}
	}
	return scanner.Err()
}

// loadFromFlags parses command-line flags and overrides config values.
func (c *Config) loadFromFlags() {
	port := flag.Int("port", 0, "TCP port to listen on")
	bind := flag.String("bind", "", "Address to bind to")
	aofPath := flag.String("aof-path", "", "Path to AOF persistence file")
	logLevel := flag.String("loglevel", "", "Log level: debug, info, warn, error")
	noPersist := flag.Bool("no-persist", false, "Disable AOF persistence")

	flag.Parse()

	if *port != 0 {
		c.Port = *port
	}
	if *bind != "" {
		c.Bind = *bind
	}
	if *aofPath != "" {
		c.AOFPath = *aofPath
	}
	if *logLevel != "" {
		c.LogLevel = *logLevel
	}
	if *noPersist {
		c.NoPersist = true
	}
}

// ListenAddr returns the full address string for net.Listen (e.g., "0.0.0.0:6379").
func (c *Config) ListenAddr() string {
	return fmt.Sprintf("%s:%d", c.Bind, c.Port)
}
