# Valkyr 🐺

> Writing a Redis-compatible key-value store from scratch in Go. Hand-rolled RESP2 parser, concurrent client handling via goroutines, TTL expiry with a min-heap, AOF persistence, Pub/Sub via channels, and support for strings, hashes, lists, and sets.

A Redis clone built without any Redis libraries. Just raw TCP, Go's concurrency primitives, and a lot of careful systems design.

---

## Features

- **RESP2 Protocol** — Full parser and writer for simple strings, errors, integers, bulk strings, and arrays. Inline command fallback for `telnet`/`netcat` clients.
- **42 Commands** — Strings, hashes, lists, sets, and server utility commands.
- **TTL & Key Expiry** — `EXPIRE`, `TTL`, `PERSIST`. Min-heap expiry engine swept every 100ms by a background goroutine.
- **AOF Persistence** — Every write appended to `valkyr.aof`. Replayed on startup to restore full state. Run with `--no-persist` for pure in-memory mode.
- **Pub/Sub** — `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE`. Fan-out via Go channels. Zero message loss.
- **Concurrent Clients** — Each connection runs in its own goroutine. Per-data-type `sync.RWMutex` — readers never block each other.
- **Config File** — `valkyr.conf` with port, bind address, AOF path, maxmemory, loglevel. All overridable via CLI flags.
- **Built-in CLI** — `valkyr-cli` with readline history and tab completion. No `redis-cli` dependency needed.
- **Observability** — `INFO` returns uptime, connected clients, total commands processed, memory usage, and keyspace summary.

---

## Installation

```bash
git clone https://github.com/lande26/valkyr.git
cd valkyr
make build
```

Run the server:

```bash
make run
# or
./bin/valkyr --port 6379
```

Connect with the built-in CLI:

```bash
./bin/valkyr-cli
```

Or with `redis-cli`:

```bash
redis-cli -p 6379
```

---

## Usage

### Strings

```
valkyr:6379> SET foo bar
OK

valkyr:6379> GET foo
"bar"

valkyr:6379> INCR counter
(integer) 1

valkyr:6379> MSET k1 v1 k2 v2
OK

valkyr:6379> MGET k1 k2
1) "v1"
2) "v2"
```

### TTL

```
valkyr:6379> SET session abc
OK

valkyr:6379> EXPIRE session 3600
(integer) 1

valkyr:6379> TTL session
(integer) 3599

valkyr:6379> PERSIST session
(integer) 1
```

### Hashes

```
valkyr:6379> HSET user:1 name Arjun age 22
(integer) 2

valkyr:6379> HGET user:1 name
"Arjun"

valkyr:6379> HGETALL user:1
1) "name"
2) "Arjun"
3) "age"
4) "22"
```

### Lists

```
valkyr:6379> RPUSH tasks "deploy" "review" "test"
(integer) 3

valkyr:6379> LRANGE tasks 0 -1
1) "deploy"
2) "review"
3) "test"

valkyr:6379> LPOP tasks
"deploy"
```

### Sets

```
valkyr:6379> SADD tags "go" "redis" "systems"
(integer) 3

valkyr:6379> SMEMBERS tags
1) "go"
2) "redis"
3) "systems"

valkyr:6379> SISMEMBER tags "go"
(integer) 1
```

### Pub/Sub

```
# Terminal 1
valkyr:6379> SUBSCRIBE alerts
Reading messages...

# Terminal 2
valkyr:6379> PUBLISH alerts "server:ready"
(integer) 1
```

### Server

```
valkyr:6379> INFO
# Server
uptime_seconds:3612
connected_clients:4
total_commands_processed:48291
used_memory_human:2.4mb
# Keyspace
db0:keys=142,expires=18

valkyr:6379> DBSIZE
(integer) 142

valkyr:6379> FLUSHDB
OK
```

---

## Commands

| Category | Commands |
|----------|----------|
| **String** | `SET` `GET` `DEL` `EXISTS` `MSET` `MGET` `INCR` `DECR` `INCRBY` `APPEND` `STRLEN` |
| **Hash** | `HSET` `HGET` `HGETALL` `HDEL` `HLEN` `HKEYS` `HEXISTS` `HMSET` `HMGET` |
| **List** | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` |
| **Set** | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SINTER` `SUNION` `SDIFF` |
| **Key** | `EXPIRE` `TTL` `PERSIST` `EXPIREAT` `KEYS` `TYPE` `RENAME` `RANDOMKEY` |
| **Server** | `PING` `INFO` `DBSIZE` `FLUSHDB` `BGSAVE` `CONFIG` |
| **Pub/Sub** | `SUBSCRIBE` `UNSUBSCRIBE` `PUBLISH` |

---

## Architecture

```
TCP Client 1 ──► Peer goroutine ──► RESP Parser ──┐
TCP Client 2 ──► Peer goroutine ──► RESP Parser ──┤
TCP Client N ──► Peer goroutine ──► RESP Parser ──┴──► Command Router
                                                           │
                                          ┌────────────────┼────────────────┐
                                          ▼                ▼                ▼
                                   String Store      Hash Store       List Store
                                   (RWMutex)         (RWMutex)        (RWMutex)
                                          │
                                   Set Store
                                   (RWMutex)
                                          │
                              ┌───────────┼───────────┐
                              ▼           ▼           ▼
                         TTL Worker   AOF Writer  PubSub Broker
                        (goroutine)  (goroutine)  map[chan][]chan
                        min-heap     buffered     fan-out
                        100ms tick   fsync        Go channels
```

### Concurrency Model

- Each client connection spawns a dedicated goroutine
- Data types have independent `sync.RWMutex` locks — concurrent reads never block
- TTL expiry uses `container/heap`, swept every 100ms by a background goroutine
- AOF writes are buffered and flushed on `BGSAVE` or graceful shutdown
- Pub/Sub subscribers block in a `select` loop, isolated from the command router

---

## Configuration

Create `valkyr.conf` in the project root:

```conf
port        6379
bind        127.0.0.1
loglevel    info
aof-path    valkyr.aof
maxmemory   0
```

All options are overridable via CLI flags:

```bash
./bin/valkyr --port 6380 --no-persist --loglevel debug
```

---

## Development

```bash
make build     # build server + CLI binaries
make run       # run server on default port
make test      # run all unit + integration tests
make bench     # run benchmarks
make lint      # run golangci-lint
make clean     # remove build artifacts
```

### Running Tests

```bash
# Unit tests
go test ./resp/... -v
go test ./store/... -v

# Integration test (spins up real server on random port)
go test ./integration/... -v
```

---

## Acknowledgements

- Inspired by [Redis](https://redis.io)
- Built entirely in Go
