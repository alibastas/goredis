# goredis

A Redis-compatible in-memory key-value store written from scratch in Go.

goredis speaks the Redis wire protocol (RESP2), so you can talk to it with the
stock `redis-cli` or any Redis client library. The goal of the project is not
to replace Redis but to rebuild its core ideas step by step — networking,
concurrency, persistence, messaging and replication — and to document the
trade-offs made along the way.

It uses only the Go standard library.

> **Status:** early development. See the [roadmap](#roadmap).

## Quick start

Requires Go 1.23 or newer. Available once Phase 1 lands.

```bash
go run ./cmd/goredis          # listens on :6380
redis-cli -p 6380 PING        # PONG
```

## Roadmap

- [x] **Phase 0:** Project scaffolding
- [ ] **Phase 1:** TCP server and RESP2 protocol (`PING`, `ECHO`, pipelining)
- [ ] **Phase 2a:** Concurrency-safe storage engine, string commands
      (`GET`, `SET` with `EX/PX/NX/XX`, `DEL`, `EXISTS`, `INCR`, `EXPIRE`, `TTL`, `PERSIST`, `KEYS`)
- [ ] **Phase 2b:** Hash, list and set data types
- [ ] **Phase 2c:** Lazy and active key expiry, sharded keyspace, benchmarks
- [ ] **Phase 3:** Persistence: append-only file (AOF) and snapshots
- [ ] **Phase 4:** Pub/Sub
- [ ] **Phase 5:** Leader/follower replication
- [ ] **Phase 6:** Leader election and automatic failover with Raft

## Architecture

```
cmd/goredis/          entry point: flags, config, startup
internal/resp/        RESP2 parser and writer (pure protocol, no I/O policy)
internal/server/      TCP listener, one goroutine per connection, client state
internal/command/     command table and argument validation
internal/store/       storage engine: keyspace, data types, expiry
internal/persistence/ AOF and snapshot
internal/pubsub/      channel and pattern subscriptions
internal/replication/ leader/follower sync
internal/raft/        leader election
test/integration/     end-to-end tests over real TCP connections
```

Dependencies flow in one direction: `server → command → store`. The `resp`
package depends on nothing internal, which keeps every layer testable on its
own.

## Design decisions

This section records the non-obvious choices and why they were made. It grows
with each phase.

### Concurrency model: goroutine per connection, locked keyspace

Redis runs commands on a single-threaded event loop. goroutines are cheap and
the Go runtime multiplexes them onto OS threads, so the idiomatic Go approach
is one goroutine per client connection with a keyspace protected by locks.
The keyspace starts behind a single `sync.RWMutex`. It will later be split
into shards so that unrelated keys don't contend on the same lock. That change
will only be made once a benchmark shows the difference.

### RESP2 only

RESP2 is what `redis-cli` and every client library support by default. RESP3
adds richer types but no new ideas relevant to this project.

### Custom snapshot format

Snapshots use a small versioned binary format with a checksum instead of
Redis's RDB format. RDB compatibility would be a lot of work that teaches
little. What matters here is atomic writes (write to a temp file, fsync,
rename) and detecting corruption.

### Default port 6380

goredis defaults to port 6380 so it can run next to a real Redis instance on
6379.

## License

[MIT](LICENSE)
