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

Requires Go 1.23 or newer.

```bash
go run ./cmd/goredis          # listens on 127.0.0.1:6380
redis-cli -p 6380 PING        # PONG
```

Flags: `-addr` sets the listen address, `-debug` enables per-connection logs.

No `redis-cli` at hand? The server also understands inline commands, so
`telnet 127.0.0.1 6380` followed by `PING` works too.

## Development

```bash
go test -race ./...                                              # unit and TCP tests
go test ./internal/resp -run='^$' -fuzz=FuzzReadValue -fuzztime=30s  # fuzz the protocol parser
```

## Roadmap

- [x] **Phase 0:** Project scaffolding
- [x] **Phase 1:** TCP server and RESP2 protocol (`PING`, `ECHO`, pipelining)
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

### Replies are flushed only when the input buffer is empty

Each connection has a buffered reader and a buffered writer. After a
command runs, its reply goes into the write buffer. The buffer is flushed
only when there are no more unread bytes from the client, right before the
server would block waiting for more input. A client that pipelines many
commands in one packet gets all the replies back in a single write, without
any pipelining-specific code.

### Untrusted lengths are never used to preallocate

A bulk string header like `$536870912` is only a claim by the client.
The parser caps lengths (512 MB per bulk string, 1M elements per array,
4 KB per line). It also grows buffers as the data actually arrives
rather than allocating the declared size up front, so a single header
can't make the server reserve half a gigabyte.

### Graceful shutdown

On Ctrl+C or SIGTERM the server closes the listener, closes every client
connection and waits for all connection goroutines to return before
exiting. Once persistence exists, this is the point where buffered data
gets flushed to disk.

### RESP2 only

RESP2 is what `redis-cli` and every client library support by default. RESP3
adds richer types but no new ideas relevant to this project.

### Custom snapshot format

Snapshots use a small versioned binary format with a checksum instead of
Redis's RDB format. RDB compatibility would be a lot of work that teaches
little. What matters here is atomic writes (write to a temp file, fsync,
rename) and detecting corruption.

### Default address 127.0.0.1:6380

goredis defaults to port 6380 so it can run next to a real Redis instance on
6379. Like Redis, it only listens on localhost unless told otherwise,
because there is no authentication.

## License

[MIT](LICENSE)
