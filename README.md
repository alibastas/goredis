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

Start the server:

```bash
go run ./cmd/goredis          # listens on 127.0.0.1:6380
```

Talk to it with the bundled client, which works on any OS without
installing Redis:

```
$ go run ./cmd/goredis-cli
127.0.0.1:6380> SET greeting "hello world" EX 60
OK
127.0.0.1:6380> GET greeting
"hello world"
127.0.0.1:6380> TTL greeting
(integer) 60
```

`goredis-cli GET greeting` runs a single command and exits. The official
`redis-cli -p 6380` works as well.

Server flags: `-addr` sets the listen address, `-debug` enables
per-connection logs, `-shards` sets the number of keyspace shards.
Client flags: `-h` host, `-p` port.

Measure a running server with the bundled load generator, a small
`redis-benchmark` look-alike:

```bash
go run ./cmd/goredis-benchmark -c 50 -n 200000 -P 16 -t set,get,incr,lpush
```

## Supported commands

| Group      | Commands |
|------------|----------|
| Connection | `PING`, `ECHO` |
| Strings    | `GET`, `SET [NX\|XX] [EX s\|PX ms\|KEEPTTL]`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Keyspace   | `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `TYPE`, `DBSIZE`, `FLUSHDB` |
| Hashes     | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN` |
| Lists      | `LPUSH`, `RPUSH`, `LPOP [count]`, `RPOP [count]`, `LRANGE`, `LINDEX`, `LLEN` |
| Sets       | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |

Replies and error messages match Redis, so existing clients behave the
same way against goredis.

## Development

```bash
go test -race ./...                                              # unit and TCP tests
go test ./internal/resp -run='^$' -fuzz=FuzzReadValue -fuzztime=30s  # fuzz the protocol parser
go test ./internal/store -run='^$' -bench=Store -benchtime=2s        # shard-count benchmark
```

## Roadmap

- [x] **Phase 0:** Project scaffolding
- [x] **Phase 1:** TCP server and RESP2 protocol (`PING`, `ECHO`, pipelining)
- [x] **Phase 2a:** Concurrency-safe storage engine, string and keyspace commands
- [x] **Phase 2b:** Hash, list and set data types
- [x] **Phase 2c:** Active key expiry, sharded keyspace, benchmarks
- [ ] **Phase 3:** Persistence: append-only file (AOF) and snapshots
- [ ] **Phase 4:** Pub/Sub
- [ ] **Phase 5:** Leader/follower replication
- [ ] **Phase 6:** Leader election and automatic failover with Raft

## Architecture

```
cmd/goredis/          server entry point: flags, config, startup
cmd/goredis-cli/      interactive command-line client
cmd/goredis-benchmark/ load generator reporting throughput and latency
internal/resp/        RESP2 parser and writer (pure protocol, no I/O policy)
internal/server/      TCP listener, one goroutine per connection, client state
internal/command/     command table and argument validation
internal/store/       storage engine: keyspace, data types, expiry
internal/deque/       generic ring-buffer deque backing lists
internal/glob/        Redis-style glob matching for KEYS
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

### A sharded keyspace, justified by measurement

The keyspace started out as one map behind one `sync.RWMutex`. It is now
split into 256 shards, each with its own lock. A key's shard is picked with
`hash/maphash`, whose seed is random on every start. With a fixed hash,
a client could craft keys that all land in one shard and bring back the
contention (hash flooding).

The change was only made after measuring it. `BenchmarkStore` hits the
store from 20 goroutines on a 20-thread i7-13700H. The old single-lock
store and the new one were run interleaved, three rounds each:

| Workload                | 1 global lock  | 256 shards     |
|-------------------------|----------------|----------------|
| 90% GET / 10% SET       | 510–573 ns/op  | 125–137 ns/op  |
| 50% GET / 50% SET       | 806–917 ns/op  | 185–204 ns/op  |

That is about 4x on both workloads. Reads benefit too, even though an
`RWMutex` lets readers in together: every `RLock` still updates a shared
counter, and 20 cores fighting over that one cache line is its own
bottleneck. With 256 shards, that counter is split 256 ways as well.

End to end, over TCP with `goredis-benchmark` (50 clients, 200k requests,
median of five runs, thousands of requests per second):

| Command | Pipeline | 1 shard | 256 shards |
|---------|---------:|--------:|-----------:|
| SET     | 16       | 261k    | **663k**   |
| INCR    | 16       | 250k    | **870k**   |
| GET     | 16       | 864k    | 888k       |
| LPUSH   | 16       | 445k    | 413k       |
| SET     | 1        | 138k    | 133k       |
| GET     | 1        | 145k    | 143k       |

This is what the design predicts. Writes gain 2.5–3.5x once pipelining
takes network round trips out of the picture. GET barely moves because
readers already shared the lock. `LPUSH` gains nothing because every client
pushes to the same list, and a single hot key always lives in a single
shard. Without pipelining, each request pays a full loopback round trip,
which dwarfs the time spent holding a lock, so sharding makes no
measurable difference there.

Two lessons came out of this. First, a single benchmark run is not
evidence: the first single-lock measurement came out 2x faster than the
same code measured an hour later, so the numbers above come from
interleaved runs. Second, latency percentiles from this Windows machine
are not reported: Go's clock there advances in steps of about 0.4 ms,
so sub-millisecond latencies all read as zero.

### Multi-key commands lock shards in a fixed order

`DEL a b c` must be atomic, so it locks every shard holding one of its
keys before touching any. If two commands locked their shards in the
order their keys were given, `DEL a c` and `DEL c a` could each grab one
shard and wait forever for the other. Every multi-key operation therefore
locks shards in ascending index order, which rules out that cycle. A test
runs 16 goroutines issuing overlapping multi-key commands in random key
order. When the sort is removed from the locking code, the test hangs
and fails, so it really catches the problem.

### Active expiry by random sampling

Lazy expiry alone leaks memory: keys that expire but are never read again
(sessions of users who never come back) would stay in RAM forever.
Scanning every key would stall the server. goredis uses Redis's
approach instead. Ten times a second it samples 20 keys that have a TTL
in each shard and deletes the expired ones. If more than a quarter of a
sample was expired, it samples that shard again. Each cycle has a 25 ms
time budget, and the next cycle resumes where the last one stopped.

The work scales with the mess: an idle keyspace costs almost nothing,
while a burst of expirations gets cleaned aggressively. Each shard keeps
a separate index of keys that have a TTL (Redis's `expires` dict), so
sampling never wastes time on keys that can't expire. The sample itself
comes from ranging over that index, because Go starts every map iteration
at a random position.

### One keyspace, typed values

Every key maps to an entry whose value is a small interface implemented
by four types: string, hash (`map[string]string`), set
(`map[string]struct{}`) and list. Commands reach the concrete type
through a single generic helper:

```go
h, found, err := as[hashValue](s.lookup(key, now))
```

The helper turns "key missing" into `found == false` and "key holds
another type" into `ErrWrongType`. Each of the ~20 type-specific
operations therefore handles WRONGTYPE in one line instead of repeating
the type switch. Like in Redis, removing the last element of a
collection removes the key itself, and `SET` replaces a value of any
type.

### Lists are ring buffers, not linked lists

A list must support cheap pushes and pops at both ends (`LPUSH`, `RPOP`)
and cheap indexing (`LINDEX`, `LRANGE`). A plain slice makes `LPUSH`
O(n) because every element has to shift. A linked list (`container/list`)
allocates a node per element and makes `LINDEX` O(n). `internal/deque`
is a generic ring buffer: elements sit in a circular slice, and pushing
to the front only moves a head index, so pushes, pops and indexing are
all O(1). The buffer doubles when full and halves when it drops to a
quarter full, which keeps memory bounded without resizing back and
forth around a boundary. It is tested against a plain slice with 20,000
random operations.

### Lazy expiry that never blocks readers

Each key stores an optional deadline. Once the deadline has passed, every
command treats the key as missing. Reads (`GET`, `EXISTS`, `TTL`) only take
the read side of their shard's `RWMutex`, so they skip expired keys without
deleting them. That way concurrent readers never wait for each other.
Writes already hold the exclusive lock, so they delete any expired key they
come across. Keys that nobody touches again are removed by active expiry.

### Atomic read-modify-write

`INCR` reads the value, adds to it and writes it back while holding the
write lock the whole time. Two clients incrementing the same key can't
interleave and lose an update. A test fires 4,000 concurrent `INCR`s over
TCP and checks the total.

### Time is injected

The store reads the time through a `func() time.Time` it receives at
construction. Production code passes `time.Now`, tests pass a fake clock
they move forward by hand. Expiry tests therefore run instantly and never
flake, with no `time.Sleep` anywhere.

### A linear-time glob matcher

`KEYS` (and later `PSUBSCRIBE`) patterns use a small hand-written matcher
instead of `path.Match`, whose `*` stops at `/`. A naive recursive matcher
takes exponential time on patterns like `*a*a*a*a*b`, so this one
remembers the last `*` and retries from there, which keeps it at
O(pattern × input). A test checks this with a pathological pattern.

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
