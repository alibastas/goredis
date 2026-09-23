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

Data survives a restart. There are two ways to keep it, and they make
different promises.

A **snapshot** is a copy of the whole keyspace, written on shutdown, on
`SAVE` and on `BGSAVE`, and loaded again at startup:

```
127.0.0.1:6380> BGSAVE
Background saving started
                                  # restart the server
127.0.0.1:6380> GET greeting
"hello world"
127.0.0.1:6380> TTL greeting
(integer) 41
```

Expiry times are stored as absolute deadlines, so a key that had 60
seconds left before a 19-second downtime comes back with 41, not 60.

Everything written between two snapshots is lost in a crash. The
**append-only file** closes that gap by logging every write as it happens
and replaying the log at startup:

```bash
go run ./cmd/goredis -appendonly -appendfsync everysec
```

`-appendfsync` picks how much a crash may cost: `always` waits for the
disk before answering the client, `everysec` risks the last second, `no`
leaves it to the operating system. See
[the design notes](#the-append-only-file-a-log-of-every-write) for what
each one actually guarantees.

Server flags: `-addr` sets the listen address, `-debug` enables
per-connection logs, `-shards` sets the number of keyspace shards,
`-dir` says where the data files live, `-dbfilename` and
`-appendfilename` name them, `-snapshot=false` turns snapshots off and
`-appendonly` turns the log on.
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
| Strings    | `GET`, `SET [NX\|XX] [EX s\|PX ms\|EXAT ts\|PXAT ms\|KEEPTTL]`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Keyspace   | `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `TYPE`, `DBSIZE`, `FLUSHDB` |
| Hashes     | `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN` |
| Lists      | `LPUSH`, `RPUSH`, `LPOP [count]`, `RPOP [count]`, `LRANGE`, `LINDEX`, `LLEN` |
| Sets       | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD` |
| Persistence| `SAVE`, `BGSAVE`, `LASTSAVE` |

Replies and error messages match Redis, so existing clients behave the
same way against goredis.

## Development

```bash
go test -race ./...                                              # unit and TCP tests
go test ./internal/resp -run='^$' -fuzz=FuzzReadValue -fuzztime=30s  # fuzz the protocol parser
go test ./internal/store -run='^$' -bench=Store -benchtime=2s        # shard-count benchmark
go test ./internal/persistence/snapshot -run='^$' -fuzz=FuzzDecode   # fuzz the snapshot parser
```

## Roadmap

- [x] **Phase 0:** Project scaffolding
- [x] **Phase 1:** TCP server and RESP2 protocol (`PING`, `ECHO`, pipelining)
- [x] **Phase 2a:** Concurrency-safe storage engine, string and keyspace commands
- [x] **Phase 2b:** Hash, list and set data types
- [x] **Phase 2c:** Active key expiry, sharded keyspace, benchmarks
- [x] **Phase 3a:** Snapshots: `SAVE`, `BGSAVE`, crash-safe file replacement
- [x] **Phase 3b:** Append-only file with selectable fsync policies
- [ ] **Phase 3c:** AOF rewrite
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
internal/persistence/ crash-safe file replacement shared by the two below
internal/persistence/snapshot/ point-in-time dump of the whole keyspace
internal/persistence/aof/  append-only command log
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
exiting. Only then, with nothing else touching the keyspace, does it wait
for any background save to finish and write a final snapshot, and flush
and fsync the append-only file whatever policy it is running under. A
clean shutdown is the one moment where losing buffered writes would be
inexcusable, so `no` and `everysec` sync there too.

### RESP2 only

RESP2 is what `redis-cli` and every client library support by default. RESP3
adds richer types but no new ideas relevant to this project.

### The append-only file: a log of every write

A snapshot loses everything written since it was taken. The append-only
file is the other half of the answer: every command that changed the
keyspace is appended to a file as it happens, and replaying that file
from the start rebuilds the keyspace. Nothing in it is ever overwritten,
which is what makes it cheap to write and possible to repair.

Commands are stored in RESP, the same encoding clients speak, so the file
needs no format of its own and the existing parser reads it back. It is
also readable with `head`, which is worth something when debugging:

```
*3\r\n$3\r\nSET\r\n$4\r\nisim\r\n$10\r\nali bastas\r\n
```

Only commands that change data are logged, and only after they ran,
which means the file holds what the server actually accepted rather than
what clients asked for. A write that was refused, say `LPUSH` on a
string, never reaches it. A write that was skipped on purpose, such as
`SET ... NX` on an existing key, is logged anyway: it replays to the same
nothing, and letting the replay decide is simpler and safer than trying
to judge here whether the keyspace really changed.

### Timeouts are logged as deadlines, not durations

`SET k v EX 60` cannot be logged as written. Replayed three days later it
would give the key another minute of life, and a key that should have
died long ago would come back on every restart. Every timeout is
therefore rewritten to an absolute moment before it is logged, which is
what Redis does too:

```
SET k v EX 60   ->  SET k v PXAT 1790110408000
EXPIRE k 60     ->  PEXPIREAT k 1790110408000
```

That required implementing `EXPIREAT`, `PEXPIREAT` and `SET ... EXAT|PXAT`,
which are Redis commands anyway. It also pays for itself twice: because
deadlines are absolute, keys that expire during normal operation do not
have to be logged at all. Redis writes an explicit `DEL` when a key
expires; here the replay simply does not load a key whose deadline has
passed.

Deadlines beyond the year 9999 are refused, so every expiry the server
accepts is one that fits in a snapshot and cannot overflow time
arithmetic later.

### fsync is the dial, and `write` is not `fsync`

`write` returning successfully does not mean the data is on the disk. It
means the kernel has a copy in its page cache and will get to it. The
data lives in three places on its way to safety, and the `appendfsync`
setting picks which one the server waits for:

| Where the data is | Survives `kill -9` | Survives power loss |
|---|---|---|
| goredis's own buffer | no | no |
| written, in the OS page cache | yes | no |
| fsynced, on the disk | yes | yes |

`always` fsyncs before the reply is sent, `everysec` fsyncs once a second
in the background, `no` never fsyncs and lets the operating system
decide.

Buffered commands are handed to the operating system at the exact moment
the server flushes replies to the client, which it already does when
there is nothing left to read from the connection. That point does double
duty: a client can never receive a reply for a write the kernel has not
been told about, and a pipeline of sixteen commands costs one `write`
rather than sixteen.

### Group commit: fifty clients, one fsync

Under `always` the first implementation was catastrophic. Each connection
took the log's lock, fsynced, and released it, so fifty concurrent
clients queued up and every one paid for all the ones ahead of it:

| appendfsync | Throughput (SET, 50 clients, no pipelining) |
|---|---|
| off (no log)    | 229k req/s |
| `no`            | 104k req/s |
| `everysec`      |  89k req/s |
| `always`, one fsync per client | **2.6k req/s** |
| `always`, shared fsync | **16.5k req/s** |

The fix is that an fsync is not per-caller. It flushes the whole file, so
an fsync already in flight will cover every write made before it started.
Callers therefore wait on *what has been synced* rather than on a turn to
sync: a counter records how many writes have reached the operating system
and how many of those an fsync has covered, whoever arrives first does
the work while the rest wait on a `sync.Cond`, and they all wake up to
find their write already on the disk. Databases call this group commit.
It is six times faster here and gives away nothing: when `Flush` returns,
an fsync that included that write has completed.

The lock is released while the fsync runs, so other connections keep
filling the buffer meanwhile. The counter is read before unlocking, which
is what keeps the bookkeeping honest: an fsync only ever claims writes
that were already handed to the kernel when it began.

Numbers with pipelining, where the log costs relatively more because the
network no longer dominates: 1071k without the log, 382k with
`everysec`, 106k with `always`.

### A broken last command is normal; a broken middle one is not

Writing to a file is not atomic, so a crash can land in the middle of a
command and leave a fragment at the end of the log. Startup treats that
as expected: it replays everything complete, logs a warning, and
truncates the file to the last command boundary so the next append does
not glue itself onto half a command.

Damage anywhere earlier is refused and the server does not start. The
reason is the same as for a corrupt snapshot: a server that starts with a
hole in its data will happily write that state back out as the truth.

Finding the truncation point is the fiddly part. The parser reads ahead
into a buffer, so the file offset is well past the last command that was
actually consumed. The loader counts the bytes it pulls from the file and
subtracts what is still sitting in the reader's buffer. A test replays a
log long enough to fill that buffer many times to make sure the
arithmetic holds.

The log is also read with inline commands disabled. The normal parser
accepts `PING\r\n` typed over telnet, which is a feature for clients and
a liability for a file: it would turn a line of damage into a plausible
command instead of reporting it.

### The log replays through a command table with no log attached

Replaying means running the commands again, which the command table
already knows how to do. But the live table appends to the log, so
replaying through it would write every command straight back into the
file it came from. Startup therefore builds a second table over the same
store with no log attached, and hands its `Replay` method to the loader.

The loader does not import the command package at all. It takes a
`func(args []string) error` instead, because the command package has to
know about the log in order to append to it, and two packages importing
each other is not allowed in Go.

### One source of truth at startup

With the log enabled it is the only thing loaded; the snapshot is
ignored. Mixing the two means reasoning about which stretch of time each
file covers, and getting it wrong resurrects deleted keys. Redis behaves
the same way. `SAVE` and `BGSAVE` keep working either way, so a snapshot
is still available as a compact backup.

### Writing a file a crash cannot catch halfway

Writing a snapshot straight over the previous one has a window in which
neither version is intact: if the machine loses power mid-write, the only
copy on disk is half old and half new. Every save therefore goes through
the same four steps, in `internal/persistence`:

```
write  -> dump.goredis.tmp-1234
fsync  -> the bytes are really on the disk, not just in the OS cache
rename -> the temp file becomes dump.goredis in one indivisible step
fsync  -> the directory, so the rename itself survives a power cut
```

Renaming is atomic in the operating system while writing is not, so at
every instant the real name points at one complete file. The first fsync
is what makes that guarantee real: without it the rename can land while
the contents are still sitting in the kernel's write cache, which is how
people end up with a snapshot full of zeroes after a power cut. The temp
file has to be in the same directory, because a rename across file
systems is a copy and not atomic. Directory fsync is a no-op on Windows,
where directories cannot be opened for syncing, so that one function has
two build-tagged versions.

### BGSAVE without fork: copy under the lock, write outside it

Redis takes a background snapshot by calling `fork()`. The child gets a
frozen view of memory that the kernel keeps cheap through copy-on-write,
and writes it out while the parent keeps serving. Go cannot do this: the
runtime's threads do not survive a fork, so a forked child of a Go
program is not allowed to do much more than `exec`.

The equivalent here splits the work differently. `Store.Export` copies the
keyspace while holding every shard's read lock, and `BGSAVE` hands that
copy to a goroutine which does the slow part, the disk, with no locks
held at all. Writers wait for the copy, not for the write; readers never
wait. The price is memory: while the save runs, the copy lives next to
the real keyspace. Only the maps and slices are duplicated, not the
strings inside them, because strings in Go are immutable and can be
shared safely.

`SAVE` is the same thing without the goroutine, and a mutex makes sure
only one save is in flight, so two of them can't race to replace the same
file.

### A snapshot format of its own, with a checksum

Snapshots use a small versioned binary format rather than Redis's RDB.
Byte-level RDB compatibility is a large amount of work that teaches
little; the parts worth building are elsewhere.

```
"GOREDIS" | version | record... | 0x00 | crc64 (8 bytes)
```

A record is a kind byte, an expiry, a key and a payload, with every
length written as a varint so the common small ones cost one byte. The
trailing CRC-64 covers everything before it, and a file whose checksum,
magic or version does not match is refused rather than loaded. Starting
with silently incomplete data is worse than not starting: the server
would then happily save that damaged state back over the good one.

Expiry is stored as an absolute Unix timestamp, not as the remaining
time. A key with 60 seconds left that is reloaded 19 seconds later has
41 seconds left, not 60 again, and a key whose deadline passed while the
server was down is dropped on load instead of coming back from the dead.

The decoder treats the file as untrusted input, exactly like the RESP
parser treats a client: lengths are capped and never used to preallocate,
so a corrupt header claiming a billion elements fails instead of
reserving the memory first. `FuzzDecode` runs in CI to keep it that way.

### Default address 127.0.0.1:6380

goredis defaults to port 6380 so it can run next to a real Redis instance on
6379. Like Redis, it only listens on localhost unless told otherwise,
because there is no authentication.

## License

[MIT](LICENSE)
