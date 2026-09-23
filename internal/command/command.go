// Package command maps incoming requests to the functions that implement
// them. It checks the request shape and argument count so individual
// handlers only have to deal with well-formed input.
package command

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// Handler runs one command. args holds the arguments after the command
// name, so for "ECHO hello" it is []string{"hello"}.
type Handler func(args []string) resp.Value

type command struct {
	handler Handler
	// arity follows the Redis convention: it counts the command name too,
	// a positive value means exactly that many and a negative value means
	// at least -arity. PING has arity -1, ECHO has arity 2.
	arity int
	// changesData marks the commands that modify the keyspace. Only those
	// are written to the append-only file; replaying a GET would be a
	// waste of space and time.
	changesData bool
	// rewrite turns a command into the form that should be logged instead
	// of the one the client sent, and may return nil to log nothing. It
	// exists because a relative timeout is only meaningful at the moment
	// it is given: a log full of "expire in 60 seconds" would hand every
	// key another minute of life on every replay.
	rewrite func(args []string, now time.Time) []string
}

// write marks a command as one that changes the keyspace.
func (c *command) write() *command {
	c.changesData = true
	return c
}

// writeAs marks a command as a write and gives it a different shape in
// the log.
func (c *command) writeAs(rewrite func(args []string, now time.Time) []string) *command {
	c.changesData = true
	c.rewrite = rewrite
	return c
}

func (c command) acceptsArgCount(n int) bool {
	if c.arity >= 0 {
		return n == c.arity
	}
	return n >= -c.arity
}

// Registry holds every command the server understands, keyed by its
// upper-case name.
type Registry struct {
	commands map[string]*command
	db       *store.Store
	// log is nil unless the server runs with an append-only file.
	log AppendOnly
	// writeLock puts write commands in a single order when there is a log.
	// It is not taken at all without one. See runWrite.
	writeLock sync.Mutex
}

// AppendOnly is the part of the append-only file the command table uses.
// As with Persister, the interface lives here so the dependency runs from
// the command package to the persistence one and not back again.
type AppendOnly interface {
	// Append records a command that changed the keyspace. It buffers, so
	// nothing reaches the operating system until Flush.
	Append(args []string)
	// Flush hands the buffered commands to the operating system, and
	// waits for the disk too if the configured policy says so.
	Flush() error
	// Rewrite replaces the log with the shortest series of commands that
	// rebuilds the keyspace as it is now, in the background.
	Rewrite() error
}

// Option configures a Registry. Options are Go's usual answer to a
// constructor that keeps growing: the required argument stays positional,
// anything optional is a function the caller passes in, and existing call
// sites keep compiling when a new one is added.
type Option func(*options)

type options struct {
	persister Persister
	log       AppendOnly
}

// WithPersistence enables the SAVE, BGSAVE and LASTSAVE commands. Without
// it they report that persistence is disabled.
func WithPersistence(p Persister) Option {
	return func(o *options) { o.persister = p }
}

// WithAppendOnly logs every command that changes the keyspace to l.
func WithAppendOnly(l AppendOnly) Option {
	return func(o *options) { o.log = l }
}

// NewRegistry creates a registry whose data commands operate on db.
func NewRegistry(db *store.Store, opts ...Option) *Registry {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}

	r := &Registry{commands: make(map[string]*command), db: db, log: cfg.log}
	h := &handlers{db: db, persister: cfg.persister, log: cfg.log}

	// Connection
	r.register("PING", -1, ping)
	r.register("ECHO", 2, echo)

	// Strings
	r.register("GET", 2, h.get)
	r.register("SET", -3, h.set).writeAs(rewriteSet)
	r.register("INCR", 2, h.incr).write()
	r.register("DECR", 2, h.decr).write()
	r.register("INCRBY", 3, h.incrBy).write()
	r.register("DECRBY", 3, h.decrBy).write()

	// Keyspace
	r.register("DEL", -2, h.del).write()
	r.register("EXISTS", -2, h.exists)
	r.register("EXPIRE", 3, h.expire).writeAs(rewriteExpire(time.Second))
	r.register("PEXPIRE", 3, h.pexpire).writeAs(rewriteExpire(time.Millisecond))
	r.register("EXPIREAT", 3, h.expireAt).writeAs(rewriteExpireAt(time.Second))
	r.register("PEXPIREAT", 3, h.pexpireAt).writeAs(rewriteExpireAt(time.Millisecond))
	r.register("TTL", 2, h.ttl)
	r.register("PTTL", 2, h.pttl)
	r.register("PERSIST", 2, h.persist).write()
	r.register("KEYS", 2, h.keys)
	r.register("DBSIZE", 1, h.dbsize)
	r.register("FLUSHDB", 1, h.flushdb).write()
	r.register("TYPE", 2, h.typeOf)

	// Hashes
	r.register("HSET", -4, h.hset).write()
	r.register("HGET", 3, h.hget)
	r.register("HDEL", -3, h.hdel).write()
	r.register("HGETALL", 2, h.hgetall)
	r.register("HEXISTS", 3, h.hexists)
	r.register("HLEN", 2, h.hlen)

	// Lists
	r.register("LPUSH", -3, h.lpush).write()
	r.register("RPUSH", -3, h.rpush).write()
	r.register("LPOP", -2, h.lpop).write()
	r.register("RPOP", -2, h.rpop).write()
	r.register("LRANGE", 4, h.lrange)
	r.register("LINDEX", 3, h.lindex)
	r.register("LLEN", 2, h.llen)

	// Sets
	r.register("SADD", -3, h.sadd).write()
	r.register("SREM", -3, h.srem).write()
	r.register("SMEMBERS", 2, h.smembers)
	r.register("SISMEMBER", 3, h.sismember)
	r.register("SCARD", 2, h.scard)

	// Persistence
	r.register("SAVE", 1, h.save)
	r.register("BGSAVE", 1, h.bgsave)
	r.register("LASTSAVE", 1, h.lastsave)
	r.register("BGREWRITEAOF", 1, h.bgrewriteaof)
	return r
}

// handlers groups the commands that need access to the store. Its methods
// are registered as Handlers: a method value like h.get is an ordinary
// func(args []string) resp.Value with h already bound to it.
type handlers struct {
	db *store.Store
	// persister is nil when the server runs without persistence.
	persister Persister
	// log is nil when the server runs without an append-only file.
	log AppendOnly
}

func (r *Registry) register(name string, arity int, h Handler) *command {
	c := &command{handler: h, arity: arity}
	r.commands[name] = c
	return c
}

// Dispatch runs the command described by req and returns its reply.
// Problems with the request itself (unknown command, wrong number of
// arguments) are reported as RESP errors rather than Go errors, since they
// are normal replies from the client's point of view.
func (r *Registry) Dispatch(req resp.Value) resp.Value {
	args, err := requestArgs(req)
	if err != nil {
		return resp.NewError("ERR " + err.Error())
	}
	return r.DispatchArgs(args)
}

// DispatchArgs is Dispatch for a command that is already split into its
// arguments, which is the shape the append-only file replays them in.
func (r *Registry) DispatchArgs(args []string) resp.Value {
	name := strings.ToUpper(args[0])
	cmd, ok := r.commands[name]
	if !ok {
		return unknownCommand(args)
	}
	if !cmd.acceptsArgCount(len(args)) {
		return wrongArgCount(name)
	}

	if cmd.changesData && r.log != nil {
		return r.runWrite(cmd, args)
	}
	return cmd.handler(args[1:])
}

// runWrite executes a command that changes data and records it in the
// append-only file, with both halves under one lock.
//
// The lock is what makes the log a faithful account of the keyspace, and
// it is only taken when there is a log to write to. Without it the two
// halves are ordered independently: the keyspace orders them by its shard
// locks and the log by its own, so two clients pushing to the same list at
// the same time can be applied in one order and written down in the other.
// Nothing is lost that way, but a restart would quietly reorder the list,
// and a command whose effect a rewrite's copy already holds could be
// written to the new log as well and counted twice.
//
// The cost is real: write commands no longer run in parallel with each
// other while the log is on, which measures at 1.3x to 1.7x depending on
// pipelining. Reads never take this lock and are unaffected, and a server
// without a log never takes it at all. The README has the numbers.
func (r *Registry) runWrite(cmd *command, args []string) resp.Value {
	r.writeLock.Lock()
	defer r.writeLock.Unlock()

	reply := cmd.handler(args[1:])
	r.appendToLog(cmd, args, reply)
	return reply
}

// ExportForRewrite copies the keyspace for a log rewrite and calls buffer
// once it has the copy, while still holding the write lock.
//
// Taking that lock is what puts the copy at a definite point in the log:
// every write command is either entirely before it, and so already
// written down, or entirely after it, and so in the rewrite's buffer.
// Nothing can be in both, and nothing can fall between them.
func (r *Registry) ExportForRewrite(buffer func()) []store.Record {
	r.writeLock.Lock()
	defer r.writeLock.Unlock()

	records := r.db.Export()
	buffer()
	return records
}

// appendToLog records a command in the append-only file, after it ran.
//
// Logging afterwards, and only when the reply is not an error, means the
// log holds commands that the server actually accepted. A write that was
// skipped on purpose, such as a SET NX on a key that already exists, is
// still logged: it replays to the same nothing, and leaving the decision
// to the replay is simpler than trying to guess here whether the
// keyspace really changed.
func (r *Registry) appendToLog(cmd *command, args []string, reply resp.Value) {
	if r.log == nil || !cmd.changesData || reply.Type == resp.Error {
		return
	}
	logged := args
	if cmd.rewrite != nil {
		logged = cmd.rewrite(args, r.db.Now())
	}
	if logged != nil {
		r.log.Append(logged)
	}
}

// Flush pushes everything the commands have written to the append-only
// file out to the operating system. The server calls it once a batch of
// pipelined commands is done, right before it sends the replies, so no
// client ever sees a reply for a write the log has not been told about.
func (r *Registry) Flush() error {
	if r.log == nil {
		return nil
	}
	return r.log.Flush()
}

// Replay applies one command read back from the append-only file. An
// error reply means the log does not match this server, which is worth
// stopping for rather than starting up with part of the data.
func (r *Registry) Replay(args []string) error {
	if reply := r.DispatchArgs(args); reply.Type == resp.Error {
		return errors.New(reply.Str)
	}
	return nil
}

// requestArgs checks that req has the shape clients use for commands, a
// non-empty array of bulk strings, and returns those strings.
func requestArgs(req resp.Value) ([]string, error) {
	if req.Type != resp.Array || req.Null || len(req.Array) == 0 {
		return nil, errors.New("expected a non-empty array of bulk strings")
	}
	args := make([]string, len(req.Array))
	for i, v := range req.Array {
		if v.Type != resp.BulkString || v.Null {
			return nil, errors.New("expected a non-empty array of bulk strings")
		}
		args[i] = v.Str
	}
	return args, nil
}

// maxArgInError limits how much of a client's input is echoed back in an
// error message, so a huge argument doesn't turn into a huge reply.
const maxArgInError = 128

func unknownCommand(args []string) resp.Value {
	var b strings.Builder
	fmt.Fprintf(&b, "ERR unknown command '%s', with args beginning with: ", truncate(args[0]))
	for _, arg := range args[1:] {
		fmt.Fprintf(&b, "'%s' ", truncate(arg))
	}
	return resp.NewError(b.String())
}

// Replies shared by many commands.
var (
	okReply      = resp.NewSimpleString("OK")
	syntaxError  = resp.NewError("ERR syntax error")
	notAnInteger = resp.NewError("ERR " + store.ErrNotInteger.Error())
)

// errorReply turns an error from the store into a RESP error. Redis
// prefixes generic errors with "ERR"; WRONGTYPE errors carry their own
// prefix, which clients check for.
func errorReply(err error) resp.Value {
	if errors.Is(err, store.ErrWrongType) {
		return resp.NewError(err.Error())
	}
	return resp.NewError("ERR " + err.Error())
}

// stringsReply turns a slice of strings into an array of bulk strings.
func stringsReply(items []string) resp.Value {
	elems := make([]resp.Value, len(items))
	for i, s := range items {
		elems[i] = resp.NewBulkString(s)
	}
	return resp.Value{Type: resp.Array, Array: elems}
}

// intReply wraps the (count, error) results most store methods return.
func intReply(n int, err error) resp.Value {
	if err != nil {
		return errorReply(err)
	}
	return resp.NewInteger(int64(n))
}

func invalidExpireTime(cmd string) resp.Value {
	return resp.NewError(fmt.Sprintf("ERR invalid expire time in '%s' command", cmd))
}

func wrongArgCount(name string) resp.Value {
	return resp.NewError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
}

func truncate(s string) string {
	if len(s) > maxArgInError {
		return s[:maxArgInError]
	}
	return s
}
