// Package aof implements the append-only file: a log of every command
// that changed the keyspace, replayed at startup to rebuild it.
//
// A snapshot is a photograph taken now and then, so everything written
// between two of them is lost in a crash. The log is the other approach:
// nothing is ever overwritten, each write is appended as it happens, and
// the data is whatever you get by running the log from the beginning.
// Redis calls the file appendonly.aof and so does this.
//
// Commands are stored in RESP, the same encoding clients use, so the file
// is a recording of the requests the server accepted and needs no format
// of its own. Replay means handing those requests back to the command
// table.
package aof

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// FsyncPolicy decides how often the log is forced onto the physical disk.
// This is the durability dial, and the reason it exists is that fsync is
// expensive: it waits for the drive, which costs hundreds of microseconds
// on an SSD and milliseconds on a spinning disk.
type FsyncPolicy int

const (
	// FsyncEverySecond forces the log to disk once a second. A power cut
	// can lose up to a second of writes. This is Redis's default and a
	// reasonable trade for most uses.
	FsyncEverySecond FsyncPolicy = iota
	// FsyncAlways forces the log to disk before the reply is sent, so a
	// client that got "OK" can rely on it. It is by far the slowest.
	FsyncAlways
	// FsyncNever leaves it to the operating system, which typically
	// writes within half a minute. Fastest, least durable.
	FsyncNever
)

// ParseFsyncPolicy turns a configuration string into a policy, using
// Redis's names.
func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "everysec":
		return FsyncEverySecond, nil
	case "always":
		return FsyncAlways, nil
	case "no":
		return FsyncNever, nil
	}
	return 0, fmt.Errorf("unknown fsync policy %q, want always, everysec or no", s)
}

func (p FsyncPolicy) String() string {
	switch p {
	case FsyncEverySecond:
		return "everysec"
	case FsyncAlways:
		return "always"
	case FsyncNever:
		return "no"
	}
	return "unknown"
}

// syncInterval is how often the background goroutine calls fsync under
// the everysec policy.
const syncInterval = time.Second

// Options configures a Log.
type Options struct {
	// Policy decides how often the log is forced to disk.
	Policy FsyncPolicy
	// Export copies the keyspace, which is what a rewrite is built from.
	// Without it the log cannot be rewritten.
	//
	// It is handed a function to call once the copy is taken, while it is
	// still holding whatever keeps writes out. Calling it switches the log
	// over to buffering, and doing that at exactly this point is what
	// keeps a command from being counted twice: see Log.Rewrite.
	Export func(buffer func()) []store.Record
	// RewritePercentage asks for an automatic rewrite once the file has
	// grown this much beyond its size after the last one. Zero turns
	// automatic rewriting off.
	RewritePercentage int
	// RewriteMinSize keeps the automatic rewrite from firing on a small
	// file, where doubling in size means very little work was done.
	RewriteMinSize int64
	// Logger reports background rewrites. Nil means slog.Default.
	Logger *slog.Logger
}

// Log is an open append-only file.
//
// Data moves through three places on its way to safety, and it is worth
// being precise about which is which, because that is exactly what the
// fsync policy chooses between:
//
//	buf    the log's own memory. Lost if the process dies.
//	write  handed to the operating system. Survives the process dying,
//	       not the power going out.
//	fsync  on the physical disk. Survives everything.
//
// Append only fills buf. Flush performs the write, and the server calls
// it at the same moment it flushes replies to the client, so a client can
// never receive a reply for a command the operating system has not been
// told about. Under the always policy Flush also fsyncs, before those
// replies go out.
type Log struct {
	// mu guards everything below it. The log is a single file, so writers
	// have to take turns; this is the one place in the server where all
	// writes meet.
	mu     sync.Mutex
	f      *os.File
	enc    *resp.Writer
	policy FsyncPolicy
	err    error
	// pending holds commands that have been appended but not yet handed to
	// the operating system. Keeping the buffer here rather than inside the
	// encoder lets a rewrite see what is in it, which it needs when it
	// replaces the file those bytes were queued for.
	pending bytes.Buffer
	// written counts the writes that have reached the operating system and
	// synced counts how many of those an fsync has covered, which is what
	// lets several callers share one fsync. See syncLocked.
	written uint64
	synced  uint64
	syncing bool
	cond    *sync.Cond
	// sync forces the file to the disk. It is a field so tests can count
	// the calls and make them slow on purpose.
	sync func() error

	// path is where the file lives, which a rewrite needs in order to put
	// a new one in its place. counter tracks how much has been written to
	// the current file and base is how big it was after the last rewrite.
	path    string
	counter *countingWriter
	base    int64

	opts Options

	// rewriting is set while a rewrite is building a new file. Commands
	// appended in the meantime go into rewriteBuf as well as into the file
	// being replaced, and are added to the new file just before it takes
	// over.
	rewriting  bool
	rewriteBuf bytes.Buffer
	rewriteWG  sync.WaitGroup
	closed     bool
	// preparing covers the moment between claiming a rewrite and the copy
	// of the keyspace being taken, so two rewrites cannot start at once.
	preparing bool
	// wantRewrite records that the file has outgrown its threshold. The
	// rewrite itself is started by Flush once the lock is free, because
	// taking the copy needs locks this one must be released before.
	wantRewrite bool
	// afterBaseWritten is a test seam: it runs inside a rewrite, after the
	// new file has been written and before it takes over.
	afterBaseWritten func()

	stop chan struct{}
	done chan struct{}
}

// Open opens (or creates) the log at path for appending, with no rewriting.
func Open(path string, policy FsyncPolicy) (*Log, error) {
	return OpenWithOptions(path, Options{Policy: policy})
}

// OpenWithOptions opens the log at path and, if opts.Export is set, allows
// it to be rewritten.
func OpenWithOptions(path string, opts Options) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	size, err := fileSize(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	l := &Log{
		policy: opts.Policy,
		path:   path,
		// Whatever the file already holds counts as the starting point, so
		// a server that restarts does not immediately rewrite a log it just
		// finished replaying.
		base: size,
		opts: opts,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	l.useFile(f)
	l.cond = sync.NewCond(&l.mu)
	l.enc = resp.NewWriter(&l.pending)

	if opts.Policy == FsyncEverySecond {
		go l.syncEverySecond()
	} else {
		close(l.done)
	}
	return l, nil
}

// useFile points the log at f. Writes go through a counter so the log
// always knows how big the file has grown without asking the operating
// system after every command.
func (l *Log) useFile(f *os.File) {
	l.f = f
	l.counter = &countingWriter{w: f}
	l.sync = f.Sync
}

// size is the current length of the file, in bytes.
func (l *Log) size() int64 { return l.base + l.counter.n }

func fileSize(f *os.File) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// countingWriter remembers how many bytes have passed through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// Append records a command. It only fills the buffer: nothing reaches the
// operating system until Flush.
func (l *Log) Append(args []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return
	}
	// Both calls write into memory, so neither can fail.
	start := l.pending.Len()
	l.enc.WriteValue(request(args))
	l.enc.Flush()

	// While a rewrite is running the command goes into both files: the one
	// being replaced, so a crash before the swap loses nothing, and the
	// buffer that will be appended to the new one.
	if l.rewriting {
		l.rewriteBuf.Write(l.pending.Bytes()[start:])
	}
}

// Flush writes everything buffered to the operating system, and waits for
// the disk as well when the policy is always.
func (l *Log) Flush() error {
	l.mu.Lock()
	err := l.flushLocked()
	want := l.wantRewrite
	l.wantRewrite = false
	l.mu.Unlock()

	// An automatic rewrite starts here rather than inside the locked
	// section above, because taking the copy of the keyspace needs locks
	// that must be taken before this one, never after.
	if err == nil && want {
		if rewriteErr := l.Rewrite(); rewriteErr != nil && !errors.Is(rewriteErr, ErrRewriteInProgress) {
			l.opts.Logger.Error("could not start an automatic rewrite", "path", l.path, "err", rewriteErr)
		}
	}
	return err
}

func (l *Log) flushLocked() error {
	if l.err != nil {
		return l.err
	}
	if l.pending.Len() > 0 {
		if _, err := l.counter.Write(l.pending.Bytes()); err != nil {
			// A failed write leaves the file in an unknown state, so the
			// log remembers it and stops pretending to work.
			l.err = err
			return err
		}
		l.pending.Reset()
		l.written++
	}
	if l.policy == FsyncAlways {
		if err := l.syncLocked(); err != nil {
			return err
		}
	}
	l.markRewriteIfTooBigLocked()
	return nil
}

// syncLocked forces everything written so far onto the disk, sharing one
// fsync between everybody waiting for one. It must be called with the lock
// held and returns with it held again.
//
// Without the sharing, the always policy serialises: fifty connections
// that each want their own fsync queue up behind each other and every one
// pays for all the ones ahead of it. But an fsync is not per-caller. It
// flushes the whole file, so the one already running will cover writes
// made before it started. Callers therefore look at what has been synced
// rather than at who synced it: whoever gets there first does the work,
// the rest wait and then find their write already on disk. Databases call
// this group commit.
//
// The lock is released while fsync runs, which is the point: other
// connections keep writing to the buffer meanwhile. The counter is read
// before unlocking, so this fsync only ever claims writes that were
// already handed to the operating system when it started.
func (l *Log) syncLocked() error {
	for l.err == nil && l.synced < l.written {
		if l.syncing {
			l.cond.Wait()
			continue
		}
		l.syncing = true
		target := l.written

		l.mu.Unlock()
		err := l.sync()
		l.mu.Lock()

		l.syncing = false
		switch {
		case err != nil:
			l.err = err
		case target > l.synced:
			l.synced = target
		}
		l.cond.Broadcast()
	}
	return l.err
}

// syncEverySecond is the background half of the everysec policy. Writes
// have already reached the operating system by then; this decides how
// long they may sit in its cache.
func (l *Log) syncEverySecond() {
	defer close(l.done)
	ticker := time.NewTicker(syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			l.mu.Lock()
			l.syncLocked()
			l.mu.Unlock()
		}
	}
}

// Close flushes and fsyncs whatever is left, whatever the policy, and
// closes the file. Shutting down cleanly is the one moment where losing
// buffered writes would be inexcusable.
// Closing twice is allowed and does nothing the second time, so a
// deferred Close costs nothing next to an explicit one.
func (l *Log) Close() error {
	// Marking the log closed first refuses any new rewrite, so the wait
	// below cannot be extended by one that starts in the meantime.
	l.mu.Lock()
	if l.closed {
		defer l.mu.Unlock()
		return l.err
	}
	l.closed = true
	l.mu.Unlock()

	close(l.stop)
	<-l.done
	// Waiting happens without the lock, because a rewrite that is already
	// running needs it to finish swapping its file in.
	l.rewriteWG.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()
	// An fsync started by someone else may still be running, and it is
	// holding the file this is about to close.
	for l.syncing {
		l.cond.Wait()
	}
	l.flushLocked()
	l.syncLocked()
	if err := l.f.Close(); err != nil && l.err == nil {
		l.err = err
	}
	return l.err
}

func request(args []string) resp.Value {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString(a)
	}
	return resp.NewArray(elems...)
}
