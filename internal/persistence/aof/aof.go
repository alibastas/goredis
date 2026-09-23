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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/alibastas/goredis/internal/resp"
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
	// buffered means Append has written something the operating system has
	// not been handed yet.
	buffered bool
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

	stop chan struct{}
	done chan struct{}
}

// Open opens (or creates) the log at path for appending.
func Open(path string, policy FsyncPolicy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{
		f:      f,
		enc:    resp.NewWriter(f),
		policy: policy,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	l.cond = sync.NewCond(&l.mu)
	l.sync = f.Sync

	if policy == FsyncEverySecond {
		go l.syncEverySecond()
	} else {
		close(l.done)
	}
	return l, nil
}

// Append records a command. It only fills the buffer: nothing reaches the
// operating system until Flush.
func (l *Log) Append(args []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.err != nil {
		return
	}
	// resp.Writer reports write errors from Flush, so there is nothing to
	// check here.
	l.enc.WriteValue(request(args))
	l.buffered = true
}

// Flush writes everything buffered to the operating system, and waits for
// the disk as well when the policy is always.
func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushLocked()
}

func (l *Log) flushLocked() error {
	if l.err != nil {
		return l.err
	}
	if l.buffered {
		if err := l.enc.Flush(); err != nil {
			// A failed write leaves the file in an unknown state, so the
			// log remembers it and stops pretending to work.
			l.err = err
			return err
		}
		l.buffered = false
		l.written++
	}
	if l.policy == FsyncAlways {
		return l.syncLocked()
	}
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
func (l *Log) Close() error {
	close(l.stop)
	<-l.done

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
