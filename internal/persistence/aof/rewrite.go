package aof

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/alibastas/goredis/internal/persistence"
	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

var (
	// ErrRewriteInProgress is returned when a rewrite is asked for while
	// one is already running. The message matches Redis's.
	ErrRewriteInProgress = errors.New("Background append only file rewriting already in progress")
	// ErrRewriteUnavailable is returned by a log that was opened without a
	// way to read the keyspace, which is what a rewrite is built from.
	ErrRewriteUnavailable = errors.New("this log cannot be rewritten")
)

// Rewrite replaces the log with the shortest series of commands that
// rebuilds the current keyspace, in the background. This is BGREWRITEAOF.
//
// The log only ever grows: ten million increments of one counter are ten
// million lines on disk describing a single number. A rewrite turns the
// history back into a recipe, which is both smaller and faster to replay.
//
// It returns as soon as the copy of the keyspace has been taken. The
// writing happens on another goroutine, and failures are logged rather
// than returned, since by then nobody is waiting: the old file is still
// in place and still correct.
//
// The copy is taken by Export rather than here, and Export switches the
// log over to buffering at the moment it has the copy in hand. Both have
// to happen while writes are held out, and the reason is worth spelling
// out. A command changes the keyspace first and is written to the log
// second. If the copy were taken between those two steps, the new file
// would be built from a keyspace that already has the command's effect
// and would then also carry the command itself, which for INCR means
// counting twice on the next replay.
//
// This is also why the copy is not taken while holding the log's own
// lock. Commands reach the log after they have touched the keyspace, so
// waiting for the keyspace while holding the log would be the exact
// reverse of the order they take, and the two would deadlock.
func (l *Log) Rewrite() error {
	l.mu.Lock()
	if err := l.claimRewriteLocked(); err != nil {
		l.mu.Unlock()
		return err
	}
	l.mu.Unlock()

	records := l.opts.Export(l.startBuffering)

	l.rewriteWG.Add(1)
	go func() {
		defer l.rewriteWG.Done()
		if err := l.runRewrite(records); err != nil {
			l.opts.Logger.Error("rewriting the append-only file failed",
				"path", l.path, "err", err)
			l.mu.Lock()
			l.stopRewritingLocked()
			l.mu.Unlock()
		}
	}()
	return nil
}

// claimRewriteLocked reserves the single rewrite slot.
func (l *Log) claimRewriteLocked() error {
	switch {
	case l.opts.Export == nil:
		return ErrRewriteUnavailable
	case l.rewriting || l.preparing:
		return ErrRewriteInProgress
	case l.closed || l.err != nil:
		return ErrRewriteUnavailable
	}
	l.preparing = true
	return nil
}

// startBuffering is handed to Export and called once it holds the copy.
// From here on every command is recorded twice: in the file about to be
// replaced, so a crash before the swap loses nothing, and in the buffer
// that will be appended to the new file.
func (l *Log) startBuffering() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.preparing = false
	l.rewriting = true
	l.rewriteBuf.Reset()
}

// Rewriting reports whether a rewrite is currently running.
func (l *Log) Rewriting() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rewriting
}

func (l *Log) stopRewritingLocked() {
	l.rewriting = false
	// The buffer can have grown large; let it go rather than holding the
	// memory until the next rewrite.
	l.rewriteBuf = bytes.Buffer{}
}

// runRewrite writes the new file and swaps it in.
func (l *Log) runRewrite(records []store.Record) error {
	start := time.Now()

	// The slow part, turning the whole keyspace into commands and writing
	// them out, happens with no lock held. Clients keep writing to the old
	// file meanwhile.
	tmp, err := os.CreateTemp(filepath.Dir(l.path), filepath.Base(l.path)+".rewrite-*")
	if err != nil {
		return err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name()) // a no-op once the rename below has run
	}()

	w := bufio.NewWriter(tmp)
	enc := resp.NewWriter(w)
	for _, args := range Commands(records) {
		if err := enc.WriteValue(request(args)); err != nil {
			return err
		}
	}
	if err := enc.Flush(); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if l.afterBaseWritten != nil {
		// A seam for tests: this is the window in which a rewrite is open
		// and writes are going into the buffer rather than the new file.
		l.afterBaseWritten()
	}

	// From here on the log is locked: the commands that arrived during the
	// rewrite are appended to the new file, it is forced to disk, and it
	// takes over the name. Doing all of that under one lock is what stops a
	// command from slipping in between the last append and the swap.
	l.mu.Lock()
	defer l.mu.Unlock()

	// An fsync started by someone else runs with the lock released, and it
	// is holding the file handle this is about to replace.
	for l.syncing {
		l.cond.Wait()
	}

	if _, err := tmp.Write(l.rewriteBuf.Bytes()); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

	// The file being replaced is closed before the rename, not after.
	// Windows refuses to rename over a file that anyone still has open, so
	// keeping the old handle around until afterwards works on Unix and
	// fails here. Nothing can write in the meantime: this holds the lock.
	l.f.Close()

	if err := os.Rename(tmp.Name(), l.path); err != nil {
		// The old file is still under its own name, so the log can carry
		// on with it once it is opened again.
		l.reopenAfterFailedRename()
		return err
	}
	if err := persistence.SyncDir(filepath.Dir(l.path)); err != nil {
		return err
	}

	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.err = err
		return err
	}
	size, err := fileSize(f)
	if err != nil {
		f.Close()
		l.err = err
		return err
	}
	l.useFile(f)

	// Everything that was waiting to be written went into the rewrite
	// buffer as well, so it is already in the new file.
	l.pending.Reset()
	l.base = size
	l.counter.n = 0
	// The new file was fsynced before the rename, so nothing is pending.
	l.written, l.synced = 0, 0
	l.stopRewritingLocked()

	l.opts.Logger.Info("append-only file rewritten",
		"path", l.path, "keys", len(records), "bytes", size, "took", time.Since(start))
	return nil
}

// markRewriteIfTooBigLocked notes that the file has outgrown the size it
// had after the last rewrite, so Flush can start one once it has let go
// of the lock. Redis's two conditions are used: a
// percentage of growth, and a floor below which rewriting is not worth
// the work, since doubling a small file says nothing about how much
// redundancy it holds.
func (l *Log) markRewriteIfTooBigLocked() {
	if l.opts.RewritePercentage <= 0 || l.rewriting || l.preparing || l.closed || l.opts.Export == nil {
		return
	}
	size := l.size()
	if size < l.opts.RewriteMinSize {
		return
	}
	grown := size - l.base
	if l.base > 0 && grown*100 < l.base*int64(l.opts.RewritePercentage) {
		return
	}
	l.wantRewrite = true
}

// reopenAfterFailedRename puts the log back on the file it was already
// using, after a rewrite closed it and then could not replace it. The
// caller holds the lock.
func (l *Log) reopenAfterFailedRename() {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// Nothing left to write to, so say so rather than dropping
		// commands quietly.
		l.err = err
		return
	}
	size, err := fileSize(f)
	if err != nil {
		f.Close()
		l.err = err
		return
	}
	l.useFile(f)
	// What was waiting to be written is still in pending and will go to
	// this file on the next flush; the counters restart from its size.
	l.base = size
	l.counter.n = 0
}

// SetExport gives the log the function a rewrite copies the keyspace
// with. It is separate from Options because the function has to come
// from the layer that runs commands: only that layer can take the copy at
// a moment when no command is halfway between changing the keyspace and
// being written here.
func (l *Log) SetExport(export func(buffer func()) []store.Record) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.opts.Export = export
}
