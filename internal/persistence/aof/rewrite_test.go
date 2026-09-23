package aof

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/store"
)

// openRewritable opens a log whose keyspace is whatever export returns, so
// a test can control exactly what a rewrite is built from.
func openRewritable(t *testing.T, export func(buffer func()) []store.Record, opts Options) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	opts.Export = export
	opts.Logger = discardLogger()
	l, err := OpenWithOptions(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

// oneKey stands in for a keyspace holding a single string. The buffer
// callback is what a real export calls once it has the copy, and the log
// relies on it being called, so the fake has to call it too.
func oneKey(key, value string) func(func()) []store.Record {
	return func(buffer func()) []store.Record {
		records := []store.Record{{Key: key, Kind: store.KindString, Str: value}}
		buffer()
		return records
	}
}

// waitForRewrite blocks until no rewrite is running. Rewrites finish on
// their own goroutine, so a test has to wait for one rather than assume it
// is done.
func waitForRewrite(t *testing.T, l *Log) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for l.Rewriting() {
		if time.Now().After(deadline) {
			t.Fatal("the rewrite did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRewriteCollapsesHistory is the whole point: the log records how the
// keyspace was reached, a rewrite records only where it ended up.
func TestRewriteCollapsesHistory(t *testing.T) {
	l, path := openRewritable(t, oneKey("counter", "1000"), Options{Policy: FsyncNever})

	// A thousand increments that between them describe one number.
	for range 1000 {
		l.Append([]string{"INCR", "counter"})
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	before := sizeOf(t, path)

	if err := l.Rewrite(); err != nil {
		t.Fatal(err)
	}
	waitForRewrite(t, l)

	if after := sizeOf(t, path); after >= before {
		t.Errorf("the file is %d bytes after rewriting and was %d before", after, before)
	}
	if got := joinCommands(readBack(t, path)); len(got) != 1 || got[0] != "SET counter 1000" {
		t.Fatalf("the rewritten log holds %q, want the single SET", got)
	}
}

// TestRewriteKeepsWritesThatArriveDuringIt covers the part of a rewrite
// that is easy to get wrong. The new file is built from a copy taken when
// the rewrite started, so a command that lands while it runs is in neither
// the copy nor the new file unless it is carried over on purpose.
func TestRewriteKeepsWritesThatArriveDuringIt(t *testing.T) {
	l, path := openRewritable(t, oneKey("old", "1"), Options{Policy: FsyncNever})

	// The hook runs inside the rewrite, after the new file has been
	// written and before it takes over: exactly the window where a lost
	// write would go unnoticed.
	l.afterBaseWritten = func() {
		for i := range 200 {
			l.Append([]string{"SET", "during" + strconv.Itoa(i), "x"})
			if err := l.Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	}

	if err := l.Rewrite(); err != nil {
		t.Fatal(err)
	}
	waitForRewrite(t, l)

	got := joinCommands(readBack(t, path))
	if len(got) != 201 {
		t.Fatalf("the log holds %d commands, want the base plus 200 writes:\n%s",
			len(got), strings.Join(got, "\n"))
	}
	if got[0] != "SET old 1" {
		t.Fatalf("the log starts with %q, want the copied keyspace first", got[0])
	}
	for i := range 200 {
		if want := "SET during" + strconv.Itoa(i) + " x"; got[i+1] != want {
			t.Fatalf("command %d is %q, want %q", i+1, got[i+1], want)
		}
	}
}

// TestRewriteUnderConcurrentWrites does the same under real contention,
// with the race detector watching.
func TestRewriteUnderConcurrentWrites(t *testing.T) {
	l, path := openRewritable(t, oneKey("base", "1"), Options{Policy: FsyncNever})

	const writers, each = 8, 100
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				l.Append([]string{"SET", "k" + strconv.Itoa(w) + "-" + strconv.Itoa(i), "v"})
				if err := l.Flush(); err != nil {
					t.Error(err)
					return
				}
				if w == 0 && i == each/2 {
					if err := l.Rewrite(); err != nil && !errors.Is(err, ErrRewriteInProgress) {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	waitForRewrite(t, l)

	// Writes made once the rewrite is over have to land in the new file.
	for i := range 5 {
		l.Append([]string{"SET", "afterwards" + strconv.Itoa(i), "v"})
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]bool)
	for _, args := range readBack(t, path) {
		seen[strings.Join(args, " ")] = true
	}
	if !seen["SET base 1"] {
		t.Error("the rewritten log lost the copied keyspace")
	}
	for i := range 5 {
		if key := "SET afterwards" + strconv.Itoa(i) + " v"; !seen[key] {
			t.Errorf("the log lost %q, written after the rewrite finished", key)
		}
	}
	// Commands from before the copy are gone by design: the copy stands in
	// for them. Which ones those are depends on timing, so the check that
	// nothing is really lost is TestRewriteKeepsEveryCommandsEffect, which
	// uses a real keyspace instead of a fixed copy.
}

func TestRewriteRefusesASecondOne(t *testing.T) {
	l, _ := openRewritable(t, oneKey("k", "v"), Options{Policy: FsyncNever})

	var second error
	l.afterBaseWritten = func() { second = l.Rewrite() }

	if err := l.Rewrite(); err != nil {
		t.Fatal(err)
	}
	waitForRewrite(t, l)

	if !errors.Is(second, ErrRewriteInProgress) {
		t.Fatalf("a second rewrite returned %v, want ErrRewriteInProgress", second)
	}
}

func TestRewriteNeedsAKeyspace(t *testing.T) {
	l, _ := openTestLog(t, FsyncNever)
	if err := l.Rewrite(); !errors.Is(err, ErrRewriteUnavailable) {
		t.Fatalf("Rewrite on a log opened without Export = %v, want ErrRewriteUnavailable", err)
	}
}

// TestAutomaticRewrite checks the growth trigger, with thresholds low
// enough that a handful of commands is enough to cross them.
func TestAutomaticRewrite(t *testing.T) {
	l, path := openRewritable(t, oneKey("k", "v"), Options{
		Policy:            FsyncNever,
		RewritePercentage: 100,
		RewriteMinSize:    500,
	})

	for range 50 {
		l.Append([]string{"SET", "k", "v"})
		if err := l.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	waitForRewrite(t, l)

	got := joinCommands(readBack(t, path))
	if len(got) >= 50 {
		t.Fatalf("the log still holds %d commands, so no automatic rewrite happened", len(got))
	}
	if got[0] != "SET k v" {
		t.Fatalf("the log starts with %q, want the copied keyspace", got[0])
	}
}

// TestAutomaticRewriteCanBeTurnedOff covers both brakes on the trigger: a
// percentage of zero, and a file too small to be worth the work.
func TestAutomaticRewriteCanBeTurnedOff(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{"percentage zero", Options{Policy: FsyncNever, RewritePercentage: 0, RewriteMinSize: 1}},
		{"below the minimum size", Options{Policy: FsyncNever, RewritePercentage: 100, RewriteMinSize: 1 << 30}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, path := openRewritable(t, oneKey("k", "v"), tt.opts)
			for range 50 {
				l.Append([]string{"SET", "k", "v"})
				if err := l.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if l.Rewriting() {
				t.Fatal("a rewrite started although it was turned off")
			}
			if got := readBack(t, path); len(got) != 50 {
				t.Fatalf("the log holds %d commands, want all 50 still there", len(got))
			}
		})
	}
}

// TestRewriteSurvivesAFailure covers a rewrite that cannot write its new
// file. The old one has to stay in place and keep taking writes, because
// it is still correct.
func TestRewriteSurvivesAFailure(t *testing.T) {
	l, path := openRewritable(t, oneKey("k", "v"), Options{Policy: FsyncNever})

	l.Append([]string{"SET", "before", "1"})
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}

	// Point the rewrite at a directory that does not exist, so creating
	// its temporary file fails.
	good := l.path
	l.path = filepath.Join(path, "missing", "appendonly.aof")
	if err := l.Rewrite(); err != nil {
		t.Fatal(err)
	}
	waitForRewrite(t, l)
	l.path = good

	l.Append([]string{"SET", "after", "1"})
	if err := l.Flush(); err != nil {
		t.Fatalf("the log stopped working after a failed rewrite: %v", err)
	}
	if got := joinCommands(readBack(t, path)); len(got) != 2 || got[1] != "SET after 1" {
		t.Fatalf("the log holds %q, want both writes around the failed rewrite", got)
	}

	if entries, err := os.ReadDir(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 {
		t.Errorf("the directory holds %d files, want only the log itself", len(entries))
	}
}

// TestRewriteKeepsTheLogUsable checks that the log keeps working normally
// once a rewrite has swapped the file underneath it.
func TestRewriteKeepsTheLogUsable(t *testing.T) {
	l, path := openRewritable(t, oneKey("base", "1"), Options{Policy: FsyncEverySecond})

	l.Append([]string{"SET", "before", "1"})
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := l.Rewrite(); err != nil {
		t.Fatal(err)
	}
	waitForRewrite(t, l)

	for i := range 10 {
		l.Append([]string{"SET", "after" + strconv.Itoa(i), "1"})
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close after a rewrite: %v", err)
	}

	got := joinCommands(readBack(t, path))
	if len(got) != 11 {
		t.Fatalf("the log holds %d commands, want the base plus 10 later writes:\n%s",
			len(got), strings.Join(got, "\n"))
	}
	if got[0] != "SET base 1" || got[10] != "SET after9 1" {
		t.Fatalf("the log holds %q", got)
	}
}
