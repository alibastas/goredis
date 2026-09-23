package aof

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func openTestLog(t *testing.T, policy FsyncPolicy) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	l, err := Open(path, policy)
	if err != nil {
		t.Fatal(err)
	}
	// Windows will not delete the temporary directory while the file is
	// still open, and Close is safe to call twice.
	t.Cleanup(func() { l.Close() })
	return l, path
}

// readBack replays a log file and returns the commands it held.
func readBack(t *testing.T, path string) [][]string {
	t.Helper()
	var got [][]string
	if _, err := Load(path, func(args []string) error {
		got = append(got, args)
		return nil
	}, discardLogger()); err != nil {
		t.Fatalf("Load(%s): %v", path, err)
	}
	return got
}

func TestParseFsyncPolicy(t *testing.T) {
	for _, name := range []string{"always", "everysec", "no"} {
		p, err := ParseFsyncPolicy(name)
		if err != nil {
			t.Fatalf("ParseFsyncPolicy(%q): %v", name, err)
		}
		if p.String() != name {
			t.Errorf("policy %q prints as %q", name, p)
		}
	}
	if _, err := ParseFsyncPolicy("sometimes"); err == nil {
		t.Error("ParseFsyncPolicy accepted an unknown policy")
	}
}

func TestAppendAndReplay(t *testing.T) {
	commands := [][]string{
		{"SET", "k", "v"},
		{"RPUSH", "list", "a", "b"},
		{"SET", "binary", "a\x00b\r\nc"},
		{"SET", "empty", ""},
		{"DEL", "k"},
	}

	for _, policy := range []FsyncPolicy{FsyncAlways, FsyncEverySecond, FsyncNever} {
		t.Run(policy.String(), func(t *testing.T) {
			l, path := openTestLog(t, policy)
			for _, args := range commands {
				l.Append(args)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got := readBack(t, path)
			if len(got) != len(commands) {
				t.Fatalf("replayed %d commands, want %d", len(got), len(commands))
			}
			for i, args := range got {
				if strings.Join(args, "|") != strings.Join(commands[i], "|") {
					t.Errorf("command %d = %q, want %q", i, args, commands[i])
				}
			}
		})
	}
}

// TestAppendBuffersUntilFlush is the whole point of the Flush split: a
// command must not reach the operating system until the server says so,
// and once it has, a reader outside the process can see it.
func TestAppendBuffersUntilFlush(t *testing.T) {
	l, path := openTestLog(t, FsyncNever)
	defer l.Close()

	l.Append([]string{"SET", "k", "v"})
	if size := sizeOf(t, path); size != 0 {
		t.Fatalf("file is %d bytes before Flush, want 0", size)
	}

	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if size := sizeOf(t, path); size == 0 {
		t.Fatal("file is still empty after Flush")
	}
	if got := readBack(t, path); len(got) != 1 || got[0][2] != "v" {
		t.Fatalf("replayed %q, want one SET", got)
	}
}

// TestAppendIsConcurrencySafe writes from many goroutines at once, the way
// many client connections do. Every command must come back whole, with
// nothing interleaved into the middle of another.
func TestAppendIsConcurrencySafe(t *testing.T) {
	l, path := openTestLog(t, FsyncNever)

	const writers, each = 8, 50
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				l.Append([]string{"RPUSH", "list", strings.Repeat("x", w*10+1)})
				if i%5 == 0 {
					if err := l.Flush(); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	got := readBack(t, path)
	if len(got) != writers*each {
		t.Fatalf("replayed %d commands, want %d", len(got), writers*each)
	}
	for i, args := range got {
		if len(args) != 3 || args[0] != "RPUSH" {
			t.Fatalf("command %d came back as %q", i, args)
		}
	}
}

func TestLoadOfAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if _, err := Load(path, func([]string) error { return nil }, discardLogger()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load of a missing file = %v, want ErrNotExist", err)
	}
}

func sizeOf(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// TestGroupCommit is the reason syncLocked is written the way it is. Under
// the always policy every connection wants its write on the disk before it
// answers, but one fsync covers the whole file, so a crowd of them should
// cost far fewer than one fsync each.
func TestGroupCommit(t *testing.T) {
	l, path := openTestLog(t, FsyncAlways)

	var syncs atomic.Int64
	l.sync = func() error {
		syncs.Add(1)
		// Stand in for a slow disk, so the goroutines really do pile up.
		time.Sleep(2 * time.Millisecond)
		return nil
	}

	const writers = 40
	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // let them all arrive together
			l.Append([]string{"SET", "k" + strconv.Itoa(w), "v"})
			if err := l.Flush(); err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := syncs.Load(); got >= writers {
		t.Errorf("%d fsyncs for %d concurrent flushes: they were not shared", got, writers)
	}
	if got := syncs.Load(); got == 0 {
		t.Error("no fsync happened at all under the always policy")
	}

	// Sharing must not cost durability: every command is still in the file.
	l.sync = func() error { return nil }
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readBack(t, path); len(got) != writers {
		t.Fatalf("the log holds %d commands, want %d", len(got), writers)
	}
}

// TestAlwaysSyncsBeforeFlushReturns states the guarantee the always policy
// makes: when Flush returns, an fsync has covered that write. The server
// only sends replies after Flush, so a client that was told "OK" can rely
// on the data being on the disk.
func TestAlwaysSyncsBeforeFlushReturns(t *testing.T) {
	l, _ := openTestLog(t, FsyncAlways)
	defer l.Close()

	var syncs atomic.Int64
	l.sync = func() error { syncs.Add(1); return nil }

	l.Append([]string{"SET", "k", "v"})
	if got := syncs.Load(); got != 0 {
		t.Fatalf("Append alone caused %d fsyncs, want 0", got)
	}
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("Flush caused %d fsyncs, want 1", got)
	}

	// Nothing new to write means nothing to sync.
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := syncs.Load(); got != 1 {
		t.Fatalf("a Flush with nothing buffered caused another fsync (%d total)", got)
	}
}

// TestOtherPoliciesDoNotSyncOnFlush is the other side of the dial: those
// policies hand the write to the operating system and move on.
func TestOtherPoliciesDoNotSyncOnFlush(t *testing.T) {
	for _, policy := range []FsyncPolicy{FsyncEverySecond, FsyncNever} {
		t.Run(policy.String(), func(t *testing.T) {
			l, _ := openTestLog(t, policy)
			var syncs atomic.Int64
			l.sync = func() error { syncs.Add(1); return nil }

			l.Append([]string{"SET", "k", "v"})
			if err := l.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := syncs.Load(); got != 0 {
				t.Errorf("Flush under %q caused %d fsyncs, want 0", policy, got)
			}

			// Closing down is the one point where every policy syncs.
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if got := syncs.Load(); got != 1 {
				t.Errorf("Close under %q caused %d fsyncs, want 1", policy, got)
			}
		})
	}
}

// TestWriteFailureIsSticky covers a disk that stops accepting writes. The
// log must report it and keep reporting it, rather than quietly carrying on
// with a file that is missing commands.
func TestWriteFailureIsSticky(t *testing.T) {
	l, _ := openTestLog(t, FsyncNever)

	l.Append([]string{"SET", "k", "v"})
	if err := l.Flush(); err != nil {
		t.Fatal(err)
	}

	// Closing the file underneath makes every later write fail.
	l.f.Close()
	l.Append([]string{"SET", "k2", "v"})
	first := l.Flush()
	if first == nil {
		t.Fatal("Flush onto a closed file succeeded")
	}
	l.Append([]string{"SET", "k3", "v"})
	if err := l.Flush(); err == nil {
		t.Fatal("the log went back to reporting success after a failed write")
	}
}
