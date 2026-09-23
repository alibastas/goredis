package aof_test

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/persistence/aof"
	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// This file tests the append-only file the way the server uses it: writes
// go through the real command table, and the log is then replayed into an
// empty store. The two keyspaces have to match, because that is the whole
// promise of the file.

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// keyspace renders a store as sorted text, so two of them can be compared
// without depending on map order.
func keyspace(t *testing.T, s *store.Store) []string {
	t.Helper()
	records := s.Export()
	lines := make([]string, 0, len(records))
	for _, rec := range records {
		elems := slices.Clone(rec.Elems)
		if rec.Kind == store.KindSet {
			slices.Sort(elems) // set members have no order
		}
		fields := make([]string, 0, len(rec.Fields))
		for f, v := range rec.Fields {
			fields = append(fields, f+"="+v)
		}
		slices.Sort(fields)

		expiry := "never"
		if !rec.ExpiresAt.IsZero() {
			expiry = rec.ExpiresAt.UTC().Format(time.RFC3339Nano)
		}
		lines = append(lines, strings.Join([]string{
			rec.Key, rec.Kind.String(), rec.Str,
			strings.Join(elems, ","), strings.Join(fields, ","), expiry,
		}, "|"))
	}
	slices.Sort(lines)
	return lines
}

// writeThroughCommands runs commands through a command table with a real log attached
// and returns the path of the file they landed in, plus the store they
// built.
func writeThroughCommands(t *testing.T, c *clock, policy aof.FsyncPolicy, commands [][]string) (string, *store.Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	log, err := aof.Open(path, policy)
	if err != nil {
		t.Fatal(err)
	}

	db := store.NewWithClock(c.Now)
	r := command.NewRegistry(db, command.WithAppendOnly(log))
	for _, args := range commands {
		if reply := r.DispatchArgs(args); reply.Type == resp.Error {
			t.Fatalf("%q replied %q", args, reply.Str)
		}
		// The server flushes once a batch of pipelined commands is done;
		// one command per batch is the worst case for the log.
		if err := r.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	return path, db
}

// replay rebuilds a store from the log at path, the way startup does.
func replay(t *testing.T, c *clock, path string) *store.Store {
	t.Helper()
	db := store.NewWithClock(c.Now)
	r := command.NewRegistry(db)
	if _, err := aof.Load(path, r.Replay, discard()); err != nil {
		t.Fatalf("Load: %v", err)
	}
	return db
}

func TestReplayRebuildsTheKeyspace(t *testing.T) {
	c := &clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	commands := [][]string{
		{"SET", "greeting", "hello world"},
		{"SET", "overwritten", "first"},
		{"SET", "overwritten", "second"},
		{"SET", "gone", "bye"},
		{"DEL", "gone"},
		{"SET", "counter", "10"},
		{"INCR", "counter"},
		{"INCRBY", "counter", "5"},
		{"DECR", "counter"},
		{"RPUSH", "list", "a", "b", "c"},
		{"LPUSH", "list", "z"},
		{"RPOP", "list"},
		{"SADD", "tags", "go", "redis", "aof"},
		{"SREM", "tags", "aof"},
		{"HSET", "user", "name", "ali", "age", "22"},
		{"HDEL", "user", "age"},
		{"HSET", "user", "city", "istanbul"},
		{"SET", "binary", "a\x00b\r\nc"},
		{"SET", "empty", ""},
		{"SET", "kept", "1", "EX", "3600"},
		{"EXPIRE", "greeting", "7200"},
		{"SET", "persisted", "1", "EX", "60"},
		{"PERSIST", "persisted"},
		{"SET", "emptied", "1"},
		{"DEL", "emptied"},
	}

	path, want := writeThroughCommands(t, c, aof.FsyncEverySecond, commands)
	got := replay(t, c, path)

	if !reflect.DeepEqual(keyspace(t, got), keyspace(t, want)) {
		t.Fatalf("replay produced a different keyspace:\n got: %s\nwant: %s",
			strings.Join(keyspace(t, got), "\n      "),
			strings.Join(keyspace(t, want), "\n      "))
	}
}

// TestReplayDoesNotRenewTimeouts is the reason timeouts are rewritten to
// absolute deadlines before they are logged. Replaying "expire in an hour"
// after two hours of downtime would bring a dead key back to life.
func TestReplayDoesNotRenewTimeouts(t *testing.T) {
	c := &clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	path, _ := writeThroughCommands(t, c, aof.FsyncNever, [][]string{
		{"SET", "short", "1", "EX", "60"},
		{"SET", "long", "1", "EX", "7200"},
		{"SET", "plain", "1"},
		{"EXPIRE", "plain", "3600"},
	})

	// The server was down for an hour and a half.
	c.now = c.now.Add(90 * time.Minute)
	db := replay(t, c, path)

	if _, ok := db.TTL("short"); ok {
		t.Error("a key whose deadline had passed came back after replay")
	}
	if _, ok := db.TTL("plain"); ok {
		t.Error("a key expired by EXPIRE came back after replay")
	}
	ttl, ok := db.TTL("long")
	if !ok {
		t.Fatal("a key that should still be alive is missing")
	}
	if want := 30 * time.Minute; ttl != want {
		t.Errorf("TTL after replay = %v, want %v (not the original 2h)", ttl, want)
	}
}

// TestReplayAfterACrash cuts the log short in the middle of a command, the
// way a power cut does, and checks the server comes back with everything
// that was complete.
func TestReplayAfterACrash(t *testing.T) {
	c := &clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	path, _ := writeThroughCommands(t, c, aof.FsyncNever, [][]string{
		{"SET", "a", "1"},
		{"SET", "b", "2"},
		{"SET", "c", "3"},
	})

	// Chop off the last few bytes, leaving a fragment behind.
	content := readFile(t, path)
	writeFile(t, path, content[:len(content)-6])

	db := replay(t, c, path)
	if got, _, _ := db.Get("a"); got != "1" {
		t.Errorf("a = %q after a crash, want 1", got)
	}
	if got, _, _ := db.Get("b"); got != "2" {
		t.Errorf("b = %q after a crash, want 2", got)
	}
	if _, ok, _ := db.Get("c"); ok {
		t.Error("the half-written command was replayed")
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestRewriteKeepsEveryCommandsEffect is the strongest thing this package
// can check about a rewrite, and it needs a real keyspace to do it: after
// commands have run against a store while a rewrite happens underneath
// them, replaying the resulting log into an empty store has to produce
// exactly the same keyspace.
//
// It catches both ways a rewrite can go wrong. A command that ends up in
// neither the copy nor the new file is missing afterwards. A command whose
// effect is already in the copy and which is written to the new file as
// well is applied twice, which no amount of SETs would reveal, so the
// commands here are INCRs and pushes, where doing something twice shows.
//
// Both depend on an interleaving, so this is a test that has to be run to
// be believed: with the write lock removed it fails every time, and with
// only the copy taken outside that lock it fails in about one run in four.
func TestRewriteKeepsEveryCommandsEffect(t *testing.T) {
	c := &clock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "appendonly.aof")

	log, err := aof.OpenWithOptions(path, aof.Options{Policy: aof.FsyncNever, Logger: discard()})
	if err != nil {
		t.Fatal(err)
	}
	db := store.NewWithClock(c.Now)
	r := command.NewRegistry(db, command.WithAppendOnly(log))
	log.SetExport(r.ExportForRewrite)

	const writers, each = 8, 150
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				// INCR and RPUSH both count, so applying one twice changes
				// the result in a way the comparison below will catch.
				r.DispatchArgs([]string{"INCR", "counter"})
				r.DispatchArgs([]string{"RPUSH", "log", strconv.Itoa(w)})
				if err := r.Flush(); err != nil {
					t.Error(err)
					return
				}
				// Rewrites keep starting throughout the run, so the copy
				// lands at many different points relative to the writes.
				if i%37 == w {
					if err := log.Rewrite(); err != nil && !errors.Is(err, aof.ErrRewriteInProgress) {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()

	if err := log.Close(); err != nil {
		t.Fatal(err)
	}

	want := keyspace(t, db)
	got := keyspace(t, replay(t, c, path))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replaying the rewritten log gives a different keyspace:\n got: %s\nwant: %s",
			strings.Join(got, "\n      "), strings.Join(want, "\n      "))
	}
	if n, _, _ := db.Get("counter"); n != strconv.Itoa(writers*each) {
		t.Fatalf("the live counter is %q, want %d: the test itself lost writes", n, writers*each)
	}
}
