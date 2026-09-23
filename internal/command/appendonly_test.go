package command

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// fakeLog stands in for the append-only file and just remembers what it
// was handed.
type fakeLog struct {
	appended [][]string
	flushes  int
	rewrites int
	err      error
}

func (l *fakeLog) Append(args []string) {
	l.appended = append(l.appended, slices.Clone(args))
}

func (l *fakeLog) Flush() error {
	l.flushes++
	return l.err
}

// joined renders the log the way the tests compare it, one command per
// line with arguments separated by spaces.
func (l *fakeLog) joined() []string {
	out := make([]string, len(l.appended))
	for i, args := range l.appended {
		out[i] = strings.Join(args, " ")
	}
	return out
}

// logHarness runs commands against a registry with a fake log attached and
// a clock the test controls.
type logHarness struct {
	*harness
	log *fakeLog
}

func newLogHarness(t *testing.T) *logHarness {
	h := &harness{t: t, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	l := &fakeLog{}
	h.r = NewRegistry(store.NewWithClock(func() time.Time { return h.now }), WithAppendOnly(l))
	return &logHarness{harness: h, log: l}
}

// at renders a moment the way PEXPIREAT takes it, so the expected log
// lines can be written without hard-coding a timestamp.
func (h *logHarness) at(d time.Duration) string {
	return strconv.FormatInt(h.now.Add(d).UnixMilli(), 10)
}

func (h *logHarness) run(args ...string) { h.r.DispatchArgs(args) }

func (h *logHarness) wantLog(want ...string) {
	h.t.Helper()
	if got := h.log.joined(); !slices.Equal(got, want) {
		h.t.Fatalf("the log holds:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// TestOnlyWritesAreLogged checks the line between the two kinds of
// command. Reads must not reach the log: they change nothing, and
// replaying them would only cost time.
func TestOnlyWritesAreLogged(t *testing.T) {
	h := newLogHarness(t)

	h.run("SET", "k", "v")
	h.run("GET", "k")
	h.run("EXISTS", "k")
	h.run("TTL", "k")
	h.run("TYPE", "k")
	h.run("KEYS", "*")
	h.run("DBSIZE")
	h.run("PING")
	h.run("INCRBY", "n", "5")
	h.run("RPUSH", "list", "a")
	h.run("LRANGE", "list", "0", "-1")
	h.run("LPOP", "list")
	h.run("SADD", "set", "m")
	h.run("SMEMBERS", "set")
	h.run("HSET", "hash", "f", "v")
	h.run("HGETALL", "hash")
	h.run("DEL", "k")
	h.run("FLUSHDB")

	h.wantLog(
		"SET k v",
		"INCRBY n 5",
		"RPUSH list a",
		"LPOP list",
		"SADD set m",
		"HSET hash f v",
		"DEL k",
		"FLUSHDB",
	)
}

// TestFailedCommandsAreNotLogged covers the other half: a command that
// answered with an error changed nothing, so logging it would put a
// command in the file that fails again on every replay.
func TestFailedCommandsAreNotLogged(t *testing.T) {
	h := newLogHarness(t)
	h.run("SET", "str", "hello")
	h.run("LPUSH", "str", "x")           // WRONGTYPE
	h.run("INCR", "str")                 // not an integer
	h.run("SET", "k", "v", "EX", "zero") // not an integer
	h.run("SET", "k")                    // wrong number of arguments
	h.run("NOSUCHCMD", "k")              // unknown command
	h.run("EXPIRE", "str", "not-a-number")

	h.wantLog("SET str hello")
}

// TestRelativeTimeoutsBecomeAbsolute is the reason the log rewrites
// commands at all. "Expire in 60 seconds" in a file replayed days later
// would hand the key another minute of life every single time.
func TestRelativeTimeoutsBecomeAbsolute(t *testing.T) {
	h := newLogHarness(t)

	h.run("SET", "a", "1", "EX", "60")
	h.run("SET", "b", "1", "PX", "1500")
	h.run("SET", "c", "1", "NX", "EX", "30")
	h.run("EXPIRE", "a", "120")
	h.run("PEXPIRE", "b", "2500")

	h.wantLog(
		"SET a 1 PXAT "+h.at(60*time.Second),
		"SET b 1 PXAT "+h.at(1500*time.Millisecond),
		"SET c 1 NX PXAT "+h.at(30*time.Second),
		"PEXPIREAT a "+h.at(120*time.Second),
		"PEXPIREAT b "+h.at(2500*time.Millisecond),
	)
}

// TestAbsoluteTimeoutsAreNormalised keeps replay simple: however a client
// spelled the deadline, the log holds one form of it.
func TestAbsoluteTimeoutsAreNormalised(t *testing.T) {
	h := newLogHarness(t)
	deadline := h.now.Add(time.Hour)
	secs := strconv.FormatInt(deadline.Unix(), 10)
	ms := strconv.FormatInt(deadline.UnixMilli(), 10)

	h.run("SET", "a", "1")
	h.run("SET", "b", "1")
	h.run("SET", "c", "1", "EXAT", secs)
	h.run("SET", "d", "1", "PXAT", ms)
	h.run("EXPIREAT", "a", secs)
	h.run("PEXPIREAT", "b", ms)

	h.wantLog(
		"SET a 1",
		"SET b 1",
		"SET c 1 PXAT "+ms,
		"SET d 1 PXAT "+ms,
		"PEXPIREAT a "+ms,
		"PEXPIREAT b "+ms,
	)
}

// TestCommandsWithoutTimeoutsAreLoggedAsSent makes sure the rewriting only
// touches what it has to.
func TestCommandsWithoutTimeoutsAreLoggedAsSent(t *testing.T) {
	h := newLogHarness(t)
	h.run("SET", "a", "1")
	h.run("SET", "b", "2", "XX")
	h.run("SET", "a", "3", "KEEPTTL")
	h.run("PERSIST", "a")

	h.wantLog("SET a 1", "SET b 2 XX", "SET a 3 KEEPTTL", "PERSIST a")
}

// TestSkippedConditionalWritesAreStillLogged documents the choice not to
// guess whether a command really changed anything. SET NX on an existing
// key replies with nil, gets logged, and is skipped again on replay, so
// the outcome is the same either way.
func TestSkippedConditionalWritesAreStillLogged(t *testing.T) {
	h := newLogHarness(t)
	h.run("SET", "k", "first")
	h.expect(null, "SET", "k", "second", "NX")

	h.wantLog("SET k first", "SET k second NX")
}

func TestFlushGoesToTheLog(t *testing.T) {
	h := newLogHarness(t)
	if err := h.r.Flush(); err != nil {
		t.Fatal(err)
	}
	if h.log.flushes != 1 {
		t.Fatalf("the log saw %d flushes, want 1", h.log.flushes)
	}

	h.log.err = errors.New("disk is full")
	if err := h.r.Flush(); err == nil {
		t.Fatal("Flush hid the log's error")
	}
}

// TestRegistryWithoutALogDoesNothing keeps the default path free of
// surprises: a server started without the append-only file must not need
// one to work.
func TestRegistryWithoutALogDoesNothing(t *testing.T) {
	h := newHarness(t)
	h.expect(OK, "SET", "k", "v")
	if err := h.r.Flush(); err != nil {
		t.Fatalf("Flush without a log = %v, want nil", err)
	}
}

// TestReplay is the entry point the append-only file calls back into.
func TestReplay(t *testing.T) {
	h := newHarness(t)
	if err := h.r.Replay([]string{"SET", "k", "v"}); err != nil {
		t.Fatalf("Replay of a good command = %v", err)
	}
	h.expect(bulk("v"), "GET", "k")

	err := h.r.Replay([]string{"NOSUCHCMD"})
	if err == nil {
		t.Fatal("Replay accepted an unknown command")
	}
	if !strings.Contains(err.Error(), "unknown command") {
		t.Errorf("Replay error = %q, want it to mention the unknown command", err)
	}
}

func (l *fakeLog) Rewrite() error {
	l.rewrites++
	return l.err
}

func TestBgrewriteaof(t *testing.T) {
	h := newLogHarness(t)
	h.expect(resp.NewSimpleString("Background append only file rewriting started"), "BGREWRITEAOF")
	if h.log.rewrites != 1 {
		t.Fatalf("the log saw %d rewrites, want 1", h.log.rewrites)
	}

	h.log.err = errors.New("Background append only file rewriting already in progress")
	h.expect(errReply("ERR Background append only file rewriting already in progress"), "BGREWRITEAOF")

	// BGREWRITEAOF is not itself a write, so it must not end up in the log.
	h.wantLog()
}

// TestBgrewriteaofWithoutALog covers a server started without the
// append-only file: the command answers clearly instead of looking unknown.
func TestBgrewriteaofWithoutALog(t *testing.T) {
	h := newHarness(t)
	h.expect(errReply("ERR the append-only file is disabled on this server"), "BGREWRITEAOF")
}
