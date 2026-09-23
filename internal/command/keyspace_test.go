package command

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/resp"
)

func TestDelAndExists(t *testing.T) {
	h := newHarness(t)
	h.expect(OK, "SET", "a", "1")
	h.expect(OK, "SET", "b", "2")

	h.expect(integer(3), "EXISTS", "a", "b", "a", "missing")
	h.expect(integer(2), "DEL", "a", "b", "missing")
	h.expect(integer(0), "EXISTS", "a", "b")
}

func TestExpireTTLAndPersist(t *testing.T) {
	h := newHarness(t)

	h.expect(integer(-2), "TTL", "k")
	h.expect(integer(0), "EXPIRE", "k", "10")

	h.expect(OK, "SET", "k", "v")
	h.expect(integer(-1), "TTL", "k")
	h.expect(integer(1), "EXPIRE", "k", "10")
	h.advance(4 * time.Second)
	h.expect(integer(6), "TTL", "k")
	h.expect(integer(6000), "PTTL", "k")

	h.expect(integer(1), "PERSIST", "k")
	h.expect(integer(0), "PERSIST", "k")
	h.expect(integer(-1), "TTL", "k")

	h.expect(integer(1), "PEXPIRE", "k", "500")
	h.advance(500 * time.Millisecond)
	h.expect(integer(-2), "TTL", "k")
}

func TestExpireWithNonPositiveTimeoutDeletes(t *testing.T) {
	h := newHarness(t)
	h.expect(OK, "SET", "k", "v")
	h.expect(integer(1), "EXPIRE", "k", "-1")
	h.expect(integer(0), "EXISTS", "k")
}

func TestExpireErrors(t *testing.T) {
	h := newHarness(t)
	h.expect(OK, "SET", "k", "v")

	h.expect(errReply("ERR value is not an integer or out of range"), "EXPIRE", "k", "later")
	h.expect(errReply("ERR invalid expire time in 'expire' command"), "EXPIRE", "k", "9223372036854775807")
	h.expect(integer(-1), "TTL", "k")
}

func TestKeys(t *testing.T) {
	h := newHarness(t)
	for _, k := range []string{"user:1", "user:2", "session:1"} {
		h.expect(OK, "SET", k, "x")
	}

	// KEYS returns keys in no particular order, so sort before comparing.
	got := h.r.Dispatch(cmd("KEYS", "user:*"))
	slices.SortFunc(got.Array, func(a, b resp.Value) int { return strings.Compare(a.Str, b.Str) })
	want := resp.NewArray(bulk("user:1"), bulk("user:2"))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("KEYS user:*\n got: %+v\nwant: %+v", got, want)
	}

	if got := h.r.Dispatch(cmd("KEYS", "nope*")); got.Type != resp.Array || got.Null || len(got.Array) != 0 {
		t.Fatalf("KEYS nope*: got %+v, want an empty array", got)
	}
}

func TestDBSizeAndFlushDB(t *testing.T) {
	h := newHarness(t)
	h.expect(integer(0), "DBSIZE")
	h.expect(OK, "SET", "a", "1")
	h.expect(OK, "SET", "b", "2")
	h.expect(integer(2), "DBSIZE")
	h.expect(OK, "FLUSHDB")
	h.expect(integer(0), "DBSIZE")
}

// TestExpireAt covers the absolute form of the timeout commands. They
// exist because the append-only file stores every deadline this way: a
// relative timeout written to a log would be measured again from whenever
// the log is replayed.
func TestExpireAt(t *testing.T) {
	h := newHarness(t)
	secs := func(d time.Duration) string {
		return strconv.FormatInt(h.now.Add(d).Unix(), 10)
	}
	ms := func(d time.Duration) string {
		return strconv.FormatInt(h.now.Add(d).UnixMilli(), 10)
	}

	h.expect(integer(0), "EXPIREAT", "k", secs(time.Hour))
	h.expect(OK, "SET", "k", "v")

	h.expect(integer(1), "EXPIREAT", "k", secs(time.Hour))
	h.expect(integer(3600), "TTL", "k")

	h.expect(integer(1), "PEXPIREAT", "k", ms(1500*time.Millisecond))
	h.expect(integer(1500), "PTTL", "k")

	// A deadline that has already passed deletes the key, like a negative
	// timeout does.
	h.expect(integer(1), "PEXPIREAT", "k", ms(-time.Second))
	h.expect(integer(0), "EXISTS", "k")
}

func TestExpireAtErrors(t *testing.T) {
	h := newHarness(t)
	h.expect(OK, "SET", "k", "v")

	h.expect(errReply("ERR value is not an integer or out of range"), "EXPIREAT", "k", "soon")
	// Deadlines past the year 9999 are refused, so every expiry the server
	// accepts is one a snapshot can carry.
	h.expect(errReply("ERR invalid expire time in 'expireat' command"), "EXPIREAT", "k", "999999999999")
	h.expect(errReply("ERR invalid expire time in 'pexpireat' command"), "PEXPIREAT", "k", "999999999999999")
	h.expect(integer(-1), "TTL", "k")
}
