package command

import (
	"reflect"
	"slices"
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
