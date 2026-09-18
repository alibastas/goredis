package command

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/alibastas/goredis/internal/resp"
)

var wrongType = errReply("WRONGTYPE Operation against a key holding the wrong kind of value")

func array(items ...string) resp.Value {
	return stringsReply(items)
}

// expectUnordered is expect for replies whose element order isn't defined,
// like SMEMBERS.
func (h *harness) expectUnordered(want []string, args ...string) {
	h.t.Helper()
	got := h.r.Dispatch(cmd(args...))
	var items []string
	for _, v := range got.Array {
		items = append(items, v.Str)
	}
	slices.Sort(items)
	slices.Sort(want)
	if got.Type != resp.Array || !slices.Equal(items, want) {
		h.t.Fatalf("%s\n got: %+v\nwant (any order): %v", strings.Join(args, " "), got, want)
	}
}

func TestHashCommands(t *testing.T) {
	h := newHarness(t)

	h.expect(integer(2), "HSET", "user:1", "name", "ali", "age", "21")
	h.expect(integer(0), "HSET", "user:1", "age", "22")
	h.expect(bulk("22"), "HGET", "user:1", "age")
	h.expect(null, "HGET", "user:1", "missing")
	h.expect(null, "HGET", "nokey", "f")
	h.expect(integer(1), "HEXISTS", "user:1", "name")
	h.expect(integer(0), "HEXISTS", "user:1", "missing")
	h.expect(integer(2), "HLEN", "user:1")
	h.expect(resp.NewSimpleString("hash"), "TYPE", "user:1")

	// HGETALL order isn't defined, so compare as pairs.
	got := h.r.Dispatch(cmd("HGETALL", "user:1"))
	pairs := map[string]string{}
	for i := 0; i+1 < len(got.Array); i += 2 {
		pairs[got.Array[i].Str] = got.Array[i+1].Str
	}
	if want := map[string]string{"name": "ali", "age": "22"}; !reflect.DeepEqual(pairs, want) {
		t.Fatalf("HGETALL pairs = %v, want %v", pairs, want)
	}
	h.expect(array(), "HGETALL", "nokey")

	h.expect(integer(2), "HDEL", "user:1", "name", "age", "missing")
	h.expect(integer(0), "EXISTS", "user:1")

	h.expect(errReply("ERR wrong number of arguments for 'hset' command"), "HSET", "k", "f", "v", "dangling")
}

func TestListCommands(t *testing.T) {
	h := newHarness(t)

	h.expect(integer(2), "RPUSH", "q", "b", "c")
	h.expect(integer(4), "LPUSH", "q", "a", "z")
	h.expect(array("z", "a", "b", "c"), "LRANGE", "q", "0", "-1")
	h.expect(array("b", "c"), "LRANGE", "q", "-2", "100")
	h.expect(array(), "LRANGE", "q", "5", "10")
	h.expect(array(), "LRANGE", "nokey", "0", "-1")
	h.expect(integer(4), "LLEN", "q")
	h.expect(bulk("z"), "LINDEX", "q", "0")
	h.expect(bulk("c"), "LINDEX", "q", "-1")
	h.expect(null, "LINDEX", "q", "9")
	h.expect(resp.NewSimpleString("list"), "TYPE", "q")

	h.expect(bulk("z"), "LPOP", "q")
	h.expect(bulk("c"), "RPOP", "q")
	h.expect(array("a", "b"), "LPOP", "q", "5")
	h.expect(null, "LPOP", "q")
	h.expect(resp.NullArray(), "LPOP", "q", "2")
	h.expect(integer(0), "EXISTS", "q")

	h.expect(errReply("ERR value is not an integer or out of range"), "LRANGE", "q", "zero", "1")
	h.expect(errReply("ERR value is out of range, must be positive"), "LPOP", "q", "-1")
	h.expect(errReply("ERR wrong number of arguments for 'lpop' command"), "LPOP", "q", "1", "2")
}

func TestSetCommands(t *testing.T) {
	h := newHarness(t)

	h.expect(integer(2), "SADD", "tags", "go", "redis", "go")
	h.expect(integer(1), "SADD", "tags", "db")
	h.expect(integer(3), "SCARD", "tags")
	h.expect(integer(1), "SISMEMBER", "tags", "go")
	h.expect(integer(0), "SISMEMBER", "tags", "java")
	h.expectUnordered([]string{"db", "go", "redis"}, "SMEMBERS", "tags")
	h.expect(array(), "SMEMBERS", "nokey")
	h.expect(resp.NewSimpleString("set"), "TYPE", "tags")

	h.expect(integer(3), "SREM", "tags", "go", "redis", "db", "java")
	h.expect(integer(0), "EXISTS", "tags")
	h.expect(resp.NewSimpleString("none"), "TYPE", "tags")
}

func TestWrongTypeReplies(t *testing.T) {
	h := newHarness(t)
	h.expect(OK, "SET", "str", "v")
	h.expect(integer(1), "LPUSH", "list", "v")

	for _, args := range [][]string{
		{"GET", "list"},
		{"INCR", "list"},
		{"HSET", "str", "f", "v"},
		{"HGET", "str", "f"},
		{"HLEN", "str"},
		{"LPUSH", "str", "v"},
		{"LPOP", "str"},
		{"LRANGE", "str", "0", "-1"},
		{"LLEN", "str"},
		{"SADD", "list", "m"},
		{"SISMEMBER", "list", "m"},
		{"SCARD", "list"},
	} {
		h.expect(wrongType, args...)
	}

	// SET is the exception: it replaces a value of any type.
	h.expect(OK, "SET", "list", "now a string")
	h.expect(bulk("now a string"), "GET", "list")
}
