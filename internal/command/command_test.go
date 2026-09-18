package command

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// cmd builds a request the way a client sends it: an array of bulk strings.
func cmd(args ...string) resp.Value {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString(a)
	}
	return resp.NewArray(elems...)
}

// harness runs commands against a fresh registry whose clock only moves
// when the test calls advance.
type harness struct {
	t   *testing.T
	r   *Registry
	now time.Time
}

func newHarness(t *testing.T) *harness {
	h := &harness{t: t, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.r = NewRegistry(store.NewWithClock(func() time.Time { return h.now }))
	return h
}

func (h *harness) advance(d time.Duration) { h.now = h.now.Add(d) }

// expect runs the command given by args and fails the test if the reply is
// not want.
func (h *harness) expect(want resp.Value, args ...string) {
	h.t.Helper()
	if got := h.r.Dispatch(cmd(args...)); !reflect.DeepEqual(got, want) {
		h.t.Fatalf("%s\n got: %+v\nwant: %+v", strings.Join(args, " "), got, want)
	}
}

// Shorthands for the replies that show up in almost every test.
var (
	null = resp.NullBulkString()
	OK   = resp.NewSimpleString("OK")
)

func bulk(s string) resp.Value       { return resp.NewBulkString(s) }
func integer(n int64) resp.Value     { return resp.NewInteger(n) }
func errReply(msg string) resp.Value { return resp.NewError(msg) }

func TestDispatch(t *testing.T) {
	long := strings.Repeat("x", 200)

	tests := []struct {
		name string
		req  resp.Value
		want resp.Value
	}{
		{"ping", cmd("PING"), resp.NewSimpleString("PONG")},
		{"ping with message", cmd("PING", "hi"), resp.NewBulkString("hi")},
		{"command names are case-insensitive", cmd("pInG"), resp.NewSimpleString("PONG")},
		{"echo", cmd("ECHO", "hello world"), resp.NewBulkString("hello world")},
		{"echo empty string", cmd("ECHO", ""), resp.NewBulkString("")},
		{
			"ping with too many arguments",
			cmd("PING", "a", "b"),
			resp.NewError("ERR wrong number of arguments for 'ping' command"),
		},
		{
			"echo without argument",
			cmd("echo"),
			resp.NewError("ERR wrong number of arguments for 'echo' command"),
		},
		{
			"unknown command",
			cmd("NOPE", "a", "b"),
			resp.NewError("ERR unknown command 'NOPE', with args beginning with: 'a' 'b' "),
		},
		{
			"long arguments are truncated in errors",
			cmd("NOPE", long),
			resp.NewError("ERR unknown command 'NOPE', with args beginning with: '" + long[:maxArgInError] + "' "),
		},
		{
			"request is not an array",
			resp.NewSimpleString("PING"),
			resp.NewError("ERR expected a non-empty array of bulk strings"),
		},
		{
			"array contains a non-bulk value",
			resp.NewArray(resp.NewBulkString("ECHO"), resp.NewInteger(1)),
			resp.NewError("ERR expected a non-empty array of bulk strings"),
		},
		{
			"null array",
			resp.NullArray(),
			resp.NewError("ERR expected a non-empty array of bulk strings"),
		},
	}

	r := NewRegistry(store.New())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := r.Dispatch(tt.req); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dispatch(%+v)\n got: %+v\nwant: %+v", tt.req, got, tt.want)
			}
		})
	}
}
