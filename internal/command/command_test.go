package command

import (
	"reflect"
	"strings"
	"testing"

	"github.com/alibastas/goredis/internal/resp"
)

// cmd builds a request the way a client sends it: an array of bulk strings.
func cmd(args ...string) resp.Value {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString(a)
	}
	return resp.NewArray(elems...)
}

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

	r := NewRegistry()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := r.Dispatch(tt.req); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Dispatch(%+v)\n got: %+v\nwant: %+v", tt.req, got, tt.want)
			}
		})
	}
}
