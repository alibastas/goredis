package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/server"
	"github.com/alibastas/goredis/internal/store"
)

func TestSplitArgs(t *testing.T) {
	tests := []struct {
		line string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"PING", []string{"PING"}},
		{"  SET   a  b ", []string{"SET", "a", "b"}},
		{`SET msg "hello world"`, []string{"SET", "msg", "hello world"}},
		{`SET msg 'hello world'`, []string{"SET", "msg", "hello world"}},
		{`SET empty ""`, []string{"SET", "empty", ""}},
		{`ECHO "line\nbreak \"quoted\""`, []string{"ECHO", "line\nbreak \"quoted\""}},
		{`ECHO 'it\'s a\b'`, []string{"ECHO", `it's a\b`}},
		{`ECHO "türkçe ğüşiöç"`, []string{"ECHO", "türkçe ğüşiöç"}},
	}

	for _, tt := range tests {
		got, err := splitArgs(tt.line)
		if err != nil {
			t.Errorf("splitArgs(%q) error: %v", tt.line, err)
			continue
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitArgs(%q) = %q, want %q", tt.line, got, tt.want)
		}
	}
}

func TestSplitArgsUnbalancedQuotes(t *testing.T) {
	for _, line := range []string{`SET a "b`, `SET a 'b`, `ECHO "ends with backslash\"`} {
		if _, err := splitArgs(line); !errors.Is(err, errUnbalancedQuotes) {
			t.Errorf("splitArgs(%q) error = %v, want errUnbalancedQuotes", line, err)
		}
	}
}

func TestFormatReply(t *testing.T) {
	var ten []resp.Value
	for range 10 {
		ten = append(ten, resp.NewInteger(0))
	}

	tests := []struct {
		name  string
		reply resp.Value
		want  string
	}{
		{"simple string", resp.NewSimpleString("OK"), "OK"},
		{"error", resp.NewError("ERR boom"), "(error) ERR boom"},
		{"integer", resp.NewInteger(-3), "(integer) -3"},
		{"bulk string", resp.NewBulkString("ali"), `"ali"`},
		{"bulk string with newline", resp.NewBulkString("a\nb"), `"a\nb"`},
		{"null bulk string", resp.NullBulkString(), "(nil)"},
		{"null array", resp.NullArray(), "(nil)"},
		{"empty array", resp.NewArray(), "(empty array)"},
		{
			"flat array",
			resp.NewArray(resp.NewBulkString("a"), resp.NewInteger(1)),
			"1) \"a\"\n2) (integer) 1",
		},
		{
			"nested array",
			resp.NewArray(resp.NewArray(resp.NewBulkString("a"), resp.NewBulkString("b")), resp.NewInteger(3)),
			"1) 1) \"a\"\n   2) \"b\"\n2) (integer) 3",
		},
		{
			"numbers are padded when there are ten or more",
			resp.NewArray(ten...),
			" 1) (integer) 0\n 2) (integer) 0\n 3) (integer) 0\n 4) (integer) 0\n 5) (integer) 0\n" +
				" 6) (integer) 0\n 7) (integer) 0\n 8) (integer) 0\n 9) (integer) 0\n10) (integer) 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatReply(tt.reply); got != tt.want {
				t.Errorf("got:\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

// startServer runs a goredis server on a free port and returns a client
// connected to it.
func startServer(t *testing.T) *client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(command.NewRegistry(store.New()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.Serve(ctx, ln)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return newClient(conn)
}

// TestREPL drives a whole interactive session against a real server.
func TestREPL(t *testing.T) {
	c := startServer(t)

	input := strings.Join([]string{
		`SET greeting "merhaba dünya"`,
		`GET greeting`,
		``,
		`NOSUCHCMD`,
		`SET broken "quote`,
		`INCR n`,
		`quit`,
		`GET greeting`, // never sent: quit ends the session
	}, "\n")

	var out strings.Builder
	if err := repl(c, "test", strings.NewReader(input), &out); err != nil {
		t.Fatalf("repl returned error: %v", err)
	}

	want := strings.Join([]string{
		`test> OK`,
		`test> "merhaba dünya"`,
		`test> test> (error) ERR unknown command 'NOSUCHCMD', with args beginning with: `,
		`test> invalid argument(s): unbalanced quotes`,
		`test> (integer) 1`,
		`test> `,
	}, "\n")
	if got := out.String(); got != want {
		t.Errorf("session output:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestREPLStripsByteOrderMark(t *testing.T) {
	c := startServer(t)

	var out strings.Builder
	if err := repl(c, "test", strings.NewReader(byteOrderMark+"PING\n"), &out); err != nil {
		t.Fatalf("repl returned error: %v", err)
	}
	if want := "test> PONG\ntest> \n"; out.String() != want {
		t.Errorf("got %q, want %q", out.String(), want)
	}
}
