package main

import (
	"bufio"
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
// startServer runs a server for the test and returns a connected client, the
// address to open more connections on, and a function that shuts the server
// down early.
func startServer(t *testing.T) (c *client, addr string, shutdown func()) {
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
	shutdown = func() { cancel(); <-done }
	t.Cleanup(shutdown)

	return connect(t, ln.Addr().String()), ln.Addr().String(), shutdown
}

// connect opens one more client connection to the test server.
func connect(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return newClient(conn)
}

// TestREPL drives a whole interactive session against a real server.
func TestREPL(t *testing.T) {
	c, _, _ := startServer(t)

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
	c, _, _ := startServer(t)

	var out strings.Builder
	if err := repl(c, "test", strings.NewReader(byteOrderMark+"PING\n"), &out); err != nil {
		t.Fatalf("repl returned error: %v", err)
	}
	if want := "test> PONG\ntest> \n"; out.String() != want {
		t.Errorf("got %q, want %q", out.String(), want)
	}
}

// TestSubscribeMode checks the other half of the client: after SUBSCRIBE it
// stops waiting for the user and prints whatever the server sends.
func TestSubscribeMode(t *testing.T) {
	c, addr, shutdown := startServer(t)

	// A pipe makes this deterministic: every line the client prints is read
	// here, so the test never has to wait and guess.
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		err := listen(c, pw, []string{"SUBSCRIBE", "news"})
		pw.Close()
		done <- err
	}()

	lines := bufio.NewScanner(pr)
	next := func() string {
		t.Helper()
		if !lines.Scan() {
			t.Fatalf("the client stopped printing: %v", lines.Err())
		}
		return lines.Text()
	}

	if got, want := next(), "Reading messages... (press Ctrl+C to quit)"; got != want {
		t.Errorf("first line = %q, want %q", got, want)
	}
	for _, want := range []string{`1) "subscribe"`, `2) "news"`, `3) (integer) 1`} {
		if got := next(); got != want {
			t.Errorf("subscribe confirmation line = %q, want %q", got, want)
		}
	}

	publisher := connect(t, addr)
	if reply, err := publisher.do([]string{"PUBLISH", "news", "hello"}); err != nil {
		t.Fatalf("publish: %v", err)
	} else if reply.Int != 1 {
		t.Fatalf("PUBLISH reached %d subscribers, want 1", reply.Int)
	}

	for _, want := range []string{`1) "message"`, `2) "news"`, `3) "hello"`} {
		if got := next(); got != want {
			t.Errorf("message line = %q, want %q", got, want)
		}
	}

	// A subscription ends when the server closes the connection, which is
	// not an error for the client.
	shutdown()
	if err := <-done; err != nil {
		t.Errorf("listen returned %v, want nil after the connection closed", err)
	}
}
