package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// startServer runs a server on a random free port and returns its address.
// The server is shut down automatically when the test ends.
func startServer(t *testing.T) (addr string, shutdown func()) {
	t.Helper()

	// Port 0 asks the OS for any free port, so tests never collide with
	// each other or with a real Redis instance.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(command.NewRegistry(store.New()), logger)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()

	var once sync.Once
	shutdown = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Serve returned an error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("server did not shut down within 5s")
			}
		})
	}
	t.Cleanup(shutdown)
	return ln.Addr().String(), shutdown
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	// A deadline keeps a broken server from hanging the test forever.
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &client{t: t, conn: conn, r: resp.NewReader(conn)}
}

func (c *client) send(raw string) {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, raw); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func (c *client) expect(want resp.Value) {
	c.t.Helper()
	got, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		c.t.Fatalf("got reply %+v, want %+v", got, want)
	}
}

func (c *client) expectClosed() {
	c.t.Helper()
	if _, err := c.r.ReadValue(); !errors.Is(err, io.EOF) {
		c.t.Fatalf("expected the server to close the connection, got err = %v", err)
	}
}

func TestPingAndEcho(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)

	c.send("*1\r\n$4\r\nPING\r\n")
	c.expect(resp.NewSimpleString("PONG"))

	c.send("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
	c.expect(resp.NewBulkString("hello"))

	c.send("*1\r\n$3\r\nFOO\r\n")
	c.expect(resp.NewError("ERR unknown command 'FOO', with args beginning with: "))
}

func TestInlineCommand(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)

	c.send("ECHO hi\r\n")
	c.expect(resp.NewBulkString("hi"))
}

func TestPipelining(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)

	// Three commands in a single write, plus an empty array that Redis
	// ignores without replying.
	c.send("*1\r\n$4\r\nPING\r\n*0\r\n*2\r\n$4\r\nECHO\r\n$1\r\na\r\nPING b\r\n")
	c.expect(resp.NewSimpleString("PONG"))
	c.expect(resp.NewBulkString("a"))
	c.expect(resp.NewBulkString("b"))
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)

	c.send("*1\r\n$abc\r\n")
	c.expect(resp.NewError(`ERR protocol error: invalid length "abc"`))
	c.expectClosed()
}

func TestConcurrentClients(t *testing.T) {
	addr, _ := startServer(t)

	const clients, requests = 50, 100
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// t.Fatal must not be called from a goroutine other than the
			// test's own, so report problems with t.Error and bail out.
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			r := resp.NewReader(conn)
			for range requests {
				if _, err := io.WriteString(conn, "PING\r\n"); err != nil {
					t.Error(err)
					return
				}
				got, err := r.ReadValue()
				if err != nil || !reflect.DeepEqual(got, resp.NewSimpleString("PONG")) {
					t.Errorf("got %+v, %v; want PONG", got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestShutdownClosesOpenConnections(t *testing.T) {
	addr, shutdown := startServer(t)
	c := dial(t, addr)

	c.send("PING\r\n")
	c.expect(resp.NewSimpleString("PONG"))

	shutdown()
	c.expectClosed()

	if conn, err := net.Dial("tcp", addr); err == nil {
		conn.Close()
		t.Fatal("server still accepts connections after shutdown")
	}
}
