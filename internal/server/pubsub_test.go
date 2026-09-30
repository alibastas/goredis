package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// sendCmd writes a command the way a client library does, as an array of
// bulk strings.
func (c *client) sendCmd(args ...string) {
	c.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	c.send(b.String())
}

func subReply(kind, channel string, count int64) resp.Value {
	return resp.NewArray(resp.NewBulkString(kind), resp.NewBulkString(channel), resp.NewInteger(count))
}

func messageReply(channel, payload string) resp.Value {
	return resp.NewArray(resp.NewBulkString("message"), resp.NewBulkString(channel), resp.NewBulkString(payload))
}

// The point of pub/sub is that the server writes to a client that is not
// asking for anything, so this is the test that matters most: the
// subscribers sit idle and the message still reaches them.
func TestPublishReachesIdleSubscribers(t *testing.T) {
	addr, _ := startServer(t)

	first, second := dial(t, addr), dial(t, addr)
	for _, c := range []*client{first, second} {
		c.sendCmd("SUBSCRIBE", "news")
		c.expect(subReply("subscribe", "news", 1))
	}

	publisher := dial(t, addr)
	publisher.sendCmd("PUBLISH", "news", "hello")
	publisher.expect(resp.NewInteger(2))

	first.expect(messageReply("news", "hello"))
	second.expect(messageReply("news", "hello"))

	// Unsubscribing takes a client out of the delivery straight away.
	second.sendCmd("UNSUBSCRIBE", "news")
	second.expect(subReply("unsubscribe", "news", 0))

	publisher.sendCmd("PUBLISH", "news", "again")
	publisher.expect(resp.NewInteger(1))
	first.expect(messageReply("news", "again"))
}

// A subscriber that hangs up must stop counting, even though the server only
// notices when it next tries to read from that connection.
func TestSubscriptionsGoAwayWithTheClient(t *testing.T) {
	addr, _ := startServer(t)

	subscriber := dial(t, addr)
	subscriber.sendCmd("SUBSCRIBE", "news")
	subscriber.expect(subReply("subscribe", "news", 1))

	publisher := dial(t, addr)
	publisher.sendCmd("PUBLISH", "news", "hello")
	publisher.expect(resp.NewInteger(1))

	subscriber.conn.Close()

	// The server learns about the closed connection when its read fails, so
	// try until the count drops instead of assuming it already has.
	for i := 0; ; i++ {
		publisher.sendCmd("PUBLISH", "news", "hello")
		got, err := publisher.r.ReadValue()
		if err != nil {
			t.Fatalf("read reply: %v", err)
		}
		if got.Int == 0 {
			return
		}
		if i == 100 {
			t.Fatalf("the subscriber is still counted as %d after hanging up", got.Int)
		}
	}
}

func TestQuitClosesTheConnectionAfterReplying(t *testing.T) {
	addr, _ := startServer(t)
	c := dial(t, addr)

	c.sendCmd("QUIT")
	c.expect(resp.NewSimpleString("OK"))
	c.expectClosed()
}

// A client that subscribes and then stops reading cannot be allowed to make
// the server buffer messages forever, so once its queue is full the server
// hangs up on it. The queue is filled directly here: waiting for a real
// socket to back up would be slow and timing-dependent.
func TestSubscriberThatNeverReadsIsDisconnected(t *testing.T) {
	ours, theirs := net.Pipe()
	defer theirs.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := newClientConn(ours, command.NewRegistry(store.New()), logger)
	// No writeLoop is started, so nothing drains the queue. A small one
	// keeps the test short.
	c.out = make(chan resp.Value, 2)

	// Messages that fit are just queued, and leave the connection alone.
	c.Push(resp.NewInteger(1))
	c.Push(resp.NewInteger(2))
	buf := make([]byte, 1)
	theirs.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := theirs.Read(buf); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the connection should still be open and idle, reading it gave err = %v", err)
	}

	// One more than the queue holds, and the client is hung up on.
	c.Push(resp.NewInteger(3))
	theirs.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := theirs.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("expected the server to close the connection, read gave err = %v", err)
	}
}
