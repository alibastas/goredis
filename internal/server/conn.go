package server

import (
	"log/slog"
	"net"
	"sync"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/resp"
)

// outboxSize is how many values may be waiting to be written to one
// client.
//
// It only matters for a client that is not reading: replies simply make the
// goroutine that produces them wait, but a subscriber that stops reading
// while messages keep being published has to be cut off at some point, or
// the server would buffer them until it runs out of memory. Redis does the
// same thing by size (client-output-buffer-limit, 32mb for pubsub clients
// by default); counting values is cruder but needs no bookkeeping.
const outboxSize = 1024

// clientConn owns the writing half of one client connection.
//
// Everything the server sends to a client goes through out, and a single
// goroutine takes values from there and writes them to the socket. That is
// what makes pushed messages possible at all: without it the connection
// would be written to both by the goroutine reading commands and by
// whichever client published a message, and two goroutines sharing a
// buffered writer produce a stream with their bytes interleaved, which the
// client cannot parse.
type clientConn struct {
	conn     net.Conn
	w        *resp.Writer
	registry *command.Registry
	log      *slog.Logger

	// out carries replies and pushed messages to the writing goroutine. It
	// is never closed: a publisher may try to send on it at any time, and
	// closing a channel while someone else is sending on it panics.
	out chan resp.Value
	// quit is closed by the reading goroutine once it is done, which tells
	// the writer to send what is left and stop.
	quit chan struct{}
	// dead is closed by the writing goroutine when it stops, so that
	// nothing waits forever for a client that can no longer be written to.
	dead chan struct{}

	// hangUp makes sure the connection is only closed once from here, no
	// matter how many publishers notice the client is too slow at the same
	// time.
	hangUp sync.Once
}

func newClientConn(conn net.Conn, registry *command.Registry, log *slog.Logger) *clientConn {
	return &clientConn{
		conn:     conn,
		w:        resp.NewWriter(conn),
		registry: registry,
		log:      log,
		out:      make(chan resp.Value, outboxSize),
		quit:     make(chan struct{}),
		dead:     make(chan struct{}),
	}
}

// Send queues one reply and blocks while the client is too far behind to
// take it. Blocking here is what pushes back on a client that pipelines
// faster than it reads: its own commands stop being processed.
func (c *clientConn) Send(v resp.Value) {
	select {
	case c.out <- v:
	case <-c.dead:
		// The writer has stopped, so nothing more will reach this client.
		// Returning rather than waiting is what lets the reading goroutine
		// notice the connection is gone and finish.
	}
}

// Push queues a message the client subscribed to, and never blocks. A
// client that cannot keep up is disconnected instead, because the goroutine
// calling this one is some other client's PUBLISH and it may have many more
// subscribers to deliver to.
func (c *clientConn) Push(v resp.Value) {
	select {
	case c.out <- v:
	default:
		c.tooSlow()
	}
}

func (c *clientConn) tooSlow() {
	c.hangUp.Do(func() {
		c.log.Warn("dropping a subscriber that is not reading its messages", "queued", cap(c.out))
		// Closing the socket unblocks both of this connection's goroutines:
		// the reader's ReadValue and the writer's next write both fail.
		c.conn.Close()
	})
}

// writeLoop is the only goroutine that writes to the socket. It runs until
// the reading side is done or the connection breaks.
func (c *clientConn) writeLoop() {
	defer close(c.dead)

	for {
		select {
		case v := <-c.out:
			if !c.write(v) {
				return
			}
			// Flush once there is nothing else queued. That is the moment
			// the client would otherwise be kept waiting, and it keeps the
			// property that a batch of pipelined commands is answered with
			// a single write to the socket.
			if len(c.out) == 0 && !c.flush() {
				return
			}
		case <-c.quit:
			c.drain()
			return
		}
	}
}

// drain writes whatever is still queued and flushes it, so that a client
// gets the reply to its last command, or the error explaining why the
// server is hanging up, before the connection closes.
func (c *clientConn) drain() {
	for {
		select {
		case v := <-c.out:
			if !c.write(v) {
				return
			}
		default:
			c.flush()
			return
		}
	}
}

func (c *clientConn) write(v resp.Value) bool {
	if err := c.w.WriteValue(v); err != nil {
		// Only reachable if a handler returns a malformed Value, which is a
		// bug on our side, not the client's.
		c.log.Error("cannot encode reply", "err", err)
		return false
	}
	return true
}

func (c *clientConn) flush() bool {
	// The append-only file goes out first: a client must never be told "OK"
	// for a write the log has not been handed to the operating system, and
	// under the always policy not before it is on the disk. Batching this
	// way also means one pipeline of commands costs one write to the log
	// rather than one per command.
	if err := c.registry.Flush(); err != nil {
		c.log.Error("could not write to the append-only file", "err", err)
	}
	if err := c.w.Flush(); err != nil {
		c.log.Debug("write failed", "err", err)
		return false
	}
	return true
}
