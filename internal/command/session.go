package command

import (
	"github.com/alibastas/goredis/internal/pubsub"
	"github.com/alibastas/goredis/internal/resp"
)

// Sink is everything the server sends to one client. The command layer
// hands values to it instead of writing to a socket, so it stays free of
// networking, and the server can put a buffer and a single writing
// goroutine behind it.
//
// The two methods differ in what they do when the client is behind:
//
// Send is for replies, and may block. The only thing that produces them is
// the client's own stream of commands, so making it wait is exactly right:
// a client that floods the server with pipelined commands without reading
// the answers slows itself down.
//
// Push is for messages the client did not ask for one at a time, and must
// not block. A publisher may have hundreds of subscribers to serve, and one
// of them being slow cannot be allowed to hold up the others; if such a
// client's buffer is full it is disconnected instead.
type Sink interface {
	Send(v resp.Value)
	Push(v resp.Value)
}

// Session is the state that belongs to one client connection. Commands
// like SUBSCRIBE are about a particular client rather than about the
// keyspace, so they need it; GET and SET do not.
type Session struct {
	out Sink
	hub *pubsub.Broker

	// subs is the number of channels this client is subscribed to, as the
	// broker last reported it. It is kept here so that the check every
	// single command has to pass ("is this client in subscribe mode?")
	// does not take the broker's lock. Only the connection's own goroutine
	// touches it, from the commands below.
	subs int

	// quit is set when the client asked to be disconnected.
	quit bool
}

// NewSession creates the per-connection state for one client. The server
// calls it once per connection and passes the session to Dispatch.
func (r *Registry) NewSession(out Sink) *Session {
	return &Session{out: out, hub: r.hub}
}

// Send queues one reply for the client.
func (s *Session) Send(v resp.Value) { s.out.Send(v) }

// Deliver takes a published message and turns it into the push a
// subscribed client expects. It implements pubsub.Subscriber, and is
// called by whichever client's goroutine ran PUBLISH, so it only queues
// the message and returns.
func (s *Session) Deliver(m pubsub.Message) {
	s.out.Push(resp.NewArray(
		resp.NewBulkString("message"),
		resp.NewBulkString(m.Channel),
		resp.NewBulkString(m.Payload),
	))
}

// subscribed reports whether the client is in subscribe mode, which limits
// the commands it may run.
func (s *Session) subscribed() bool { return s.subs > 0 }

// QuitRequested reports whether the client ran QUIT, in which case the
// server closes the connection once the reply is out.
func (s *Session) QuitRequested() bool { return s.quit }

// Close drops every subscription the client had. The server calls it when
// the connection ends; without it the broker would keep delivering
// messages to a socket nobody reads.
func (s *Session) Close() { s.hub.Remove(s) }
