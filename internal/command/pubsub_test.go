package command

import (
	"reflect"
	"strings"
	"testing"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// conn is one connected client: a session plus everything the server would
// have sent it.
type conn struct {
	t    *testing.T
	r    *Registry
	sess *Session
	out  *sink
	// seen is how many replies the test has already looked at, so that run
	// can return just the ones the latest command produced.
	seen int
}

func connect(t *testing.T, r *Registry) *conn {
	t.Helper()
	out := &sink{}
	c := &conn{t: t, r: r, sess: r.NewSession(out), out: out}
	t.Cleanup(c.sess.Close)
	return c
}

// run sends one command and returns the replies it produced, which is more
// than one for SUBSCRIBE and friends.
func (c *conn) run(args ...string) []resp.Value {
	c.t.Helper()
	c.r.Dispatch(c.sess, cmd(args...))
	replies := c.out.replies[c.seen:]
	c.seen = len(c.out.replies)
	return replies
}

// expect runs a command and compares every reply it produced.
func (c *conn) expect(want []resp.Value, args ...string) {
	c.t.Helper()
	if got := c.run(args...); !reflect.DeepEqual(got, want) {
		c.t.Fatalf("%s\n got: %+v\nwant: %+v", strings.Join(args, " "), got, want)
	}
}

// expectPushed compares the messages delivered to the client, which are the
// values the server pushed rather than replies to its own commands.
func (c *conn) expectPushed(want ...resp.Value) {
	c.t.Helper()
	if got := c.out.pushed; !reflect.DeepEqual(got, want) {
		c.t.Fatalf("pushed messages\n got: %+v\nwant: %+v", got, want)
	}
}

func newRegistry(t *testing.T) *Registry {
	t.Helper()
	return NewRegistry(store.New())
}

// sub builds the reply SUBSCRIBE and UNSUBSCRIBE send: the kind, the
// channel, and how many subscriptions the client has left.
func sub(kind, channel string, count int64) resp.Value {
	return resp.NewArray(bulk(kind), bulk(channel), integer(count))
}

func message(channel, payload string) resp.Value {
	return resp.NewArray(bulk("message"), bulk(channel), bulk(payload))
}

func TestSubscribeRepliesOncePerChannel(t *testing.T) {
	c := connect(t, newRegistry(t))

	c.expect([]resp.Value{
		sub("subscribe", "news", 1),
		sub("subscribe", "sport", 2),
	}, "SUBSCRIBE", "news", "sport")

	// Subscribing again still answers, with the count unchanged.
	c.expect([]resp.Value{sub("subscribe", "news", 2)}, "SUBSCRIBE", "news")
}

func TestUnsubscribe(t *testing.T) {
	c := connect(t, newRegistry(t))
	c.run("SUBSCRIBE", "news", "sport", "weather")

	c.expect([]resp.Value{sub("unsubscribe", "sport", 2)}, "UNSUBSCRIBE", "sport")

	// Without arguments UNSUBSCRIBE means every channel, in a predictable
	// order, with the count counting down.
	c.expect([]resp.Value{
		sub("unsubscribe", "news", 1),
		sub("unsubscribe", "weather", 0),
	}, "UNSUBSCRIBE")
}

func TestUnsubscribeWithNoSubscriptionsStillReplies(t *testing.T) {
	c := connect(t, newRegistry(t))
	want := resp.NewArray(bulk("unsubscribe"), null, integer(0))
	c.expect([]resp.Value{want}, "UNSUBSCRIBE")
}

func TestPublishDeliversToSubscribers(t *testing.T) {
	r := newRegistry(t)
	a, b, publisher := connect(t, r), connect(t, r), connect(t, r)

	a.run("SUBSCRIBE", "news")
	b.run("SUBSCRIBE", "news", "sport")

	publisher.expect([]resp.Value{integer(2)}, "PUBLISH", "news", "hello")
	publisher.expect([]resp.Value{integer(1)}, "PUBLISH", "sport", "goal")
	publisher.expect([]resp.Value{integer(0)}, "PUBLISH", "weather", "rain")

	a.expectPushed(message("news", "hello"))
	b.expectPushed(message("news", "hello"), message("sport", "goal"))

	// A message is never a reply: the subscriber's own command stream is
	// untouched by what was published to it.
	if got := len(a.out.replies); got != 1 {
		t.Errorf("subscriber got %d replies, want only the one for SUBSCRIBE", got)
	}
}

func TestClosingASessionStopsDelivery(t *testing.T) {
	r := newRegistry(t)
	a, publisher := connect(t, r), connect(t, r)
	a.run("SUBSCRIBE", "news")

	a.sess.Close()

	publisher.expect([]resp.Value{integer(0)}, "PUBLISH", "news", "hello")
	a.expectPushed()
}

func TestSubscribedClientMayOnlyRunAFewCommands(t *testing.T) {
	c := connect(t, newRegistry(t))
	c.run("SUBSCRIBE", "news")

	denied := errReply("ERR Can't execute 'get': only (P)SUBSCRIBE / " +
		"(P)UNSUBSCRIBE / PING / QUIT are allowed in this context")
	c.expect([]resp.Value{denied}, "GET", "key")

	// Even publishing is out: the client is there to read messages.
	c.expect([]resp.Value{errReply("ERR Can't execute 'publish': only (P)SUBSCRIBE / " +
		"(P)UNSUBSCRIBE / PING / QUIT are allowed in this context")},
		"PUBLISH", "news", "hi")

	// Unsubscribing from everything ends subscribe mode.
	c.run("UNSUBSCRIBE")
	c.expect([]resp.Value{null}, "GET", "key")
}

func TestPingAnswersInAnArrayWhileSubscribed(t *testing.T) {
	c := connect(t, newRegistry(t))
	c.expect([]resp.Value{resp.NewSimpleString("PONG")}, "PING")

	c.run("SUBSCRIBE", "news")
	c.expect([]resp.Value{resp.NewArray(bulk("pong"), bulk(""))}, "PING")
	c.expect([]resp.Value{resp.NewArray(bulk("pong"), bulk("hi"))}, "PING", "hi")

	c.run("UNSUBSCRIBE")
	c.expect([]resp.Value{bulk("hi")}, "PING", "hi")
}

func TestQuit(t *testing.T) {
	c := connect(t, newRegistry(t))
	if c.sess.QuitRequested() {
		t.Fatal("a fresh session already asked to quit")
	}
	c.expect([]resp.Value{OK}, "QUIT")
	if !c.sess.QuitRequested() {
		t.Error("QUIT did not ask the server to hang up")
	}
}

// The replay path has no client behind it, so the commands that belong to a
// connection must say so rather than panic on a missing session.
func TestSessionCommandsNeedAClient(t *testing.T) {
	r := newRegistry(t)
	for _, args := range [][]string{
		{"SUBSCRIBE", "news"},
		{"UNSUBSCRIBE"},
		{"PUBLISH", "news", "hi"},
		{"QUIT"},
	} {
		got := r.DispatchArgs(args)
		if got.Type != resp.Error || !strings.Contains(got.Str, "needs a client connection") {
			t.Errorf("DispatchArgs(%v) = %+v, want an error about needing a client", args, got)
		}
	}
}
