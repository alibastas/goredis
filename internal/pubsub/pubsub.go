// Package pubsub routes published messages to the clients that asked for
// them.
//
// It deals in subscribers, not connections: a subscriber is anything that
// can take a Message. Keeping the protocol and the socket out of here is
// the same split the store has, and it means the routing table can be
// tested without a network.
package pubsub

import (
	"sort"
	"sync"
)

// Message is one delivery to one subscriber.
type Message struct {
	// Channel is the channel the message was published to.
	Channel string
	// Payload is what the publisher sent.
	Payload string
}

// Subscriber receives the messages published to the channels it asked for.
//
// Deliver must not block. It is called by the goroutine running PUBLISH,
// which may still have many subscribers to serve and holds the broker's
// lock while it does; a subscriber that waits for a slow network there
// would hold up everyone else. Implementations hand the message to their
// own buffer and return.
//
// A Subscriber is used as a map key, so it has to be comparable. In
// practice it is a pointer to whatever the caller uses for a client.
type Subscriber interface {
	Deliver(Message)
}

// Broker is the routing table: which subscribers want which channels.
type Broker struct {
	// mu guards both maps. PUBLISH only reads them, and is by far the most
	// common operation, so this is an RWMutex: any number of publishers can
	// look up channels at the same time.
	mu sync.RWMutex
	// channels maps a channel to its subscribers. This is the direction
	// PUBLISH needs.
	channels map[string]map[Subscriber]struct{}
	// subscribed maps a subscriber to its channels. Without it, dropping a
	// client that disconnects would mean walking every channel in the
	// server to look for it.
	subscribed map[Subscriber]map[string]struct{}
}

func New() *Broker {
	return &Broker{
		channels:   make(map[string]map[Subscriber]struct{}),
		subscribed: make(map[Subscriber]map[string]struct{}),
	}
}

// Subscribe adds s to channel and returns how many channels s is now
// subscribed to. Subscribing twice to the same channel changes nothing,
// which matches what clients expect: SUBSCRIBE always answers, whether or
// not it had anything to do.
func (b *Broker) Subscribe(s Subscriber, channel string) int {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.channels[channel]
	if !ok {
		subs = make(map[Subscriber]struct{})
		b.channels[channel] = subs
	}
	subs[s] = struct{}{}

	mine, ok := b.subscribed[s]
	if !ok {
		mine = make(map[string]struct{})
		b.subscribed[s] = mine
	}
	mine[channel] = struct{}{}
	return len(mine)
}

// Unsubscribe removes s from channel and returns how many channels are
// left. Removing a channel s never had is not an error.
func (b *Broker) Unsubscribe(s Subscriber, channel string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unsubscribeLocked(s, channel)
}

func (b *Broker) unsubscribeLocked(s Subscriber, channel string) int {
	if subs, ok := b.channels[channel]; ok {
		delete(subs, s)
		// A channel nobody listens to no longer exists. Leaving the empty
		// map behind would let a client that subscribes to a new channel
		// name every second grow the table forever.
		if len(subs) == 0 {
			delete(b.channels, channel)
		}
	}

	mine := b.subscribed[s]
	delete(mine, channel)
	if len(mine) == 0 {
		delete(b.subscribed, s)
	}
	return len(mine)
}

// Channels returns the channels s is subscribed to, sorted by name.
//
// The order is only there to make replies and tests predictable: a client
// that says UNSUBSCRIBE with no arguments gets one reply per channel, and
// map order would shuffle them on every run.
func (b *Broker) Channels(s Subscriber) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.subscribed[s]))
	for channel := range b.subscribed[s] {
		names = append(names, channel)
	}
	sort.Strings(names)
	return names
}

// Count returns how many channels s is subscribed to.
func (b *Broker) Count(s Subscriber) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribed[s])
}

// Remove drops every subscription s has. It is what a disconnecting client
// needs: without it the broker would keep delivering to a closed socket.
func (b *Broker) Remove(s Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for channel := range b.subscribed[s] {
		if subs, ok := b.channels[channel]; ok {
			delete(subs, s)
			if len(subs) == 0 {
				delete(b.channels, channel)
			}
		}
	}
	delete(b.subscribed, s)
}

// Publish hands payload to everyone subscribed to channel and returns how
// many subscribers it reached. Nothing is stored: a message published to a
// channel with no subscribers is simply gone, and the count says so.
func (b *Broker) Publish(channel, payload string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subs := b.channels[channel]
	msg := Message{Channel: channel, Payload: payload}
	for s := range subs {
		s.Deliver(msg)
	}
	return len(subs)
}
