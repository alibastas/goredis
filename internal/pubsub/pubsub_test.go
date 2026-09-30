package pubsub

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

// recorder is a subscriber that remembers what it was given.
type recorder struct {
	mu   sync.Mutex
	msgs []Message
}

func (r *recorder) Deliver(m Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
}

func (r *recorder) got() []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Message(nil), r.msgs...)
}

func TestSubscribeCountsChannelsPerSubscriber(t *testing.T) {
	b := New()
	a, c := &recorder{}, &recorder{}

	if got := b.Subscribe(a, "news"); got != 1 {
		t.Errorf("first subscribe returned %d, want 1", got)
	}
	if got := b.Subscribe(a, "sport"); got != 2 {
		t.Errorf("second subscribe returned %d, want 2", got)
	}
	// Subscribing twice to the same channel changes nothing.
	if got := b.Subscribe(a, "news"); got != 2 {
		t.Errorf("repeated subscribe returned %d, want 2", got)
	}
	// Counts are per subscriber, not global.
	if got := b.Subscribe(c, "news"); got != 1 {
		t.Errorf("other subscriber returned %d, want 1", got)
	}

	if got, want := b.Channels(a), []string{"news", "sport"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Channels = %v, want %v", got, want)
	}
	if got := b.Count(a); got != 2 {
		t.Errorf("Count = %d, want 2", got)
	}
}

func TestPublishReachesEverySubscriberOfTheChannel(t *testing.T) {
	b := New()
	a, c, d := &recorder{}, &recorder{}, &recorder{}
	b.Subscribe(a, "news")
	b.Subscribe(c, "news")
	b.Subscribe(d, "sport")

	if got := b.Publish("news", "hello"); got != 2 {
		t.Errorf("Publish reached %d subscribers, want 2", got)
	}
	want := []Message{{Channel: "news", Payload: "hello"}}
	if got := a.got(); !reflect.DeepEqual(got, want) {
		t.Errorf("first subscriber got %+v, want %+v", got, want)
	}
	if got := c.got(); !reflect.DeepEqual(got, want) {
		t.Errorf("second subscriber got %+v, want %+v", got, want)
	}
	if got := d.got(); len(got) != 0 {
		t.Errorf("subscriber of another channel got %+v, want nothing", got)
	}
}

func TestPublishToNobodyIsNotAnError(t *testing.T) {
	b := New()
	if got := b.Publish("news", "hello"); got != 0 {
		t.Errorf("Publish returned %d, want 0", got)
	}
	// A message with no subscribers is dropped, not stored: subscribing
	// afterwards must not produce it.
	a := &recorder{}
	b.Subscribe(a, "news")
	if got := a.got(); len(got) != 0 {
		t.Errorf("a late subscriber got %+v, want nothing", got)
	}
}

func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := New()
	a := &recorder{}
	b.Subscribe(a, "news")
	b.Subscribe(a, "sport")

	if got := b.Unsubscribe(a, "news"); got != 1 {
		t.Errorf("Unsubscribe returned %d, want 1", got)
	}
	// Unsubscribing from a channel the subscriber never had is harmless.
	if got := b.Unsubscribe(a, "weather"); got != 1 {
		t.Errorf("Unsubscribe from an unknown channel returned %d, want 1", got)
	}
	if got := b.Publish("news", "hello"); got != 0 {
		t.Errorf("Publish reached %d subscribers after unsubscribe, want 0", got)
	}
	if got := b.Publish("sport", "goal"); got != 1 {
		t.Errorf("Publish reached %d subscribers, want 1", got)
	}
}

func TestRemoveDropsEverySubscription(t *testing.T) {
	b := New()
	a, c := &recorder{}, &recorder{}
	b.Subscribe(a, "news")
	b.Subscribe(a, "sport")
	b.Subscribe(c, "news")

	b.Remove(a)
	if got := b.Count(a); got != 0 {
		t.Errorf("Count after Remove = %d, want 0", got)
	}
	if got := b.Publish("news", "hello"); got != 1 {
		t.Errorf("Publish reached %d subscribers, want only the one left", got)
	}
	if got := a.got(); len(got) != 0 {
		t.Errorf("removed subscriber got %+v, want nothing", got)
	}
}

// A channel nobody listens to must leave nothing behind, or a client that
// subscribes to a new name every second would grow the table forever.
func TestEmptyChannelsAreForgotten(t *testing.T) {
	b := New()
	a := &recorder{}
	for i := 0; i < 100; i++ {
		channel := fmt.Sprintf("channel:%d", i)
		b.Subscribe(a, channel)
		b.Unsubscribe(a, channel)
	}
	if got := len(b.channels); got != 0 {
		t.Errorf("%d channels left behind, want 0", got)
	}
	if got := len(b.subscribed); got != 0 {
		t.Errorf("%d subscribers left behind, want 0", got)
	}
	b.Remove(a)

	b.Subscribe(a, "news")
	b.Remove(a)
	if got := len(b.channels); got != 0 {
		t.Errorf("%d channels left behind after Remove, want 0", got)
	}
}

// Publishing and subscribing happen on different clients' goroutines at the
// same time, so the broker has to hold up under -race.
func TestConcurrentPublishAndSubscribe(t *testing.T) {
	b := New()
	const clients = 8

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := &recorder{}
			channel := fmt.Sprintf("channel:%d", i%3)
			for n := 0; n < 200; n++ {
				b.Subscribe(s, channel)
				b.Publish(channel, "x")
				b.Unsubscribe(s, channel)
			}
			b.Remove(s)
		}(i)
	}
	wg.Wait()

	if got := len(b.channels); got != 0 {
		t.Errorf("%d channels left behind, want 0", got)
	}
	if got := len(b.subscribed); got != 0 {
		t.Errorf("%d subscribers left behind, want 0", got)
	}
}
