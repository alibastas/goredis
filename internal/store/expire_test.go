package store

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func volatileCount(s *Store) int {
	n := 0
	for _, sh := range s.shards {
		n += len(sh.volatile)
	}
	return n
}

// Keys that expire but are never read again must still be freed.
func TestActiveExpiryRemovesUntouchedKeys(t *testing.T) {
	s, clock := newTestStore()
	for i := range 5000 {
		s.Set("session:"+strconv.Itoa(i), "x", SetOptions{TTL: time.Second})
	}
	for i := range 100 {
		s.Set("user:"+strconv.Itoa(i), "x", SetOptions{})
	}
	for i := range 100 {
		s.Set("later:"+strconv.Itoa(i), "x", SetOptions{TTL: time.Hour})
	}
	clock.Advance(2 * time.Second)

	if n := s.Len(); n != 5200 {
		t.Fatalf("before active expiry Len = %d, want 5200 (expired keys still in memory)", n)
	}

	// One cycle with a generous budget keeps sampling every shard until
	// fewer than a quarter of its samples are expired.
	s.expireCycle(time.Minute)

	// Sampling is random, so a few expired keys may survive one cycle, but
	// the vast majority must be gone.
	if n := s.Len(); n > 400 {
		t.Fatalf("after one cycle Len = %d, want at most ~400", n)
	}
	for range 50 {
		s.expireCycle(time.Minute)
	}
	if n := s.Len(); n != 200 {
		t.Fatalf("after repeated cycles Len = %d, want 200 (only live keys)", n)
	}
	if n := s.Exists("user:1", "later:1"); n != 2 {
		t.Fatalf("active expiry deleted live keys: Exists = %d, want 2", n)
	}
}

func TestActiveExpiryStopsWithContext(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.RunActiveExpiry(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunActiveExpiry did not return after its context was cancelled")
	}
}

// The volatile index must list exactly the keys that have a TTL, whatever
// sequence of commands produced them.
func TestVolatileIndexStaysInSync(t *testing.T) {
	s, clock := newTestStore()

	steps := []struct {
		name string
		do   func()
		want int
	}{
		{"SET with TTL", func() { s.Set("a", "1", SetOptions{TTL: time.Minute}) }, 1},
		{"SET without TTL clears it", func() { s.Set("a", "2", SetOptions{}) }, 0},
		{"EXPIRE adds it", func() { s.Expire("a", time.Minute) }, 1},
		{"INCR keeps it", func() { s.Set("n", "1", SetOptions{TTL: time.Minute}); s.IncrBy("n", 1) }, 2},
		{"PERSIST removes it", func() { s.Persist("n") }, 1},
		{"KEEPTTL keeps it", func() { s.Set("a", "3", SetOptions{KeepTTL: true}) }, 1},
		{"HSET on a volatile key keeps it", func() { s.HSet("h", "f", "v"); s.Expire("h", time.Minute); s.HSet("h", "g", "w") }, 2},
		{"emptying a collection removes it", func() { s.HDel("h", "f", "g") }, 1},
		{"DEL removes it", func() { s.Delete("a") }, 0},
		{"lazy expiry on write removes it", func() {
			s.Set("b", "1", SetOptions{TTL: time.Second})
			clock.Advance(2 * time.Second)
			s.Set("b", "2", SetOptions{Condition: IfNotExists})
		}, 0},
		{"FLUSHDB clears it", func() { s.Set("c", "1", SetOptions{TTL: time.Minute}); s.Flush() }, 0},
	}

	for _, step := range steps {
		step.do()
		if got := volatileCount(s); got != step.want {
			t.Fatalf("after %q: %d volatile keys, want %d", step.name, got, step.want)
		}
	}
}
