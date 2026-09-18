package store

import (
	"errors"
	"math"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeClock is a clock that only moves when the test says so.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestStore() (*Store, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return NewWithClock(clock.Now), clock
}

func mustGet(t *testing.T, s *Store, key, want string) {
	t.Helper()
	got, ok := s.Get(key)
	if !ok || got != want {
		t.Fatalf("Get(%q) = %q, %v; want %q, true", key, got, ok, want)
	}
}

func mustBeMissing(t *testing.T, s *Store, key string) {
	t.Helper()
	if got, ok := s.Get(key); ok {
		t.Fatalf("Get(%q) = %q, want missing key", key, got)
	}
}

func TestSetAndGet(t *testing.T) {
	s, _ := newTestStore()

	mustBeMissing(t, s, "k")
	s.Set("k", "v1", SetOptions{})
	mustGet(t, s, "k", "v1")
	s.Set("k", "v2", SetOptions{})
	mustGet(t, s, "k", "v2")
}

func TestSetConditions(t *testing.T) {
	s, _ := newTestStore()

	if s.Set("k", "v", SetOptions{Condition: IfExists}) {
		t.Fatal("XX set a key that did not exist")
	}
	mustBeMissing(t, s, "k")

	if !s.Set("k", "v", SetOptions{Condition: IfNotExists}) {
		t.Fatal("NX refused to create a new key")
	}
	if s.Set("k", "other", SetOptions{Condition: IfNotExists}) {
		t.Fatal("NX overwrote an existing key")
	}
	mustGet(t, s, "k", "v")

	if !s.Set("k", "new", SetOptions{Condition: IfExists}) {
		t.Fatal("XX refused to overwrite an existing key")
	}
	mustGet(t, s, "k", "new")
}

func TestKeysExpire(t *testing.T) {
	s, clock := newTestStore()

	s.Set("k", "v", SetOptions{TTL: 10 * time.Second})
	clock.Advance(9 * time.Second)
	mustGet(t, s, "k", "v")

	clock.Advance(time.Second)
	mustBeMissing(t, s, "k")
	if n := s.Exists("k"); n != 0 {
		t.Fatalf("Exists on expired key = %d, want 0", n)
	}
}

func TestExpiredKeyCountsAsMissingForWrites(t *testing.T) {
	s, clock := newTestStore()
	s.Set("k", "v", SetOptions{TTL: time.Second})
	clock.Advance(2 * time.Second)

	if !s.Set("k", "fresh", SetOptions{Condition: IfNotExists}) {
		t.Fatal("NX should be able to replace an expired key")
	}
	if ttl, _ := s.TTL("k"); ttl != NoExpiry {
		t.Fatalf("new key inherited the old expiry: TTL = %v", ttl)
	}
}

func TestSetClearsOrKeepsTTL(t *testing.T) {
	s, _ := newTestStore()

	s.Set("k", "v", SetOptions{TTL: time.Minute})
	s.Set("k", "v2", SetOptions{KeepTTL: true})
	if ttl, _ := s.TTL("k"); ttl != time.Minute {
		t.Fatalf("KEEPTTL: TTL = %v, want 1m", ttl)
	}

	s.Set("k", "v3", SetOptions{})
	if ttl, _ := s.TTL("k"); ttl != NoExpiry {
		t.Fatalf("plain SET should clear the TTL, got %v", ttl)
	}
}

func TestExpirePersistAndTTL(t *testing.T) {
	s, clock := newTestStore()

	if s.Expire("missing", time.Second) {
		t.Fatal("Expire on a missing key returned true")
	}
	if _, ok := s.TTL("missing"); ok {
		t.Fatal("TTL reported a missing key as existing")
	}

	s.Set("k", "v", SetOptions{})
	if ttl, ok := s.TTL("k"); !ok || ttl != NoExpiry {
		t.Fatalf("TTL = %v, %v; want NoExpiry, true", ttl, ok)
	}
	if s.Persist("k") {
		t.Fatal("Persist on a key without expiry returned true")
	}

	s.Expire("k", 30*time.Second)
	clock.Advance(10 * time.Second)
	if ttl, _ := s.TTL("k"); ttl != 20*time.Second {
		t.Fatalf("TTL = %v, want 20s", ttl)
	}

	if !s.Persist("k") {
		t.Fatal("Persist on a volatile key returned false")
	}
	clock.Advance(time.Hour)
	mustGet(t, s, "k", "v")

	if !s.Expire("k", 0) {
		t.Fatal("Expire with ttl 0 on an existing key returned false")
	}
	mustBeMissing(t, s, "k")
}

func TestDeleteAndExists(t *testing.T) {
	s, _ := newTestStore()
	s.Set("a", "1", SetOptions{})
	s.Set("b", "2", SetOptions{})

	if n := s.Exists("a", "a", "b", "c"); n != 3 {
		t.Fatalf("Exists = %d, want 3 (duplicates count)", n)
	}
	if n := s.Delete("a", "a", "c"); n != 1 {
		t.Fatalf("Delete = %d, want 1", n)
	}
	mustBeMissing(t, s, "a")
	mustGet(t, s, "b", "2")
}

func TestIncrBy(t *testing.T) {
	s, clock := newTestStore()

	if n, err := s.IncrBy("counter", 1); err != nil || n != 1 {
		t.Fatalf("IncrBy on missing key = %d, %v; want 1, nil", n, err)
	}
	if n, err := s.IncrBy("counter", -5); err != nil || n != -4 {
		t.Fatalf("IncrBy -5 = %d, %v; want -4, nil", n, err)
	}
	mustGet(t, s, "counter", "-4")

	s.Expire("counter", time.Minute)
	s.IncrBy("counter", 1)
	clock.Advance(time.Minute)
	mustBeMissing(t, s, "counter") // INCR must keep the TTL

	for _, bad := range []string{"abc", "", "1.5", " 1"} {
		s.Set("k", bad, SetOptions{})
		if _, err := s.IncrBy("k", 1); !errors.Is(err, ErrNotInteger) {
			t.Errorf("IncrBy on %q: err = %v, want ErrNotInteger", bad, err)
		}
	}

	s.Set("max", strconv.FormatInt(math.MaxInt64, 10), SetOptions{})
	if _, err := s.IncrBy("max", 1); !errors.Is(err, ErrOverflow) {
		t.Errorf("IncrBy past MaxInt64: err = %v, want ErrOverflow", err)
	}
	s.Set("min", strconv.FormatInt(math.MinInt64, 10), SetOptions{})
	if _, err := s.IncrBy("min", -1); !errors.Is(err, ErrOverflow) {
		t.Errorf("IncrBy past MinInt64: err = %v, want ErrOverflow", err)
	}
}

func TestKeys(t *testing.T) {
	s, clock := newTestStore()
	s.Set("user:1", "a", SetOptions{})
	s.Set("user:2", "b", SetOptions{})
	s.Set("session:1", "c", SetOptions{})
	s.Set("user:3", "d", SetOptions{TTL: time.Second})
	clock.Advance(time.Second)

	got := s.Keys("user:*")
	slices.Sort(got)
	if want := []string{"user:1", "user:2"}; !slices.Equal(got, want) {
		t.Fatalf("Keys(user:*) = %v, want %v", got, want)
	}
	if got := s.Keys("nothing*"); len(got) != 0 {
		t.Fatalf("Keys(nothing*) = %v, want empty", got)
	}
}

func TestFlush(t *testing.T) {
	s, _ := newTestStore()
	s.Set("a", "1", SetOptions{})
	s.Flush()
	if n := s.Len(); n != 0 {
		t.Fatalf("Len after Flush = %d, want 0", n)
	}
}

// Many goroutines increment the same counter. If IncrBy were not atomic,
// some increments would overwrite each other and the total would come up
// short. Run with -race to also catch unsynchronized map access.
func TestConcurrentIncrements(t *testing.T) {
	s := New()
	const goroutines, perGoroutine = 50, 1000

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perGoroutine {
				s.IncrBy("counter", 1)
				s.Get("counter")
			}
		}()
	}
	wg.Wait()

	mustGet(t, s, "counter", strconv.Itoa(goroutines*perGoroutine))
}
