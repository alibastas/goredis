package store

import (
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

func sorted(s []string) []string {
	slices.Sort(s)
	return s
}

func TestHash(t *testing.T) {
	s, _ := newTestStore()

	if n, err := s.HSet("user", "name", "ali", "age", "21"); err != nil || n != 2 {
		t.Fatalf("HSet = %d, %v; want 2, nil", n, err)
	}
	// Updating an existing field doesn't count as adding one.
	if n, _ := s.HSet("user", "age", "22", "city", "izmir"); n != 1 {
		t.Fatalf("HSet with one new field = %d, want 1", n)
	}
	if v, ok, _ := s.HGet("user", "age"); !ok || v != "22" {
		t.Fatalf("HGet age = %q, %v; want 22, true", v, ok)
	}
	if _, ok, _ := s.HGet("user", "nope"); ok {
		t.Fatal("HGet on a missing field returned ok")
	}
	if n, _ := s.HLen("user"); n != 3 {
		t.Fatalf("HLen = %d, want 3", n)
	}

	all, _ := s.HGetAll("user")
	if len(all) != 6 {
		t.Fatalf("HGetAll returned %d items, want 6: %v", len(all), all)
	}
	pairs := map[string]string{}
	for i := 0; i < len(all); i += 2 {
		pairs[all[i]] = all[i+1]
	}
	if pairs["name"] != "ali" || pairs["age"] != "22" || pairs["city"] != "izmir" {
		t.Fatalf("HGetAll pairs = %v", pairs)
	}

	if n, _ := s.HDel("user", "name", "age", "nope"); n != 2 {
		t.Fatalf("HDel = %d, want 2", n)
	}
	if ok, _ := s.HExists("user", "city"); !ok {
		t.Fatal("HExists city = false, want true")
	}
}

func TestList(t *testing.T) {
	s, _ := newTestStore()

	s.Push("l", Right, "b", "c")
	if n, _ := s.Push("l", Left, "a", "z"); n != 4 {
		t.Fatalf("Push returned length %d, want 4", n)
	}
	// Left pushes happen one by one, so "z" ends up in front of "a".
	if got, _ := s.LRange("l", 0, -1); !slices.Equal(got, []string{"z", "a", "b", "c"}) {
		t.Fatalf("LRange 0 -1 = %v", got)
	}

	ranges := []struct {
		start, stop int
		want        []string
	}{
		{1, 2, []string{"a", "b"}},
		{-2, -1, []string{"b", "c"}},
		{-100, 1, []string{"z", "a"}},
		{2, 100, []string{"b", "c"}},
		{3, 1, []string{}},
		{10, 20, []string{}},
	}
	for _, r := range ranges {
		if got, _ := s.LRange("l", r.start, r.stop); !slices.Equal(got, r.want) {
			t.Errorf("LRange %d %d = %v, want %v", r.start, r.stop, got, r.want)
		}
	}

	if v, ok, _ := s.LIndex("l", -1); !ok || v != "c" {
		t.Fatalf("LIndex -1 = %q, %v; want c, true", v, ok)
	}
	if _, ok, _ := s.LIndex("l", 4); ok {
		t.Fatal("LIndex past the end returned ok")
	}

	if got, _, _ := s.Pop("l", Left, 1); !slices.Equal(got, []string{"z"}) {
		t.Fatalf("Pop left 1 = %v, want [z]", got)
	}
	if got, _, _ := s.Pop("l", Right, 2); !slices.Equal(got, []string{"c", "b"}) {
		t.Fatalf("Pop right 2 = %v, want [c b]", got)
	}
	if got, found, _ := s.Pop("l", Left, 10); !found || !slices.Equal(got, []string{"a"}) {
		t.Fatalf("Pop more than available = %v, %v; want [a], true", got, found)
	}
	if _, found, _ := s.Pop("l", Left, 1); found {
		t.Fatal("Pop on a list that was emptied still found the key")
	}
}

func TestSet(t *testing.T) {
	s, _ := newTestStore()

	if n, _ := s.SAdd("tags", "go", "redis", "go"); n != 2 {
		t.Fatalf("SAdd with a duplicate = %d, want 2", n)
	}
	if n, _ := s.SAdd("tags", "go", "db"); n != 1 {
		t.Fatalf("SAdd = %d, want 1", n)
	}
	if got, _ := s.SMembers("tags"); !slices.Equal(sorted(got), []string{"db", "go", "redis"}) {
		t.Fatalf("SMembers = %v", got)
	}
	if ok, _ := s.SIsMember("tags", "redis"); !ok {
		t.Fatal("SIsMember redis = false")
	}
	if n, _ := s.SRem("tags", "redis", "nope"); n != 1 {
		t.Fatalf("SRem = %d, want 1", n)
	}
	if n, _ := s.SCard("tags"); n != 2 {
		t.Fatalf("SCard = %d, want 2", n)
	}
}

func TestEmptyCollectionsDisappear(t *testing.T) {
	s, _ := newTestStore()

	s.HSet("h", "f", "v")
	s.HDel("h", "f")
	s.Push("l", Left, "x")
	s.Pop("l", Left, 1)
	s.SAdd("s", "m")
	s.SRem("s", "m")

	for _, key := range []string{"h", "l", "s"} {
		if n := s.Exists(key); n != 0 {
			t.Errorf("%s still exists after its last element was removed", key)
		}
	}
}

func TestType(t *testing.T) {
	s, _ := newTestStore()
	s.Set("str", "v", SetOptions{})
	s.HSet("hash", "f", "v")
	s.Push("list", Left, "v")
	s.SAdd("set", "v")

	for key, want := range map[string]string{
		"str": "string", "hash": "hash", "list": "list", "set": "set", "missing": "none",
	} {
		if got := s.Type(key); got != want {
			t.Errorf("Type(%q) = %q, want %q", key, got, want)
		}
	}
}

// Every operation on a key of the wrong type must fail with ErrWrongType
// and leave the key untouched.
func TestWrongType(t *testing.T) {
	s, _ := newTestStore()
	s.Set("str", "v", SetOptions{})
	s.Push("list", Left, "v")

	errs := map[string]error{}
	record := func(name string, err error) { errs[name] = err }

	_, _, err := s.Get("list")
	record("Get", err)
	_, err = s.IncrBy("list", 1)
	record("IncrBy", err)
	_, err = s.HSet("str", "f", "v")
	record("HSet", err)
	_, _, err = s.HGet("str", "f")
	record("HGet", err)
	_, err = s.HGetAll("str")
	record("HGetAll", err)
	_, err = s.Push("str", Left, "v")
	record("Push", err)
	_, _, err = s.Pop("str", Left, 1)
	record("Pop", err)
	_, err = s.LRange("str", 0, -1)
	record("LRange", err)
	_, err = s.SAdd("list", "m")
	record("SAdd", err)
	_, err = s.SMembers("list")
	record("SMembers", err)

	for name, err := range errs {
		if !errors.Is(err, ErrWrongType) {
			t.Errorf("%s on the wrong type: err = %v, want ErrWrongType", name, err)
		}
	}
	mustGet(t, s, "str", "v")
	if n, _ := s.LLen("list"); n != 1 {
		t.Fatalf("list was modified by a failed command: LLen = %d", n)
	}
}

func TestSetOverwritesAnyType(t *testing.T) {
	s, _ := newTestStore()
	s.Push("k", Left, "v")
	s.Set("k", "now a string", SetOptions{})
	mustGet(t, s, "k", "now a string")
}

func TestCollectionsExpire(t *testing.T) {
	s, clock := newTestStore()
	s.HSet("h", "f", "v")
	s.Expire("h", time.Second)
	clock.Advance(time.Second)

	if n, _ := s.HLen("h"); n != 0 {
		t.Fatalf("HLen on an expired hash = %d, want 0", n)
	}
	// Writing to an expired key starts a fresh hash with no expiry.
	s.HSet("h", "g", "w")
	if ttl, _ := s.TTL("h"); ttl != NoExpiry {
		t.Fatalf("recreated hash inherited TTL %v", ttl)
	}
}

// Concurrent pushes from many goroutines must all land in the list.
func TestConcurrentPushes(t *testing.T) {
	s := New()
	const goroutines, perGoroutine = 20, 500

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perGoroutine {
				s.Push("q", Right, strconv.Itoa(g*perGoroutine+i))
				s.LLen("q")
			}
		}()
	}
	wg.Wait()

	if n, _ := s.LLen("q"); n != goroutines*perGoroutine {
		t.Fatalf("LLen = %d, want %d", n, goroutines*perGoroutine)
	}
}
