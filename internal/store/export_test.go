package store

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

// fill puts one key of every type into s, two of them with an expiry.
func fill(t *testing.T, s *Store) {
	t.Helper()
	s.Set("str", "hello", SetOptions{})
	s.Set("volatile", "bye", SetOptions{TTL: time.Hour})
	if _, err := s.Push("list", Right, "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SAdd("set", "x", "y"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HSet("hash", "f1", "v1", "f2", "v2"); err != nil {
		t.Fatal(err)
	}
	if !s.Expire("hash", 2*time.Hour) {
		t.Fatal("Expire(hash) = false")
	}
}

func recordsByKey(records []Record) map[string]Record {
	byKey := make(map[string]Record, len(records))
	for _, rec := range records {
		byKey[rec.Key] = rec
	}
	return byKey
}

func TestExport(t *testing.T) {
	s, clock := newTestStore()
	fill(t, s)

	byKey := recordsByKey(s.Export())
	if len(byKey) != 5 {
		t.Fatalf("exported %d keys, want 5", len(byKey))
	}

	if got := byKey["str"]; got.Kind != KindString || got.Str != "hello" || !got.ExpiresAt.IsZero() {
		t.Errorf("str record = %+v", got)
	}
	if got := byKey["list"]; got.Kind != KindList || !slices.Equal(got.Elems, []string{"a", "b", "c"}) {
		t.Errorf("list record = %+v, want elements in order", got)
	}
	if got := byKey["set"]; got.Kind != KindSet || len(got.Elems) != 2 {
		t.Errorf("set record = %+v", got)
	}
	if got := byKey["hash"]; got.Kind != KindHash || !reflect.DeepEqual(got.Fields, map[string]string{"f1": "v1", "f2": "v2"}) {
		t.Errorf("hash record = %+v", got)
	}
	if got, want := byKey["volatile"].ExpiresAt, clock.now.Add(time.Hour); !got.Equal(want) {
		t.Errorf("volatile expires at %v, want %v", got, want)
	}
}

// TestExportSkipsExpired checks that a snapshot never carries keys that
// are already dead, even when active expiry has not reached them yet.
func TestExportSkipsExpired(t *testing.T) {
	s, clock := newTestStore()
	s.Set("alive", "1", SetOptions{TTL: time.Hour})
	s.Set("dead", "1", SetOptions{TTL: time.Minute})

	clock.Advance(2 * time.Minute)

	records := s.Export()
	if len(records) != 1 || records[0].Key != "alive" {
		t.Fatalf("exported %+v, want only the live key", records)
	}
}

// TestExportIsADeepCopy makes sure later writes cannot change records that
// were already handed out. A background save writes them long after the
// locks are gone, so sharing the underlying maps would be a data race.
func TestExportIsADeepCopy(t *testing.T) {
	s, _ := newTestStore()
	fill(t, s)
	before := recordsByKey(s.Export())

	if _, err := s.Push("list", Right, "d"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.HSet("hash", "f1", "changed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SAdd("set", "z"); err != nil {
		t.Fatal(err)
	}

	if got := before["list"].Elems; !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Errorf("exported list changed to %v", got)
	}
	if got := before["hash"].Fields["f1"]; got != "v1" {
		t.Errorf("exported hash field changed to %q", got)
	}
	if got := len(before["set"].Elems); got != 2 {
		t.Errorf("exported set grew to %d members", got)
	}
}

func TestRestore(t *testing.T) {
	src, clock := newTestStore()
	fill(t, src)
	records := src.Export()

	dst := NewWithClock(clock.Now)
	dst.Set("leftover", "gone after restore", SetOptions{})
	if err := dst.Restore(records); err != nil {
		t.Fatal(err)
	}

	mustBeMissing(t, dst, "leftover")
	mustGet(t, dst, "str", "hello")
	mustGet(t, dst, "volatile", "bye")

	items, err := dst.LRange("list", 0, -1)
	if err != nil || !slices.Equal(items, []string{"a", "b", "c"}) {
		t.Errorf("LRange after restore = %v, %v", items, err)
	}
	for field, want := range map[string]string{"f1": "v1", "f2": "v2"} {
		got, ok, err := dst.HGet("hash", field)
		if err != nil || !ok || got != want {
			t.Errorf("HGet(hash, %q) after restore = %q, %v, %v; want %q", field, got, ok, err, want)
		}
	}
	if n, err := dst.HLen("hash"); err != nil || n != 2 {
		t.Errorf("HLen after restore = %d, %v; want 2", n, err)
	}
	if n, err := dst.SCard("set"); err != nil || n != 2 {
		t.Errorf("SCard after restore = %d, %v", n, err)
	}

	if ttl, ok := dst.TTL("hash"); !ok || ttl != 2*time.Hour {
		t.Errorf("TTL(hash) after restore = %v, %v; want 2h", ttl, ok)
	}
	if ttl, ok := dst.TTL("str"); !ok || ttl != NoExpiry {
		t.Errorf("TTL(str) after restore = %v, %v; want no expiry", ttl, ok)
	}
}

// TestRestoreDropsExpired covers a server that was down long enough for
// keys to expire while nothing was running to delete them.
func TestRestoreDropsExpired(t *testing.T) {
	src, clock := newTestStore()
	src.Set("short", "1", SetOptions{TTL: time.Minute})
	src.Set("long", "1", SetOptions{TTL: time.Hour})
	records := src.Export()

	clock.Advance(10 * time.Minute)
	dst := NewWithClock(clock.Now)
	if err := dst.Restore(records); err != nil {
		t.Fatal(err)
	}

	mustBeMissing(t, dst, "short")
	mustGet(t, dst, "long", "1")
	if n := dst.Len(); n != 1 {
		t.Errorf("Len after restore = %d, want 1", n)
	}
}

func TestRestoreRejectsBadRecords(t *testing.T) {
	tests := []struct {
		name   string
		record Record
	}{
		{"unknown kind", Record{Key: "k", Kind: Kind(99)}},
		{"empty list", Record{Key: "k", Kind: KindList}},
		{"empty set", Record{Key: "k", Kind: KindSet}},
		{"empty hash", Record{Key: "k", Kind: KindHash}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := newTestStore()
			if err := s.Restore([]Record{{Key: "ok", Kind: KindString, Str: "v"}, tt.record}); err == nil {
				t.Fatal("Restore accepted a record it should have rejected")
			}
			// A rejected file must not leave half of itself loaded.
			if n := s.Len(); n != 0 {
				t.Errorf("keyspace holds %d keys after a failed restore, want 0", n)
			}
		})
	}
}
