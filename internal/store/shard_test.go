package store

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSameKeyAlwaysMapsToSameShard(t *testing.T) {
	s := NewWithOptions(Options{Shards: 16})
	for i := range 1000 {
		key := "key:" + strconv.Itoa(i)
		if a, b := s.shardIndex(key), s.shardIndex(key); a != b {
			t.Fatalf("key %q mapped to shard %d and then %d", key, a, b)
		}
	}
}

func TestKeysSpreadAcrossShards(t *testing.T) {
	s := NewWithOptions(Options{Shards: 16})
	counts := make([]int, 16)
	for i := range 16000 {
		counts[s.shardIndex("key:"+strconv.Itoa(i))]++
	}
	// With a decent hash each shard gets about 1000 keys. Allow a wide
	// margin; this only catches a badly broken distribution.
	for i, c := range counts {
		if c < 700 || c > 1300 {
			t.Fatalf("shard %d got %d of 16000 keys, distribution is skewed: %v", i, c, counts)
		}
	}
}

func TestMultiKeyCommandsSpanShards(t *testing.T) {
	s := NewWithOptions(Options{Shards: 8})
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		s.Set(keys[i], "v", SetOptions{})
	}

	if n := s.Exists(keys...); n != 50 {
		t.Fatalf("Exists = %d, want 50", n)
	}
	if n := s.Delete(append(keys, keys[0], "missing")...); n != 50 {
		t.Fatalf("Delete = %d, want 50", n)
	}
	if n := s.Len(); n != 0 {
		t.Fatalf("Len after Delete = %d, want 0", n)
	}
}

// Many goroutines delete and check overlapping sets of keys, each listing
// them in a different random order. If shards were locked in the order the
// keys were given, two goroutines could each hold a shard the other one
// needs, and the test would hang. Few shards make that collision likely.
func TestMultiKeyLockingDoesNotDeadlock(t *testing.T) {
	s := NewWithOptions(Options{Shards: 4})
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for g := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(uint64(g), 0))
				for range 2000 {
					shuffled := append([]string(nil), keys...)
					rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
					s.Set(shuffled[0], "v", SetOptions{})
					s.Delete(shuffled[:4]...)
					s.Exists(shuffled[4:]...)
				}
			}()
		}
		wg.Wait()
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("multi-key commands deadlocked")
	}
}

func TestSingleShardStillWorks(t *testing.T) {
	s := NewWithOptions(Options{Shards: 1})
	s.Set("a", "1", SetOptions{})
	s.Push("l", Left, "x")
	if n := s.Delete("a", "l"); n != 2 {
		t.Fatalf("Delete = %d, want 2", n)
	}
}

// BenchmarkStore compares shard counts under parallel load. Shards=1 is
// equivalent to one global lock.
//
//	go test ./internal/store -run '^$' -bench Store -benchtime 2s
func BenchmarkStore(b *testing.B) {
	mixes := []struct {
		name       string
		writeEvery int // every n-th operation is a SET, the rest are GETs
	}{
		{"read-heavy", 10},
		{"write-heavy", 2},
	}

	for _, mix := range mixes {
		for _, shards := range []int{1, 16, 64, 256} {
			b.Run(fmt.Sprintf("%s/shards=%d", mix.name, shards), func(b *testing.B) {
				s := NewWithOptions(Options{Shards: shards})
				keys := make([]string, 100_000)
				for i := range keys {
					keys[i] = "key:" + strconv.Itoa(i)
					s.Set(keys[i], "value", SetOptions{})
				}

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					rng := rand.New(rand.NewPCG(rand.Uint64(), 0))
					for i := 0; pb.Next(); i++ {
						key := keys[rng.IntN(len(keys))]
						if i%mix.writeEvery == 0 {
							s.Set(key, "value", SetOptions{})
						} else {
							s.Get(key)
						}
					}
				})
			})
		}
	}
}
