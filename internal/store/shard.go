package store

import (
	"sync"
	"time"
)

// shard is one slice of the keyspace with its own lock. Keys are spread
// over many shards so that operations on unrelated keys don't wait for
// each other.
type shard struct {
	mu   sync.RWMutex
	data map[string]entry
	// volatile holds the keys in data that have an expiry. Active expiry
	// samples from it, so it never spends time on keys that can't expire.
	// Every write to data goes through put and remove, which keep the two
	// maps in sync.
	volatile map[string]struct{}
}

func newShard() *shard {
	return &shard{data: make(map[string]entry), volatile: make(map[string]struct{})}
}

// lookup returns the entry for key if it exists and has not expired. The
// caller must hold sh.mu, for reading or writing.
func (sh *shard) lookup(key string, now time.Time) (entry, bool) {
	e, ok := sh.data[key]
	if !ok || e.expired(now) {
		return entry{}, false
	}
	return e, true
}

// lookupForWrite is lookup for callers holding the write lock: an expired
// entry it runs into is deleted on the spot.
func (sh *shard) lookupForWrite(key string, now time.Time) (entry, bool) {
	e, ok := sh.data[key]
	if ok && e.expired(now) {
		sh.remove(key)
		return entry{}, false
	}
	return e, ok
}

// put stores e under key and records whether the key can expire.
func (sh *shard) put(key string, e entry) {
	sh.data[key] = e
	if e.expiresAt.IsZero() {
		delete(sh.volatile, key)
	} else {
		sh.volatile[key] = struct{}{}
	}
}

func (sh *shard) remove(key string) {
	delete(sh.data, key)
	delete(sh.volatile, key)
}

// removeIfEmpty deletes key once a collection stored under it has no
// elements left. Redis never keeps empty hashes, lists or sets around:
// removing the last element removes the key.
func (sh *shard) removeIfEmpty(key string, size int) {
	if size == 0 {
		sh.remove(key)
	}
}

func (sh *shard) reset() {
	sh.data = make(map[string]entry)
	sh.volatile = make(map[string]struct{})
}
