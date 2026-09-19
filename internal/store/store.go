// Package store is the in-memory keyspace: it holds the data, enforces
// expiry and makes every operation safe to call from many goroutines at
// once. It knows nothing about RESP or networking.
package store

import (
	"errors"
	"hash/maphash"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/alibastas/goredis/internal/glob"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
)

// NoExpiry is returned by TTL for a key that exists but never expires.
const NoExpiry time.Duration = -1

// DefaultShards is the number of shards a Store gets unless told otherwise.
// It was picked with BenchmarkStore on a 20-thread machine, where 256
// shards were about 4x faster than one global lock and more consistent
// than 64. See the README for the numbers.
const DefaultShards = 256

type entry struct {
	value value
	// expiresAt is the moment the key stops existing. The zero Time means
	// the key never expires.
	expiresAt time.Time
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Store is a concurrency-safe key-value map with per-key expiry.
//
// The keyspace is split into shards, each with its own lock, and every key
// always lives in the same shard. Commands on keys in different shards run
// in parallel. Commands that touch several keys lock every shard involved,
// always in ascending shard order, so two such commands can never end up
// waiting for each other (deadlock).
//
// Expired keys are removed in two ways. Lazily: a key whose deadline has
// passed is treated as missing by every operation and deleted the next
// time a write touches it. Actively: RunActiveExpiry samples keys in the
// background and deletes expired ones nobody asks for.
type Store struct {
	shards []*shard
	// seed randomizes which shard a key lands in, differently on every
	// start. With a fixed hash function, a client could craft many keys
	// that all fall into one shard and undo the benefit of sharding.
	seed maphash.Seed
	now  func() time.Time

	// expiryCursor is the shard the next active expiry cycle starts at. It
	// is only touched by the goroutine running active expiry.
	expiryCursor int
}

// Options configures a Store. The zero value gives the defaults.
type Options struct {
	// Shards is the number of shards. Zero means DefaultShards. One shard
	// behaves like a single global lock, which benchmarks compare against.
	Shards int
	// Now reads the current time. Tests pass a fake clock to control
	// expiry without sleeping. Nil means time.Now.
	Now func() time.Time
}

func New() *Store {
	return NewWithOptions(Options{})
}

// NewWithClock creates a store with default options that reads the current
// time from now.
func NewWithClock(now func() time.Time) *Store {
	return NewWithOptions(Options{Now: now})
}

func NewWithOptions(opts Options) *Store {
	if opts.Shards <= 0 {
		opts.Shards = DefaultShards
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Store{
		shards: make([]*shard, opts.Shards),
		seed:   maphash.MakeSeed(),
		now:    opts.Now,
	}
	for i := range s.shards {
		s.shards[i] = newShard()
	}
	return s
}

func (s *Store) shardIndex(key string) int {
	return int(maphash.String(s.seed, key) % uint64(len(s.shards)))
}

func (s *Store) shardFor(key string) *shard {
	return s.shards[s.shardIndex(key)]
}

// lockKeys locks every shard that holds one of keys and returns a function
// that unlocks them again. Each shard is locked once, even if several keys
// share it, and shards are always locked in ascending order: if every
// caller follows the same order, no two callers can each hold a lock the
// other is waiting for.
func (s *Store) lockKeys(keys []string, write bool) (unlock func()) {
	indexes := make([]int, len(keys))
	for i, key := range keys {
		indexes[i] = s.shardIndex(key)
	}
	slices.Sort(indexes)
	return s.lockShards(slices.Compact(indexes), write)
}

// lockAll locks every shard, in order, for operations that need a
// consistent view of the whole keyspace.
func (s *Store) lockAll(write bool) (unlock func()) {
	indexes := make([]int, len(s.shards))
	for i := range indexes {
		indexes[i] = i
	}
	return s.lockShards(indexes, write)
}

func (s *Store) lockShards(indexes []int, write bool) (unlock func()) {
	for _, i := range indexes {
		if write {
			s.shards[i].mu.Lock()
		} else {
			s.shards[i].mu.RLock()
		}
	}
	return func() {
		for _, i := range slices.Backward(indexes) {
			if write {
				s.shards[i].mu.Unlock()
			} else {
				s.shards[i].mu.RUnlock()
			}
		}
	}
}

// Get returns the string stored at key. It fails with ErrWrongType if the
// key holds a hash, list or set.
func (s *Store) Get(key string) (string, bool, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	v, found, err := as[stringValue](sh.lookup(key, s.now()))
	return string(v), found, err
}

// Type returns the name of the type stored at key ("string", "hash",
// "list" or "set"), or "none" if the key does not exist.
func (s *Store) Type(key string) string {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	e, ok := sh.lookup(key, s.now())
	if !ok {
		return "none"
	}
	return e.value.typeName()
}

// SetCondition restricts when Set is allowed to write.
type SetCondition int

const (
	Always      SetCondition = iota // write unconditionally
	IfNotExists                     // NX: only create new keys
	IfExists                        // XX: only overwrite existing keys
)

type SetOptions struct {
	Condition SetCondition
	// TTL makes the key expire after this long. Zero means no expiry.
	TTL time.Duration
	// KeepTTL keeps the expiry of the existing key instead of clearing it.
	KeepTTL bool
}

// Set stores the string val under key. Like in Redis, it replaces whatever
// the key held before, whatever its type. It returns false if the write was
// skipped because opts.Condition did not hold.
func (s *Store) Set(key, val string, opts SetOptions) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := s.now()
	old, exists := sh.lookupForWrite(key, now)
	if (opts.Condition == IfNotExists && exists) || (opts.Condition == IfExists && !exists) {
		return false
	}

	e := entry{value: stringValue(val)}
	switch {
	case opts.TTL > 0:
		e.expiresAt = now.Add(opts.TTL)
	case opts.KeepTTL:
		e.expiresAt = old.expiresAt
	}
	sh.put(key, e)
	return true
}

// Delete removes the given keys and returns how many of them existed. All
// keys are removed atomically: no other command sees some of them gone and
// others still there.
func (s *Store) Delete(keys ...string) int {
	unlock := s.lockKeys(keys, true)
	defer unlock()

	now := s.now()
	deleted := 0
	for _, key := range keys {
		sh := s.shardFor(key)
		if _, ok := sh.lookupForWrite(key, now); ok {
			sh.remove(key)
			deleted++
		}
	}
	return deleted
}

// Exists returns how many of the given keys exist. A key listed twice is
// counted twice, like in Redis.
func (s *Store) Exists(keys ...string) int {
	unlock := s.lockKeys(keys, false)
	defer unlock()

	now := s.now()
	n := 0
	for _, key := range keys {
		if _, ok := s.shardFor(key).lookup(key, now); ok {
			n++
		}
	}
	return n
}

// IncrBy adds delta to the integer stored at key and returns the result.
// A missing key counts as 0. The key's expiry, if any, is kept.
//
// Reading, adding and writing back all happen under one write lock, so
// concurrent increments never lose an update.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, exists := sh.lookupForWrite(key, s.now())
	str, _, err := as[stringValue](e, exists)
	if err != nil {
		return 0, err
	}

	var current int64
	if exists {
		n, err := strconv.ParseInt(string(str), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		current = n
	}

	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	current += delta
	e.value = stringValue(strconv.FormatInt(current, 10))
	sh.put(key, e)
	return current, nil
}

// Expire sets key to expire after ttl. A ttl of zero or less deletes the
// key right away, which is what Redis does too. It returns false if the key
// does not exist.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	now := s.now()
	e, ok := sh.lookupForWrite(key, now)
	if !ok {
		return false
	}
	if ttl <= 0 {
		sh.remove(key)
		return true
	}
	e.expiresAt = now.Add(ttl)
	sh.put(key, e)
	return true
}

// Persist removes the expiry from key. It returns false if the key does not
// exist or had no expiry.
func (s *Store) Persist(key string) bool {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	e, ok := sh.lookupForWrite(key, s.now())
	if !ok || e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	sh.put(key, e)
	return true
}

// TTL returns how long key has left to live. The second result is false if
// the key does not exist. For a key without expiry it returns NoExpiry.
func (s *Store) TTL(key string) (time.Duration, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	now := s.now()
	e, ok := sh.lookup(key, now)
	if !ok {
		return 0, false
	}
	if e.expiresAt.IsZero() {
		return NoExpiry, true
	}
	return e.expiresAt.Sub(now), true
}

// Keys returns every live key matching the glob pattern, in no particular
// order. It scans the whole keyspace, so like in Redis it is meant for
// debugging rather than for production traffic.
func (s *Store) Keys(pattern string) []string {
	unlock := s.lockAll(false)
	defer unlock()

	now := s.now()
	keys := []string{}
	for _, sh := range s.shards {
		for key, e := range sh.data {
			if !e.expired(now) && glob.Match(pattern, key) {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

// Len returns the number of keys in the store. Expired keys that haven't
// been cleaned up yet are included, which matches Redis's DBSIZE.
func (s *Store) Len() int {
	unlock := s.lockAll(false)
	defer unlock()

	n := 0
	for _, sh := range s.shards {
		n += len(sh.data)
	}
	return n
}

// Flush removes every key.
func (s *Store) Flush() {
	unlock := s.lockAll(true)
	defer unlock()

	for _, sh := range s.shards {
		sh.reset()
	}
}
