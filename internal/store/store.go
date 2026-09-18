// Package store is the in-memory keyspace: it holds the data, enforces
// expiry and makes every operation safe to call from many goroutines at
// once. It knows nothing about RESP or networking.
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/alibastas/goredis/internal/glob"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
)

// NoExpiry is returned by TTL for a key that exists but never expires.
const NoExpiry time.Duration = -1

type entry struct {
	value string
	// expiresAt is the moment the key stops existing. The zero Time means
	// the key never expires.
	expiresAt time.Time
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && !now.Before(e.expiresAt)
}

// Store is a concurrency-safe key-value map with per-key expiry.
//
// Expired keys are removed lazily: a key whose deadline has passed is
// treated as missing by every operation, and is physically deleted the next
// time a write touches it. Read operations only hold a read lock, so they
// never delete anything; that way concurrent readers don't block each
// other. Background cleanup of keys nobody touches comes in a later phase.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry
	now  func() time.Time
}

func New() *Store {
	return NewWithClock(time.Now)
}

// NewWithClock creates a store that reads the current time from now. Tests
// use it to control time instead of sleeping.
func NewWithClock(now func() time.Time) *Store {
	return &Store{data: make(map[string]entry), now: now}
}

// lookup returns the entry for key if it exists and has not expired.
// The caller must hold s.mu, for reading or writing.
func (s *Store) lookup(key string, now time.Time) (entry, bool) {
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return entry{}, false
	}
	return e, true
}

// lookupForWrite is lookup for callers holding the write lock: an expired
// entry it runs into is deleted on the spot.
func (s *Store) lookupForWrite(key string, now time.Time) (entry, bool) {
	e, ok := s.data[key]
	if ok && e.expired(now) {
		delete(s.data, key)
		return entry{}, false
	}
	return e, ok
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, s.now())
	return e.value, ok
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

// Set stores value under key. It returns false if the write was skipped
// because opts.Condition did not hold.
func (s *Store) Set(key, value string, opts SetOptions) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	old, exists := s.lookupForWrite(key, now)
	if (opts.Condition == IfNotExists && exists) || (opts.Condition == IfExists && !exists) {
		return false
	}

	e := entry{value: value}
	switch {
	case opts.TTL > 0:
		e.expiresAt = now.Add(opts.TTL)
	case opts.KeepTTL:
		e.expiresAt = old.expiresAt
	}
	s.data[key] = e
	return true
}

// Delete removes the given keys and returns how many of them existed.
func (s *Store) Delete(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	deleted := 0
	for _, key := range keys {
		if _, ok := s.lookupForWrite(key, now); ok {
			delete(s.data, key)
			deleted++
		}
	}
	return deleted
}

// Exists returns how many of the given keys exist. A key listed twice is
// counted twice, like in Redis.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	n := 0
	for _, key := range keys {
		if _, ok := s.lookup(key, now); ok {
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
	s.mu.Lock()
	defer s.mu.Unlock()

	e, exists := s.lookupForWrite(key, s.now())
	var current int64
	if exists {
		n, err := strconv.ParseInt(e.value, 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		current = n
	}

	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	current += delta
	e.value = strconv.FormatInt(current, 10)
	s.data[key] = e
	return current, nil
}

// Expire sets key to expire after ttl. A ttl of zero or less deletes the
// key right away, which is what Redis does too. It returns false if the key
// does not exist.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	e, ok := s.lookupForWrite(key, now)
	if !ok {
		return false
	}
	if ttl <= 0 {
		delete(s.data, key)
		return true
	}
	e.expiresAt = now.Add(ttl)
	s.data[key] = e
	return true
}

// Persist removes the expiry from key. It returns false if the key does not
// exist or had no expiry.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.lookupForWrite(key, s.now())
	if !ok || e.expiresAt.IsZero() {
		return false
	}
	e.expiresAt = time.Time{}
	s.data[key] = e
	return true
}

// TTL returns how long key has left to live. The second result is false if
// the key does not exist. For a key without expiry it returns NoExpiry.
func (s *Store) TTL(key string) (time.Duration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	e, ok := s.lookup(key, now)
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := s.now()
	keys := []string{}
	for key, e := range s.data {
		if !e.expired(now) && glob.Match(pattern, key) {
			keys = append(keys, key)
		}
	}
	return keys
}

// Len returns the number of keys in the store. Until they are cleaned up,
// expired keys are included, which matches Redis's DBSIZE.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Flush removes every key.
func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]entry)
}
