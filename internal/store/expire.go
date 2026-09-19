package store

import (
	"context"
	"time"
)

// Active expiry follows the algorithm Redis uses. A lookup-only approach
// never frees keys nobody reads again (think of sessions for users who
// never come back), and scanning the whole keyspace would stall the server.
// So instead, several times a second:
//
//  1. sample a few keys that have an expiry,
//  2. delete the ones that are past their deadline,
//  3. if more than a quarter of the sample was expired, the shard is still
//     dirty, so sample it again; otherwise move on.
//
// The work adapts to the load: with few expired keys a cycle costs almost
// nothing, with many it keeps going until the ratio drops. A time budget
// caps how long one cycle may run so it never blocks clients for long.
const (
	expiryInterval   = 100 * time.Millisecond // 10 cycles per second, like Redis's default
	expirySampleSize = 20
	expiryBudget     = 25 * time.Millisecond
)

// RunActiveExpiry deletes expired keys in the background until ctx is
// cancelled. It blocks, so callers run it in its own goroutine.
func (s *Store) RunActiveExpiry(ctx context.Context) {
	ticker := time.NewTicker(expiryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.expireCycle(expiryBudget)
		}
	}
}

// expireCycle runs one round of active expiry over the shards and returns
// how many keys it deleted. It stops early once budget is used up; the next
// cycle then picks up at the shard where this one stopped, so no shard is
// starved.
func (s *Store) expireCycle(budget time.Duration) int {
	deadline := time.Now().Add(budget)
	removed := 0

	for range len(s.shards) {
		sh := s.shards[s.expiryCursor]
		s.expiryCursor = (s.expiryCursor + 1) % len(s.shards)

		for {
			n, sampled := sh.expireSample(s.now(), expirySampleSize)
			removed += n
			if sampled == 0 || n*4 <= sampled {
				break
			}
			if time.Now().After(deadline) {
				return removed
			}
		}
		if time.Now().After(deadline) {
			return removed
		}
	}
	return removed
}

// expireSample looks at up to n keys that have an expiry and deletes those
// that are past it. It returns how many it deleted and how many it looked
// at.
//
// The sample comes from simply ranging over the volatile map: Go starts
// every map iteration at a random position, so the first n keys are a cheap
// random pick. It isn't perfectly uniform, but it doesn't need to be. The
// algorithm only relies on repeated samples eventually visiting every key,
// and Redis's own sampling is approximate in the same way.
func (sh *shard) expireSample(now time.Time, n int) (removed, sampled int) {
	sh.mu.Lock()
	defer sh.mu.Unlock()

	for key := range sh.volatile {
		if sampled == n {
			break
		}
		sampled++
		if sh.data[key].expired(now) {
			// Deleting from a map while ranging over it is allowed in Go.
			sh.remove(key)
			removed++
		}
	}
	return removed, sampled
}
