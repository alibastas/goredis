package store

// End selects which end of a list an operation works on.
type End int

const (
	Left  End = iota // the head: LPUSH, LPOP
	Right            // the tail: RPUSH, RPOP
)

// Push adds values to one end of the list at key, creating the list if
// needed, and returns the new length. Values are pushed one at a time, so
// pushing a, b, c to the left gives the list c, b, a, as in Redis.
func (s *Store) Push(key string, end End, values ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, found, err := as[*listValue](s.lookupForWrite(key, s.now()))
	if err != nil {
		return 0, err
	}
	if !found {
		l = &listValue{}
		s.data[key] = entry{value: l}
	}

	for _, v := range values {
		if end == Left {
			l.items.PushFront(v)
		} else {
			l.items.PushBack(v)
		}
	}
	return l.items.Len(), nil
}

// Pop removes up to count elements from one end of the list at key and
// returns them in the order they were removed. found is false if the key
// does not exist.
func (s *Store) Pop(key string, end End, count int) (popped []string, found bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	l, found, err := as[*listValue](s.lookupForWrite(key, s.now()))
	if err != nil || !found {
		return nil, found, err
	}

	popped = make([]string, 0, min(count, l.items.Len()))
	for range count {
		var v string
		var ok bool
		if end == Left {
			v, ok = l.items.PopFront()
		} else {
			v, ok = l.items.PopBack()
		}
		if !ok {
			break
		}
		popped = append(popped, v)
	}
	s.removeIfEmpty(key, l.items.Len())
	return popped, true, nil
}

// LRange returns the elements between start and stop, both inclusive.
// Negative indexes count from the end: -1 is the last element. Indexes past
// either end are clamped, and an empty range yields an empty slice rather
// than an error, matching Redis.
func (s *Store) LRange(key string, start, stop int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, found, err := as[*listValue](s.lookup(key, s.now()))
	if err != nil || !found {
		return []string{}, err
	}

	n := l.items.Len()
	if start < 0 {
		start = max(n+start, 0)
	}
	if stop < 0 {
		stop = n + stop
	}
	stop = min(stop, n-1)
	if start > stop {
		return []string{}, nil
	}

	out := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, l.items.At(i))
	}
	return out, nil
}

// LIndex returns the element at index, where negative indexes count from
// the end. ok is false if the index is out of range or the key is missing.
func (s *Store) LIndex(key string, index int) (elem string, ok bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, found, err := as[*listValue](s.lookup(key, s.now()))
	if err != nil || !found {
		return "", false, err
	}
	if index < 0 {
		index += l.items.Len()
	}
	if index < 0 || index >= l.items.Len() {
		return "", false, nil
	}
	return l.items.At(index), true, nil
}

func (s *Store) LLen(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	l, found, err := as[*listValue](s.lookup(key, s.now()))
	if err != nil || !found {
		return 0, err
	}
	return l.items.Len(), nil
}
