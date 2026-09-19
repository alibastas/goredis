package store

// HSet sets field-value pairs in the hash at key, creating the hash if
// needed. pairs alternates fields and values, so its length must be even.
// It returns how many fields were newly added (as opposed to updated).
func (s *Store) HSet(key string, pairs ...string) (int, error) {
	if len(pairs)%2 != 0 {
		panic("store: HSet needs field-value pairs")
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	h, found, err := as[hashValue](sh.lookupForWrite(key, s.now()))
	if err != nil {
		return 0, err
	}
	if !found {
		h = hashValue{}
		sh.put(key, entry{value: h})
	}

	added := 0
	for i := 0; i < len(pairs); i += 2 {
		field, val := pairs[i], pairs[i+1]
		if _, exists := h[field]; !exists {
			added++
		}
		h[field] = val
	}
	return added, nil
}

func (s *Store) HGet(key, field string) (string, bool, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	h, _, err := as[hashValue](sh.lookup(key, s.now()))
	if err != nil {
		return "", false, err
	}
	// Reading from a nil map is fine in Go, so a missing key needs no
	// special case here.
	val, ok := h[field]
	return val, ok, nil
}

// HDel removes fields from the hash at key and returns how many existed.
func (s *Store) HDel(key string, fields ...string) (int, error) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	h, found, err := as[hashValue](sh.lookupForWrite(key, s.now()))
	if err != nil || !found {
		return 0, err
	}
	deleted := 0
	for _, f := range fields {
		if _, ok := h[f]; ok {
			delete(h, f)
			deleted++
		}
	}
	sh.removeIfEmpty(key, len(h))
	return deleted, nil
}

// HGetAll returns every field and value of the hash at key as one flat
// slice: field1, value1, field2, value2, ... in no particular order.
func (s *Store) HGetAll(key string) ([]string, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	h, _, err := as[hashValue](sh.lookup(key, s.now()))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, 2*len(h))
	for field, val := range h {
		out = append(out, field, val)
	}
	return out, nil
}

func (s *Store) HExists(key, field string) (bool, error) {
	_, ok, err := s.HGet(key, field)
	return ok, err
}

func (s *Store) HLen(key string) (int, error) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	h, _, err := as[hashValue](sh.lookup(key, s.now()))
	return len(h), err
}
