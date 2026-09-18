package store

// SAdd adds members to the set at key, creating the set if needed. It
// returns how many members were not already in the set.
func (s *Store) SAdd(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, found, err := as[setValue](s.lookupForWrite(key, s.now()))
	if err != nil {
		return 0, err
	}
	if !found {
		set = setValue{}
		s.data[key] = entry{value: set}
	}

	added := 0
	for _, m := range members {
		if _, exists := set[m]; !exists {
			set[m] = struct{}{}
			added++
		}
	}
	return added, nil
}

// SRem removes members from the set at key and returns how many were in it.
func (s *Store) SRem(key string, members ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	set, found, err := as[setValue](s.lookupForWrite(key, s.now()))
	if err != nil || !found {
		return 0, err
	}
	removed := 0
	for _, m := range members {
		if _, exists := set[m]; exists {
			delete(set, m)
			removed++
		}
	}
	s.removeIfEmpty(key, len(set))
	return removed, nil
}

// SMembers returns every member of the set at key, in no particular order.
func (s *Store) SMembers(key string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set, _, err := as[setValue](s.lookup(key, s.now()))
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	return out, nil
}

func (s *Store) SIsMember(key, member string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set, _, err := as[setValue](s.lookup(key, s.now()))
	_, ok := set[member]
	return ok, err
}

func (s *Store) SCard(key string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	set, _, err := as[setValue](s.lookup(key, s.now()))
	return len(set), err
}
