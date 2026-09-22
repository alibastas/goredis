package store

import (
	"fmt"
	"time"
)

// Kind names the data type of an exported record. Persistence code needs
// to tell the four types apart without seeing the store's internal value
// types, so the export API spells them out as a small enum.
type Kind byte

const (
	KindString Kind = 1
	KindList   Kind = 2
	KindSet    Kind = 3
	KindHash   Kind = 4
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindSet:
		return "set"
	case KindHash:
		return "hash"
	}
	return fmt.Sprintf("kind(%d)", byte(k))
}

// Record is one key and everything stored under it, in a form the store's
// internals don't leak into. Exactly one of Str, Elems and Fields carries
// the data, depending on Kind.
type Record struct {
	Key string
	// ExpiresAt is when the key dies. The zero Time means it never does.
	ExpiresAt time.Time
	Kind      Kind
	// Str holds the value of a string key.
	Str string
	// Elems holds list items in order, or set members in no order.
	Elems []string
	// Fields holds the field/value pairs of a hash.
	Fields map[string]string
}

// Export copies the whole live keyspace into records.
//
// It holds every shard's read lock for the copy, so the result is a
// consistent point-in-time view: no command can be half applied in it.
// The copy is deep enough to be safe afterwards. Maps and lists are
// duplicated, while the strings inside them are shared, which costs
// nothing because strings in Go never change.
//
// Callers that write the records to disk should do that after Export
// returns, not inside it. That way the slow part, the disk, happens with
// no locks held. BGSAVE is exactly this split.
func (s *Store) Export() []Record {
	unlock := s.lockAll(false)
	defer unlock()

	total := 0
	for _, sh := range s.shards {
		total += len(sh.data)
	}

	now := s.now()
	records := make([]Record, 0, total)
	for _, sh := range s.shards {
		for key, e := range sh.data {
			if e.expired(now) {
				continue
			}
			records = append(records, recordOf(key, e))
		}
	}
	return records
}

func recordOf(key string, e entry) Record {
	rec := Record{Key: key, ExpiresAt: e.expiresAt}
	switch v := e.value.(type) {
	case stringValue:
		rec.Kind, rec.Str = KindString, string(v)
	case hashValue:
		rec.Kind = KindHash
		rec.Fields = make(map[string]string, len(v))
		for field, val := range v {
			rec.Fields[field] = val
		}
	case setValue:
		rec.Kind = KindSet
		rec.Elems = make([]string, 0, len(v))
		for member := range v {
			rec.Elems = append(rec.Elems, member)
		}
	case *listValue:
		rec.Kind = KindList
		rec.Elems = make([]string, v.items.Len())
		for i := range rec.Elems {
			rec.Elems[i] = v.items.At(i)
		}
	}
	return rec
}

// Restore replaces the keyspace with records, which is how a server picks
// up where it left off after a restart. Records that have already expired
// by the time they are read are dropped instead of loaded.
//
// An error means the records are not self-consistent, which for a file
// read from disk means it is corrupt. The keyspace is left empty in that
// case rather than half filled.
func (s *Store) Restore(records []Record) error {
	unlock := s.lockAll(true)
	defer unlock()

	for _, sh := range s.shards {
		sh.reset()
	}

	now := s.now()
	for _, rec := range records {
		if !rec.ExpiresAt.IsZero() && !now.Before(rec.ExpiresAt) {
			continue
		}
		v, err := rec.value()
		if err != nil {
			for _, sh := range s.shards {
				sh.reset()
			}
			return fmt.Errorf("key %q: %w", rec.Key, err)
		}
		s.shardFor(rec.Key).put(rec.Key, entry{value: v, expiresAt: rec.ExpiresAt})
	}
	return nil
}

// value rebuilds the internal value a record describes. An empty
// collection is rejected: the store's invariant is that a key disappears
// once its last element is removed, so such a record cannot have been
// written by Export.
func (rec Record) value() (value, error) {
	switch rec.Kind {
	case KindString:
		return stringValue(rec.Str), nil
	case KindList:
		if len(rec.Elems) == 0 {
			return nil, fmt.Errorf("list with no elements")
		}
		lv := &listValue{}
		for _, item := range rec.Elems {
			lv.items.PushBack(item)
		}
		return lv, nil
	case KindSet:
		if len(rec.Elems) == 0 {
			return nil, fmt.Errorf("set with no members")
		}
		sv := make(setValue, len(rec.Elems))
		for _, member := range rec.Elems {
			sv[member] = struct{}{}
		}
		return sv, nil
	case KindHash:
		if len(rec.Fields) == 0 {
			return nil, fmt.Errorf("hash with no fields")
		}
		hv := make(hashValue, len(rec.Fields))
		for field, val := range rec.Fields {
			hv[field] = val
		}
		return hv, nil
	}
	return nil, fmt.Errorf("unknown kind %d", byte(rec.Kind))
}
