package store

import (
	"errors"

	"github.com/alibastas/goredis/internal/deque"
)

// ErrWrongType is returned when a command expects one data type but the
// key holds another, for example LPUSH on a key that holds a string. The
// message is Redis's own, including the WRONGTYPE prefix clients look for.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// value is anything that can be stored under a key. The interface only
// asks for a type name, which is what the TYPE command reports; the store
// uses type assertions to get at the concrete data.
type value interface {
	typeName() string
}

type stringValue string

// hashValue maps field names to values, like a small keyspace of its own.
type hashValue map[string]string

// setValue is a set of unique members. A map with empty-struct values is
// the usual way to write a set in Go: struct{} takes no memory.
type setValue map[string]struct{}

// listValue is an ordered list that grows and shrinks at both ends.
type listValue struct {
	items deque.Deque[string]
}

func (stringValue) typeName() string { return "string" }
func (hashValue) typeName() string   { return "hash" }
func (setValue) typeName() string    { return "set" }
func (*listValue) typeName() string  { return "list" }

// as extracts the concrete value of type T from an entry found by lookup.
// It is written to take lookup's results directly:
//
//	h, found, err := as[hashValue](s.lookup(key, now))
//
// A missing key is not an error: found is simply false. A key holding a
// different type yields ErrWrongType.
func as[T value](e entry, exists bool) (v T, found bool, err error) {
	if !exists {
		return v, false, nil
	}
	v, ok := e.value.(T)
	if !ok {
		return v, false, ErrWrongType
	}
	return v, true, nil
}
