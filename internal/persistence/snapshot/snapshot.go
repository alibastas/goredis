// Package snapshot saves the entire keyspace to one file and reads it
// back, so a restarted server can continue with the data it had.
//
// The format is a small versioned binary encoding of its own rather than
// Redis's RDB. Byte-level RDB compatibility is a lot of work that teaches
// little; what the file does have to get right is refusing to load
// silently corrupted data, which is what the trailing checksum is for.
//
// The layout is:
//
//	"GOREDIS" | version byte | record... | 0x00 | crc64 (8 bytes)
//
// and one record is:
//
//	kind | expiry | key | payload
//
// where kind is one of store.Kind (never 0, which marks the end of the
// records), expiry is a variable-length integer holding Unix
// milliseconds with 0 meaning "never", and every string is written as a
// variable-length length followed by its raw bytes. The payload is the
// string itself for a string key, a count followed by that many strings
// for a list or set, and a count followed by that many field/value pairs
// for a hash.
//
// Variable-length integers (encoding/binary's uvarint) keep small
// numbers small: a length below 128 takes a single byte, which matters
// because the file is mostly lengths.
package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"time"

	"github.com/alibastas/goredis/internal/persistence"
	"github.com/alibastas/goredis/internal/store"
)

const (
	magic   = "GOREDIS"
	version = 1
	// endOfRecords is the kind byte that closes the record list. No real
	// store.Kind uses 0.
	endOfRecords = 0
)

// Limits on what a file may claim, in the same spirit as the RESP
// parser's: a length read from a file is untrusted input, and a corrupt
// or hostile one must not be able to make the server allocate wildly.
const (
	maxStringLen  = 512 << 20 // 512 MB, Redis limit for one value
	maxCollection = 1 << 32   // elements in one list, set or hash
)

// ErrCorrupt reports a snapshot that cannot be trusted: a bad header, a
// truncated body or a checksum that does not match the contents.
var ErrCorrupt = errors.New("snapshot is corrupt")

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

func newChecksum() hash.Hash64 {
	return crc64.New(crc64.MakeTable(crc64.ECMA))
}

// WriteFile saves records to path, replacing any previous snapshot in one
// atomic step.
func WriteFile(path string, records []store.Record) error {
	return persistence.WriteFileAtomic(path, func(w io.Writer) error {
		return Encode(w, records)
	})
}

// ReadFile loads the snapshot stored at path. A missing file is reported
// as os.ErrNotExist so callers can treat a first start as normal.
func ReadFile(path string) ([]store.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Decode(bufio.NewReader(f))
}

// Encode writes records to w in the snapshot format.
func Encode(w io.Writer, records []store.Record) error {
	sum := newChecksum()
	// Everything written to e also goes through the checksum, so the two
	// can never drift apart.
	e := &encoder{w: io.MultiWriter(w, sum)}

	e.raw([]byte(magic))
	e.byte(version)
	for _, rec := range records {
		if err := encodeRecord(e, rec); err != nil {
			return err
		}
	}
	e.byte(endOfRecords)
	if e.err != nil {
		return e.err
	}

	// The checksum itself is not part of what it covers.
	var trailer [8]byte
	binary.BigEndian.PutUint64(trailer[:], sum.Sum64())
	_, err := w.Write(trailer[:])
	return err
}

func encodeRecord(e *encoder, rec Record) error {
	var expiry uint64
	if !rec.ExpiresAt.IsZero() {
		ms := rec.ExpiresAt.UnixMilli()
		if ms <= 0 {
			// A deadline at or before the epoch means the key is long
			// gone; writing 0 would claim it never expires.
			return nil
		}
		expiry = uint64(ms)
	}

	e.byte(byte(rec.Kind))
	e.uvarint(expiry)
	e.string(rec.Key)
	switch rec.Kind {
	case store.KindString:
		e.string(rec.Str)
	case store.KindList, store.KindSet:
		e.uvarint(uint64(len(rec.Elems)))
		for _, elem := range rec.Elems {
			e.string(elem)
		}
	case store.KindHash:
		e.uvarint(uint64(len(rec.Fields)))
		for field, val := range rec.Fields {
			e.string(field)
			e.string(val)
		}
	default:
		return fmt.Errorf("cannot encode key %q: unknown kind %d", rec.Key, byte(rec.Kind))
	}
	return e.err
}

// Record is an alias so callers of this package don't have to import the
// store just to name the type they are passing in.
type Record = store.Record

// Decode reads a snapshot from r. It fails with ErrCorrupt unless the
// whole file is present and its checksum matches.
func Decode(r io.Reader) ([]Record, error) {
	sum := newChecksum()
	// TeeReader feeds the checksum exactly the bytes the decoder consumes,
	// so reading and checksumming stay in step without a second pass.
	d := &decoder{r: io.TeeReader(r, sum)}

	header := make([]byte, len(magic)+1)
	if err := d.raw(header); err != nil {
		return nil, corruptf("reading header: %v", err)
	}
	if string(header[:len(magic)]) != magic {
		return nil, corruptf("not a goredis snapshot")
	}
	if header[len(magic)] != version {
		return nil, corruptf("unsupported format version %d, this build writes %d", header[len(magic)], version)
	}

	var records []Record
	for {
		kind, err := d.ReadByte()
		if err != nil {
			return nil, corruptf("reading record type: %v", err)
		}
		if kind == endOfRecords {
			break
		}
		rec, err := decodeRecord(d, store.Kind(kind))
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}

	// Read the trailer from r rather than from d: the checksum covers
	// everything before it, not itself.
	var trailer [8]byte
	if _, err := io.ReadFull(r, trailer[:]); err != nil {
		return nil, corruptf("reading checksum: %v", err)
	}
	if got, want := sum.Sum64(), binary.BigEndian.Uint64(trailer[:]); got != want {
		return nil, corruptf("checksum mismatch (file says %016x, contents give %016x)", want, got)
	}

	// The checksum covers what the file claims to hold, not what follows
	// it, so anything after the trailer is a sign the file is not what it
	// says it is. Callers reading a snapshot out of a longer stream should
	// hand Decode an io.LimitReader over just the snapshot.
	if _, err := r.Read(trailer[:1]); err != io.EOF {
		return nil, corruptf("unexpected data after the end of the snapshot")
	}
	return records, nil
}

func decodeRecord(d *decoder, kind store.Kind) (Record, error) {
	rec := Record{Kind: kind}

	expiry, err := d.uvarint(maxExpiry)
	if err != nil {
		return rec, corruptf("reading expiry: %v", err)
	}
	if expiry > 0 {
		rec.ExpiresAt = time.UnixMilli(int64(expiry))
	}
	if rec.Key, err = d.string(); err != nil {
		return rec, corruptf("reading key: %v", err)
	}

	switch kind {
	case store.KindString:
		rec.Str, err = d.string()
	case store.KindList, store.KindSet:
		rec.Elems, err = d.strings()
	case store.KindHash:
		rec.Fields, err = d.fields()
	default:
		return rec, corruptf("key %q has unknown kind %d", rec.Key, byte(kind))
	}
	if err != nil {
		return rec, corruptf("reading %s %q: %v", kind, rec.Key, err)
	}
	return rec, nil
}

// maxExpiry caps the deadline a file may claim at the same point the
// server refuses to set one, which keeps a corrupt value from overflowing
// time arithmetic later.
const maxExpiry = store.MaxExpiryMillis
