package snapshot

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/store"
)

// sample covers every kind, with and without an expiry, plus the values
// that tend to break a hand-written encoder: an empty string and one with
// a NUL byte in it.
func sample() []store.Record {
	return []store.Record{
		{Key: "str", Kind: store.KindString, Str: "hello"},
		{Key: "empty", Kind: store.KindString, Str: ""},
		{Key: "binary", Kind: store.KindString, Str: "a\x00b\r\nc"},
		{Key: "volatile", Kind: store.KindString, Str: "bye",
			ExpiresAt: time.UnixMilli(1893456000000)},
		{Key: "list", Kind: store.KindList, Elems: []string{"a", "b", "c"}},
		{Key: "set", Kind: store.KindSet, Elems: []string{"x", "y"}},
		{Key: "hash", Kind: store.KindHash, Fields: map[string]string{"f1": "v1", "f2": "v2"}},
	}
}

func encodeSample(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Encode(&buf, sample()); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return buf.Bytes()
}

func sortRecords(records []store.Record) {
	slices.SortFunc(records, func(a, b store.Record) int { return strings.Compare(a.Key, b.Key) })
}

func TestRoundTrip(t *testing.T) {
	got, err := Decode(bytes.NewReader(encodeSample(t)))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	want := sample()
	sortRecords(got)
	sortRecords(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip changed the records:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestRoundTripOfAnEmptyKeyspace(t *testing.T) {
	var buf bytes.Buffer
	if err := Encode(&buf, nil); err != nil {
		t.Fatal(err)
	}
	got, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("decoded %d records from an empty snapshot", len(got))
	}
}

// TestDecodeRejectsBadFiles is the point of the checksum and the header:
// a snapshot that is not exactly what was written must be refused rather
// than loaded as if nothing happened.
func TestDecodeRejectsBadFiles(t *testing.T) {
	good := encodeSample(t)

	tests := []struct {
		name string
		make func() []byte
	}{
		{"empty file", func() []byte { return nil }},
		{"another file format", func() []byte { return []byte("REDIS0011 and then some") }},
		{"a future version", func() []byte {
			b := slices.Clone(good)
			b[len(magic)] = version + 1
			return b
		}},
		{"a flipped bit in a value", func() []byte {
			b := slices.Clone(good)
			i := bytes.Index(b, []byte("hello"))
			b[i] ^= 0x20
			return b
		}},
		{"a truncated body", func() []byte { return good[:len(good)/2] }},
		{"a missing checksum", func() []byte { return good[:len(good)-8] }},
		{"a wrong checksum", func() []byte {
			b := slices.Clone(good)
			b[len(b)-1] ^= 0xff
			return b
		}},
		{"trailing junk", func() []byte { return append(slices.Clone(good), 'x') }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(bytes.NewReader(tt.make()))
			if err == nil {
				t.Fatal("Decode accepted a file it should have refused")
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
		})
	}
}

// TestDecodeDoesNotTrustLengths feeds a header that claims a huge value
// without providing the bytes. The decoder must fail instead of reserving
// the memory the file asked for.
func TestDecodeDoesNotTrustLengths(t *testing.T) {
	var buf bytes.Buffer
	e := &encoder{w: &buf}
	e.raw([]byte(magic))
	e.byte(version)
	e.byte(byte(store.KindList))
	e.uvarint(0)
	e.string("k")
	e.uvarint(maxCollection - 1) // a billion elements that are not there
	if e.err != nil {
		t.Fatal(e.err)
	}

	if _, err := Decode(&buf); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Decode = %v, want ErrCorrupt", err)
	}
}

func TestWriteAndReadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dump.goredis")

	if _, err := ReadFile(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ReadFile of a missing snapshot = %v, want ErrNotExist", err)
	}

	if err := WriteFile(path, sample()); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got, err := ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	want := sample()
	sortRecords(got)
	sortRecords(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("file round trip changed the records:\n got: %+v\nwant: %+v", got, want)
	}
}

// TestWriteFileReplacesAtomically checks that saving twice leaves exactly
// one file behind: the temporary one must be gone, not lying around next
// to the snapshot.
func TestWriteFileReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.goredis")

	if err := WriteFile(path, sample()); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(path, []store.Record{{Key: "only", Kind: store.KindString, Str: "1"}}); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "dump.goredis" {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("directory holds %v, want just the snapshot", names)
	}

	got, err := ReadFile(path)
	if err != nil || len(got) != 1 || got[0].Key != "only" {
		t.Fatalf("ReadFile = %+v, %v; want the second write", got, err)
	}
}
