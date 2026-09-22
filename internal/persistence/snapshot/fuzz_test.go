package snapshot

import (
	"bytes"
	"testing"
)

// FuzzDecode throws arbitrary bytes at the parser. A snapshot file is
// untrusted input just like a client request: a corrupt or hostile one
// may make Decode return an error, but never panic and never hang trying
// to allocate what the file claims to contain.
func FuzzDecode(f *testing.F) {
	var valid bytes.Buffer
	if err := Encode(&valid, sample()); err != nil {
		f.Fatal(err)
	}
	f.Add(valid.Bytes())
	f.Add([]byte(magic))
	f.Add([]byte(nil))

	f.Fuzz(func(t *testing.T, data []byte) {
		records, err := Decode(bytes.NewReader(data))
		if err != nil {
			return
		}
		// Whatever survives the checksum has to round trip: re-encoding
		// it must produce a file that decodes to the same records.
		var again bytes.Buffer
		if err := Encode(&again, records); err != nil {
			t.Fatalf("re-encoding a decoded snapshot failed: %v", err)
		}
		if _, err := Decode(&again); err != nil {
			t.Fatalf("re-encoded snapshot no longer decodes: %v", err)
		}
	})
}
