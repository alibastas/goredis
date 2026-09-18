package resp

import (
	"bytes"
	"testing"
)

// FuzzReadValue feeds random bytes to the reader. It checks two things:
// the reader never panics, and anything it accepts survives an
// encode -> decode -> encode round trip with identical bytes.
//
// Run it with: go test ./internal/resp -fuzz=FuzzReadValue -fuzztime=30s
func FuzzReadValue(f *testing.F) {
	seeds := []string{
		"+OK\r\n",
		"-ERR x\r\n",
		":123\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"*-1\r\n",
		"*1\r\n*1\r\n:0\r\n",
		"PING\r\n",
		"SET a b\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := NewReader(bytes.NewReader(data)).ReadValue()
		if err != nil {
			return
		}

		first := encode(t, v)
		v2, err := NewReader(bytes.NewReader(first)).ReadValue()
		if err != nil {
			t.Fatalf("cannot decode our own encoding %q: %v", first, err)
		}
		if second := encode(t, v2); !bytes.Equal(first, second) {
			t.Fatalf("round trip is not stable:\nfirst:  %q\nsecond: %q", first, second)
		}
	})
}
