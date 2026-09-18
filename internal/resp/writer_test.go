package resp

import (
	"bytes"
	"testing"
)

func encode(t *testing.T, v Value) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteValue(v); err != nil {
		t.Fatalf("WriteValue(%+v) error: %v", v, err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush error: %v", err)
	}
	return buf.Bytes()
}

func TestWriteValue(t *testing.T) {
	tests := []struct {
		name  string
		value Value
		want  string
	}{
		{"simple string", NewSimpleString("OK"), "+OK\r\n"},
		{"error", NewError("ERR boom"), "-ERR boom\r\n"},
		{"line breaks in error are replaced", NewError("ERR a\r\nb"), "-ERR a  b\r\n"},
		{"integer", NewInteger(-12), ":-12\r\n"},
		{"bulk string", NewBulkString("hello"), "$5\r\nhello\r\n"},
		{"bulk string keeps line breaks", NewBulkString("a\nb"), "$3\r\na\nb\r\n"},
		{"empty bulk string", NewBulkString(""), "$0\r\n\r\n"},
		{"null bulk string", NullBulkString(), "$-1\r\n"},
		{"null array", NullArray(), "*-1\r\n"},
		{"empty array", NewArray(), "*0\r\n"},
		{
			"nested array",
			NewArray(NewBulkString("a"), NewArray(NewInteger(1))),
			"*2\r\n$1\r\na\r\n*1\r\n:1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(encode(t, tt.value)); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWriteValueUnknownType(t *testing.T) {
	var buf bytes.Buffer
	if err := NewWriter(&buf).WriteValue(Value{}); err == nil {
		t.Fatal("expected an error for a zero Value, got nil")
	}
}
