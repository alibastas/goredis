package resp

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestReadValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{"simple string", "+OK\r\n", NewSimpleString("OK")},
		{"empty simple string", "+\r\n", NewSimpleString("")},
		{"error", "-ERR unknown command\r\n", NewError("ERR unknown command")},
		{"integer", ":42\r\n", NewInteger(42)},
		{"negative integer", ":-7\r\n", NewInteger(-7)},
		{"bulk string", "$5\r\nhello\r\n", NewBulkString("hello")},
		{"empty bulk string", "$0\r\n\r\n", NewBulkString("")},
		{"bulk string containing CRLF", "$4\r\na\r\nb\r\n", NewBulkString("a\r\nb")},
		{"null bulk string", "$-1\r\n", NullBulkString()},
		{"null array", "*-1\r\n", NullArray()},
		{"empty array", "*0\r\n", Value{Type: Array, Array: []Value{}}},
		{
			"command array",
			"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
			NewArray(NewBulkString("GET"), NewBulkString("key")),
		},
		{
			"nested array with mixed types",
			"*2\r\n:1\r\n*1\r\n+x\r\n",
			NewArray(NewInteger(1), NewArray(NewSimpleString("x"))),
		},
		{"inline command", "PING\r\n", NewArray(NewBulkString("PING"))},
		{
			"inline command with extra spaces and LF only",
			"SET  a   b\n",
			NewArray(NewBulkString("SET"), NewBulkString("a"), NewBulkString("b")),
		},
		{"blank lines before inline command", "\r\n  \r\nPING\r\n", NewArray(NewBulkString("PING"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tt.input)).ReadValue()
			if err != nil {
				t.Fatalf("ReadValue(%q) error: %v", tt.input, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadValue(%q)\n got: %+v\nwant: %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestReadValueErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr error
	}{
		{"empty input", "", io.EOF},
		{"only blank lines", "\r\n\r\n", io.EOF},
		{"missing CR", "+OK\n", ErrProtocol},
		{"invalid integer", ":12a\r\n", ErrProtocol},
		{"non-numeric bulk length", "$abc\r\n", ErrProtocol},
		{"negative bulk length", "$-2\r\n", ErrProtocol},
		{"bulk length over limit", "$536870913\r\n", ErrProtocol},
		{"bulk longer than declared", "$3\r\nabcd\r\n", ErrProtocol},
		{"array length over limit", "*1048577\r\n", ErrProtocol},
		{"unknown type inside array", "*1\r\n?\r\n", ErrProtocol},
		{"empty line inside array", "*2\r\n\r\n", ErrProtocol},
		{"line too long", strings.Repeat("a", 5000), ErrProtocol},
		{"truncated line", "+OK", io.ErrUnexpectedEOF},
		{"truncated bulk string", "$5\r\nhel", io.ErrUnexpectedEOF},
		{"bulk string missing terminator", "$3\r\nabc", io.ErrUnexpectedEOF},
		{"truncated array", "*2\r\n:1\r\n", io.ErrUnexpectedEOF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.input)).ReadValue()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("ReadValue(%q) error = %v, want %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestReadValuePipelined(t *testing.T) {
	r := NewReader(strings.NewReader("+first\r\n:2\r\nPING\r\n"))
	want := []Value{
		NewSimpleString("first"),
		NewInteger(2),
		NewArray(NewBulkString("PING")),
	}

	for i, w := range want {
		got, err := r.ReadValue()
		if err != nil {
			t.Fatalf("value %d: unexpected error: %v", i, err)
		}
		if !reflect.DeepEqual(got, w) {
			t.Errorf("value %d: got %+v, want %+v", i, got, w)
		}
	}
	if _, err := r.ReadValue(); err != io.EOF {
		t.Errorf("after last value: error = %v, want io.EOF", err)
	}
}

// TestWithoutInlineCommands covers the reader used for the append-only
// file, where a line that is not RESP means the file is damaged rather
// than that someone is typing commands over telnet.
func TestWithoutInlineCommands(t *testing.T) {
	strict := NewReader(strings.NewReader("PING\r\n"), WithoutInlineCommands())
	if _, err := strict.ReadValue(); !errors.Is(err, ErrProtocol) {
		t.Fatalf("strict reader accepted an inline command: err = %v", err)
	}

	// Proper RESP still works, and so does the default reader.
	strict = NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n"), WithoutInlineCommands())
	v, err := strict.ReadValue()
	if err != nil || len(v.Array) != 1 || v.Array[0].Str != "PING" {
		t.Fatalf("strict reader on RESP = %+v, %v", v, err)
	}
	if _, err := NewReader(strings.NewReader("PING\r\n")).ReadValue(); err != nil {
		t.Fatalf("default reader rejected an inline command: %v", err)
	}
}
