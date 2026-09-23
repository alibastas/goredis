package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrProtocol is wrapped by every error caused by malformed input. Callers
// can detect it with errors.Is and answer with a protocol error before
// closing the connection, like Redis does.
var ErrProtocol = errors.New("protocol error")

const (
	// MaxBulkLen matches Redis's default proto-max-bulk-len.
	MaxBulkLen = 512 * 1024 * 1024
	// MaxArrayLen caps how many elements a single array may declare.
	MaxArrayLen = 1024 * 1024
)

// Reader decodes RESP values from a byte stream.
type Reader struct {
	rd       *bufio.Reader
	noInline bool
}

// ReaderOption changes how a Reader parses its input.
type ReaderOption func(*Reader)

// WithoutInlineCommands makes the reader reject the inline command
// shorthand and insist on proper RESP. It is meant for streams the server
// produced itself, such as the append-only file: inline parsing turns any
// line of text into a command, so a damaged file would quietly replay as
// nonsense instead of being reported.
func WithoutInlineCommands() ReaderOption {
	return func(r *Reader) { r.noInline = true }
}

func NewReader(r io.Reader, opts ...ReaderOption) *Reader {
	rd := &Reader{rd: bufio.NewReader(r)}
	for _, opt := range opts {
		opt(rd)
	}
	return rd
}

// Buffered returns the number of bytes already read from the underlying
// reader but not consumed yet. A server can use it to tell whether a client
// has pipelined more commands that are waiting to be handled.
func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

// ReadValue reads the next value from the stream.
//
// At the top level it also accepts inline commands such as "PING\r\n",
// which is how Redis supports typing commands over telnet or netcat. An
// inline command is returned as an array of bulk strings, exactly as if the
// client had sent it in RESP form. Blank lines are skipped.
//
// It returns io.EOF only if the stream ends cleanly between two values. If
// it ends in the middle of one, the error is io.ErrUnexpectedEOF.
func (r *Reader) ReadValue() (Value, error) {
	for {
		prefix, err := r.rd.Peek(1)
		if err != nil {
			return Value{}, err
		}
		if isTypePrefix(prefix[0]) {
			return r.readValue()
		}
		if r.noInline {
			return Value{}, fmt.Errorf("%w: expected a RESP value, got %q", ErrProtocol, prefix[0])
		}

		v, err := r.readInline()
		if err != nil {
			return Value{}, err
		}
		if len(v.Array) > 0 {
			return v, nil
		}
	}
}

func (r *Reader) readValue() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, fmt.Errorf("%w: empty line", ErrProtocol)
	}

	typ, payload := Type(line[0]), line[1:]
	switch typ {
	case SimpleString, Error:
		return Value{Type: typ, Str: string(payload)}, nil
	case Integer:
		n, err := strconv.ParseInt(string(payload), 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid integer %q", ErrProtocol, payload)
		}
		return NewInteger(n), nil
	case BulkString:
		return r.readBulk(payload)
	case Array:
		return r.readArray(payload)
	}
	return Value{}, fmt.Errorf("%w: unexpected type byte %q", ErrProtocol, line[0])
}

func (r *Reader) readBulk(header []byte) (Value, error) {
	n, err := parseLength(header, MaxBulkLen)
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return NullBulkString(), nil
	}

	// The declared length comes from the client, so don't allocate all of
	// it up front: otherwise a single "$536870912\r\n" line would make us
	// reserve 512 MB before any data has arrived. The builder grows as the
	// bytes actually come in.
	var sb strings.Builder
	sb.Grow(min(n, 4096))
	if _, err := io.CopyN(&sb, r.rd, int64(n)); err != nil {
		return Value{}, unexpectedEOF(err)
	}
	if err := r.expectCRLF(); err != nil {
		return Value{}, err
	}
	return NewBulkString(sb.String()), nil
}

func (r *Reader) readArray(header []byte) (Value, error) {
	n, err := parseLength(header, MaxArrayLen)
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return NullArray(), nil
	}

	// Same reasoning as in readBulk: n is untrusted, so start small.
	elems := make([]Value, 0, min(n, 64))
	for range n {
		v, err := r.readValue()
		if err != nil {
			return Value{}, err
		}
		elems = append(elems, v)
	}
	return Value{Type: Array, Array: elems}, nil
}

func (r *Reader) readInline() (Value, error) {
	line, err := r.readRawLine()
	if err != nil {
		return Value{}, err
	}
	// strings.Fields splits on any run of whitespace, and "\r\n" counts as
	// whitespace, so it also takes care of the line terminator.
	fields := strings.Fields(string(line))
	args := make([]Value, len(fields))
	for i, f := range fields {
		args[i] = NewBulkString(f)
	}
	return Value{Type: Array, Array: args}, nil
}

// readLine returns the next line without its "\r\n" terminator. The slice
// points into the bufio buffer and is only valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.readRawLine()
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("%w: line not terminated by CRLF", ErrProtocol)
	}
	return line[:len(line)-2], nil
}

// readRawLine returns the next line including its "\n". Lines longer than
// the bufio buffer (4 KB) are rejected. Only bulk strings can carry large
// payloads, and those are read by length rather than line by line.
func (r *Reader) readRawLine() ([]byte, error) {
	line, err := r.rd.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, fmt.Errorf("%w: line too long", ErrProtocol)
	}
	if err != nil {
		return nil, unexpectedEOF(err)
	}
	return line, nil
}

func (r *Reader) expectCRLF() error {
	var crlf [2]byte
	if _, err := io.ReadFull(r.rd, crlf[:]); err != nil {
		return unexpectedEOF(err)
	}
	if crlf != [2]byte{'\r', '\n'} {
		return fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
	}
	return nil
}

// parseLength parses the length in a bulk string or array header. -1 is
// allowed because it encodes a null value.
func parseLength(b []byte, limit int) (int, error) {
	n, err := strconv.Atoi(string(b))
	if err != nil || n < -1 || n > limit {
		return 0, fmt.Errorf("%w: invalid length %q", ErrProtocol, b)
	}
	return n, nil
}

// unexpectedEOF converts io.EOF into io.ErrUnexpectedEOF. It is used once
// we are in the middle of a value, where running out of input is an error
// rather than a clean end of stream.
func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func isTypePrefix(b byte) bool {
	switch Type(b) {
	case SimpleString, Error, Integer, BulkString, Array:
		return true
	}
	return false
}
