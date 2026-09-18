package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Writer encodes RESP values into a buffered stream.
//
// Values are collected in memory and only sent when Flush is called (or the
// buffer fills up). This lets a server answer a batch of pipelined commands
// with a single write syscall instead of one per reply.
type Writer struct {
	wr *bufio.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{wr: bufio.NewWriter(w)}
}

// lineBreaks replaces "\r" and "\n" with spaces. Simple strings and errors
// are terminated by "\r\n", so a line break inside one would corrupt the
// stream. Redis sanitizes error messages the same way.
var lineBreaks = strings.NewReplacer("\r", " ", "\n", " ")

// WriteValue encodes v into the buffer.
//
// Errors from the underlying writer are not returned here. bufio.Writer
// remembers the first write error and returns it from every later call,
// so it is reported by Flush. WriteValue itself only fails if v has an
// unknown type.
func (w *Writer) WriteValue(v Value) error {
	switch v.Type {
	case SimpleString, Error:
		w.writeLine(v.Type, lineBreaks.Replace(v.Str))
	case Integer:
		w.writeLine(Integer, strconv.FormatInt(v.Int, 10))
	case BulkString:
		if v.Null {
			w.writeLine(BulkString, "-1")
			break
		}
		w.writeLine(BulkString, strconv.Itoa(len(v.Str)))
		w.wr.WriteString(v.Str)
		w.wr.WriteString("\r\n")
	case Array:
		if v.Null {
			w.writeLine(Array, "-1")
			break
		}
		w.writeLine(Array, strconv.Itoa(len(v.Array)))
		for _, elem := range v.Array {
			if err := w.WriteValue(elem); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("resp: cannot write value of unknown type %q", byte(v.Type))
	}
	return nil
}

// Flush sends everything buffered so far to the underlying writer.
func (w *Writer) Flush() error {
	return w.wr.Flush()
}

func (w *Writer) writeLine(t Type, s string) {
	w.wr.WriteByte(byte(t))
	w.wr.WriteString(s)
	w.wr.WriteString("\r\n")
}
