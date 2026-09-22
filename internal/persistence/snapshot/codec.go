package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
)

// encoder writes the primitive pieces of the format and remembers the
// first error it hits. Later calls then do nothing, so the encoding code
// can be written as a straight list of writes and check for failure once
// at the end. bufio.Writer keeps errors the same way.
type encoder struct {
	w   io.Writer
	buf [binary.MaxVarintLen64]byte
	err error
}

func (e *encoder) raw(b []byte) {
	if e.err != nil {
		return
	}
	_, e.err = e.w.Write(b)
}

func (e *encoder) byte(b byte) {
	e.buf[0] = b
	e.raw(e.buf[:1])
}

func (e *encoder) uvarint(v uint64) {
	e.raw(e.buf[:binary.PutUvarint(e.buf[:], v)])
}

func (e *encoder) string(s string) {
	e.uvarint(uint64(len(s)))
	if e.err != nil {
		return
	}
	_, e.err = io.WriteString(e.w, s)
}

// decoder reads those pieces back. It reads a byte at a time where the
// format needs it, which is cheap because the reader underneath is
// buffered, and it never trusts a length it reads.
type decoder struct {
	r   io.Reader
	one [1]byte
}

// ReadByte makes decoder an io.ByteReader, which is what
// binary.ReadUvarint needs: a varint has no length of its own, so it can
// only be read one byte at a time.
func (d *decoder) ReadByte() (byte, error) {
	if _, err := io.ReadFull(d.r, d.one[:]); err != nil {
		return 0, err
	}
	return d.one[0], nil
}

func (d *decoder) raw(b []byte) error {
	_, err := io.ReadFull(d.r, b)
	return err
}

func (d *decoder) uvarint(limit uint64) (uint64, error) {
	v, err := binary.ReadUvarint(d)
	if err != nil {
		return 0, err
	}
	if v > limit {
		return 0, fmt.Errorf("value %d exceeds the limit of %d", v, limit)
	}
	return v, nil
}

func (d *decoder) string() (string, error) {
	n, err := d.uvarint(maxStringLen)
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	if err := d.raw(b); err != nil {
		return "", err
	}
	return string(b), nil
}

// strings reads a counted list. The count is only used as a limit on the
// loop, never to preallocate: a truncated file claiming four billion
// elements would otherwise reserve the memory before failing.
func (d *decoder) strings() ([]string, error) {
	n, err := d.uvarint(maxCollection)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, min(n, 64))
	for range n {
		s, err := d.string()
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (d *decoder) fields() (map[string]string, error) {
	n, err := d.uvarint(maxCollection)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, min(n, 64))
	for range n {
		field, err := d.string()
		if err != nil {
			return nil, err
		}
		val, err := d.string()
		if err != nil {
			return nil, err
		}
		out[field] = val
	}
	return out, nil
}
