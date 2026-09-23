package main

import (
	"fmt"
	"strconv"
	"strings"
)

// byteSize is a flag value that accepts sizes the way a configuration file
// would write them: 64mb rather than 67108864.
//
// The flag package takes any type with String and Set methods (the
// flag.Value interface), which is how a flag gets a format of its own
// without the parsing leaking into the rest of the program.
type byteSize int64

// sizeUnits runs from the largest unit to the smallest, which both loops
// below rely on: Set has to try "gb" before the "b" that ends it, and
// String reports the largest unit the number divides into evenly.
var sizeUnits = []struct {
	suffix string
	scale  int64
}{
	{"gb", 1 << 30},
	{"mb", 1 << 20},
	{"kb", 1 << 10},
	{"b", 1},
}

func (s *byteSize) Set(text string) error {
	trimmed := strings.ToLower(strings.TrimSpace(text))
	for _, unit := range sizeUnits {
		digits, found := strings.CutSuffix(trimmed, unit.suffix)
		if !found {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSpace(digits), 10, 64)
		if err != nil || n < 0 {
			return fmt.Errorf("invalid size %q", text)
		}
		// A size that overflows would silently turn into a small limit.
		if n > (1<<62)/unit.scale {
			return fmt.Errorf("size %q is too large", text)
		}
		*s = byteSize(n * unit.scale)
		return nil
	}

	n, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || n < 0 {
		return fmt.Errorf("invalid size %q, want a number of bytes or something like 64mb", text)
	}
	*s = byteSize(n)
	return nil
}

func (s byteSize) String() string {
	for _, unit := range sizeUnits {
		if unit.scale > 1 && int64(s)%unit.scale == 0 && int64(s) >= unit.scale {
			return strconv.FormatInt(int64(s)/unit.scale, 10) + unit.suffix
		}
	}
	return strconv.FormatInt(int64(s), 10) + "b"
}
