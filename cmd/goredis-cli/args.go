package main

import (
	"errors"
	"strings"
)

var errUnbalancedQuotes = errors.New("invalid argument(s): unbalanced quotes")

// splitArgs splits a typed command line into arguments. Whitespace
// separates arguments unless it is inside quotes, so
//
//	SET msg "hello world"
//
// becomes ["SET", "msg", "hello world"]. Double quotes understand the
// escapes \n, \r, \t, \\ and \". Single quotes take everything literally
// except \'.
func splitArgs(line string) ([]string, error) {
	var args []string
	var current strings.Builder
	// inArg is tracked separately from current.Len() so that "" still
	// produces an (empty) argument.
	inArg := false

	for i := 0; i < len(line); i++ {
		switch c := line[i]; c {
		case ' ', '\t':
			if inArg {
				args = append(args, current.String())
				current.Reset()
				inArg = false
			}
		case '"', '\'':
			s, end, err := readQuoted(line, i)
			if err != nil {
				return nil, err
			}
			current.WriteString(s)
			inArg = true
			i = end
		default:
			current.WriteByte(c)
			inArg = true
		}
	}
	if inArg {
		args = append(args, current.String())
	}
	return args, nil
}

// readQuoted reads the quoted string that starts at line[start] and returns
// its unescaped contents and the index of the closing quote.
func readQuoted(line string, start int) (s string, end int, err error) {
	quote := line[start]
	var b strings.Builder

	for i := start + 1; i < len(line); i++ {
		c := line[i]
		switch {
		case c == quote:
			return b.String(), i, nil
		case c == '\\' && i+1 < len(line) && (quote == '"' || line[i+1] == '\''):
			i++
			b.WriteByte(unescape(line[i]))
		default:
			b.WriteByte(c)
		}
	}
	return "", 0, errUnbalancedQuotes
}

// unescape returns the byte that the escape sequence \c stands for.
// Unknown escapes like \x just produce x.
func unescape(c byte) byte {
	switch c {
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	}
	return c
}
