package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/alibastas/goredis/internal/resp"
)

// formatReply renders a reply the way redis-cli does:
//
//	OK
//	(integer) 5
//	"hello"
//	(nil)
//	1) "a"
//	2) "b"
func formatReply(v resp.Value) string {
	switch v.Type {
	case resp.SimpleString:
		return v.Str
	case resp.Error:
		return "(error) " + v.Str
	case resp.Integer:
		return "(integer) " + strconv.FormatInt(v.Int, 10)
	case resp.BulkString:
		if v.Null {
			return "(nil)"
		}
		return strconv.Quote(v.Str)
	case resp.Array:
		return formatArray(v)
	}
	return fmt.Sprintf("(unknown reply type %q)", byte(v.Type))
}

// formatArray numbers each element. Nested arrays are indented so their
// numbers line up under the parent element:
//
//	> (reply to some command)
//	1) 1) "a"
//	   2) "b"
//	2) (integer) 3
func formatArray(v resp.Value) string {
	if v.Null {
		return "(nil)"
	}
	if len(v.Array) == 0 {
		return "(empty array)"
	}

	// Pad the numbers so that " 9)" and "10)" line up.
	width := len(strconv.Itoa(len(v.Array)))
	var b strings.Builder
	for i, elem := range v.Array {
		prefix := fmt.Sprintf("%*d) ", width, i+1)
		indent := strings.Repeat(" ", len(prefix))

		lines := strings.Split(formatReply(elem), "\n")
		for j, line := range lines {
			if j == 0 {
				b.WriteString(prefix)
			} else {
				b.WriteString(indent)
			}
			b.WriteString(line)
			if i < len(v.Array)-1 || j < len(lines)-1 {
				b.WriteByte('\n')
			}
		}
	}
	return b.String()
}
