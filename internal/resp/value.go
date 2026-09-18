// Package resp implements RESP2, the protocol Redis clients and servers use
// to talk to each other.
//
// RESP is a small text-based protocol. Every value starts with a one-byte
// type prefix and every line ends with "\r\n":
//
//	+OK\r\n                       simple string
//	-ERR unknown command\r\n      error
//	:42\r\n                       integer
//	$5\r\nhello\r\n               bulk string (length-prefixed, binary safe)
//	*2\r\n$3\r\nGET\r\n$1\r\nk\r\n  array of two bulk strings
//
// Spec: https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

// Type identifies the kind of a RESP value. Its underlying value is the
// prefix byte used on the wire, so no lookup table is needed to encode it.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

func (t Type) String() string {
	switch t {
	case SimpleString:
		return "simple string"
	case Error:
		return "error"
	case Integer:
		return "integer"
	case BulkString:
		return "bulk string"
	case Array:
		return "array"
	}
	return "unknown"
}

// Value is a single RESP value. Which field is meaningful depends on Type:
// Str for simple strings, errors and bulk strings, Int for integers and
// Array for arrays. Null is only valid for bulk strings and arrays, which is
// how RESP2 represents "no value" (for example GET on a missing key).
type Value struct {
	Type  Type
	Str   string
	Int   int64
	Array []Value
	Null  bool
}

func NewSimpleString(s string) Value { return Value{Type: SimpleString, Str: s} }
func NewError(msg string) Value      { return Value{Type: Error, Str: msg} }
func NewInteger(n int64) Value       { return Value{Type: Integer, Int: n} }
func NewBulkString(s string) Value   { return Value{Type: BulkString, Str: s} }
func NewArray(elems ...Value) Value  { return Value{Type: Array, Array: elems} }
func NullBulkString() Value          { return Value{Type: BulkString, Null: true} }
func NullArray() Value               { return Value{Type: Array, Null: true} }
