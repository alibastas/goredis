package command

import (
	"testing"
	"time"
)

func TestGetSet(t *testing.T) {
	h := newHarness(t)

	h.expect(null, "GET", "k")
	h.expect(OK, "SET", "k", "hello")
	h.expect(bulk("hello"), "GET", "k")
	h.expect(OK, "SET", "k", "")
	h.expect(bulk(""), "GET", "k")
}

func TestSetNXAndXX(t *testing.T) {
	h := newHarness(t)

	h.expect(null, "SET", "k", "v", "XX")
	h.expect(null, "GET", "k")
	h.expect(OK, "SET", "k", "v", "NX")
	h.expect(null, "SET", "k", "other", "nx")
	h.expect(OK, "SET", "k", "new", "XX")
	h.expect(bulk("new"), "GET", "k")
}

func TestSetWithExpiry(t *testing.T) {
	h := newHarness(t)

	h.expect(OK, "SET", "k", "v", "EX", "10")
	h.expect(integer(10), "TTL", "k")
	h.advance(10 * time.Second)
	h.expect(null, "GET", "k")

	h.expect(OK, "SET", "k", "v", "px", "1500")
	h.expect(integer(1500), "PTTL", "k")

	h.expect(OK, "SET", "k", "v2", "KEEPTTL")
	h.expect(integer(1500), "PTTL", "k")

	h.expect(OK, "SET", "k", "v3")
	h.expect(integer(-1), "TTL", "k")
}

func TestSetErrors(t *testing.T) {
	h := newHarness(t)
	syntax := errReply("ERR syntax error")

	h.expect(syntax, "SET", "k", "v", "EX")
	h.expect(syntax, "SET", "k", "v", "NX", "XX")
	h.expect(syntax, "SET", "k", "v", "EX", "1", "PX", "1")
	h.expect(syntax, "SET", "k", "v", "EX", "1", "KEEPTTL")
	h.expect(syntax, "SET", "k", "v", "BOGUS")
	h.expect(errReply("ERR value is not an integer or out of range"), "SET", "k", "v", "EX", "soon")

	invalid := errReply("ERR invalid expire time in 'set' command")
	h.expect(invalid, "SET", "k", "v", "EX", "0")
	h.expect(invalid, "SET", "k", "v", "PX", "-5")
	h.expect(invalid, "SET", "k", "v", "EX", "9223372036854775807")

	// None of the failed commands may have written anything.
	h.expect(null, "GET", "k")
}

func TestIncrDecr(t *testing.T) {
	h := newHarness(t)

	h.expect(integer(1), "INCR", "n")
	h.expect(integer(11), "INCRBY", "n", "10")
	h.expect(integer(10), "DECR", "n")
	h.expect(integer(-5), "DECRBY", "n", "15")
	h.expect(bulk("-5"), "GET", "n")

	notInt := errReply("ERR value is not an integer or out of range")
	h.expect(OK, "SET", "s", "abc")
	h.expect(notInt, "INCR", "s")
	h.expect(notInt, "INCRBY", "n", "ten")

	h.expect(OK, "SET", "max", "9223372036854775807")
	h.expect(errReply("ERR increment or decrement would overflow"), "INCR", "max")
	h.expect(errReply("ERR decrement would overflow"), "DECRBY", "n", "-9223372036854775808")
}
