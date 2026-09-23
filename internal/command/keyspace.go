package command

import (
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

func (h *handlers) del(args []string) resp.Value {
	return resp.NewInteger(int64(h.db.Delete(args...)))
}

func (h *handlers) exists(args []string) resp.Value {
	return resp.NewInteger(int64(h.db.Exists(args...)))
}

func (h *handlers) expire(args []string) resp.Value {
	return h.setExpiry(args, time.Second, "expire")
}

func (h *handlers) pexpire(args []string) resp.Value {
	return h.setExpiry(args, time.Millisecond, "pexpire")
}

func (h *handlers) expireAt(args []string) resp.Value {
	return h.setDeadline(args, time.Second, "expireat")
}

func (h *handlers) pexpireAt(args []string) resp.Value {
	return h.setDeadline(args, time.Millisecond, "pexpireat")
}

// setExpiry implements EXPIRE and PEXPIRE. A zero or negative timeout is
// valid and deletes the key immediately.
func (h *handlers) setExpiry(args []string, unit time.Duration, cmd string) resp.Value {
	n, isInt := parseInt(args[1])
	if !isInt {
		return notAnInteger
	}
	ttl, valid := toDuration(n, unit)
	if !valid {
		return invalidExpireTime(cmd)
	}
	return boolReply(h.db.Expire(args[0], ttl))
}

// setDeadline implements EXPIREAT and PEXPIREAT, which give the moment a
// key dies rather than how long it has left. A deadline in the past
// deletes the key, exactly like a negative timeout.
func (h *handlers) setDeadline(args []string, unit time.Duration, cmd string) resp.Value {
	n, isInt := parseInt(args[1])
	if !isInt {
		return notAnInteger
	}
	deadline, valid := unixTime(n, unit)
	if !valid {
		return invalidExpireTime(cmd)
	}
	return boolReply(h.db.ExpireAt(args[0], deadline))
}

func (h *handlers) ttl(args []string) resp.Value  { return h.remainingTTL(args[0], time.Second) }
func (h *handlers) pttl(args []string) resp.Value { return h.remainingTTL(args[0], time.Millisecond) }

// remainingTTL implements TTL and PTTL. Like Redis, it replies -2 for a
// missing key and -1 for a key without expiry.
func (h *handlers) remainingTTL(key string, unit time.Duration) resp.Value {
	d, exists := h.db.TTL(key)
	switch {
	case !exists:
		return resp.NewInteger(-2)
	case d == store.NoExpiry:
		return resp.NewInteger(-1)
	}
	// Round to the nearest unit, so a key set with EX 10 reports 10 rather
	// than 9 a few microseconds later.
	return resp.NewInteger(int64((d + unit/2) / unit))
}

func (h *handlers) persist(args []string) resp.Value {
	return boolReply(h.db.Persist(args[0]))
}

func (h *handlers) keys(args []string) resp.Value {
	return stringsReply(h.db.Keys(args[0]))
}

// typeOf implements TYPE. It can't be called "type", which is a Go keyword.
func (h *handlers) typeOf(args []string) resp.Value {
	return resp.NewSimpleString(h.db.Type(args[0]))
}

func (h *handlers) dbsize(args []string) resp.Value {
	return resp.NewInteger(int64(h.db.Len()))
}

func (h *handlers) flushdb(args []string) resp.Value {
	h.db.Flush()
	return okReply
}

// boolReply turns a yes/no answer into the 1/0 integer reply Redis uses.
func boolReply(b bool) resp.Value {
	if b {
		return resp.NewInteger(1)
	}
	return resp.NewInteger(0)
}
