package command

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

func (h *handlers) get(args []string) resp.Value {
	value, found, err := h.db.Get(args[0])
	if err != nil {
		return errorReply(err)
	}
	if !found {
		return resp.NullBulkString()
	}
	return resp.NewBulkString(value)
}

// set implements SET key value [NX | XX] [EX seconds | PX milliseconds | KEEPTTL].
func (h *handlers) set(args []string) resp.Value {
	key, value := args[0], args[1]

	var opts store.SetOptions
	for i := 2; i < len(args); i++ {
		switch opt := strings.ToUpper(args[i]); opt {
		case "NX", "XX":
			if opts.Condition != store.Always {
				return syntaxError
			}
			opts.Condition = store.IfNotExists
			if opt == "XX" {
				opts.Condition = store.IfExists
			}
		case "KEEPTTL":
			if opts.KeepTTL || opts.TTL != 0 || !opts.ExpiresAt.IsZero() {
				return syntaxError
			}
			opts.KeepTTL = true
		case "EX", "PX":
			if opts.KeepTTL || opts.TTL != 0 || !opts.ExpiresAt.IsZero() || i+1 == len(args) {
				return syntaxError
			}
			i++
			n, isInt := parseInt(args[i])
			if !isInt {
				return notAnInteger
			}
			unit := time.Second
			if opt == "PX" {
				unit = time.Millisecond
			}
			ttl, valid := toDuration(n, unit)
			if !valid || ttl <= 0 {
				return invalidExpireTime("set")
			}
			opts.TTL = ttl
		case "EXAT", "PXAT":
			// The absolute form of EX and PX. The append-only file logs
			// every timeout this way, so a replay cannot hand a key its
			// full lifetime all over again.
			if opts.KeepTTL || opts.TTL != 0 || !opts.ExpiresAt.IsZero() || i+1 == len(args) {
				return syntaxError
			}
			i++
			n, isInt := parseInt(args[i])
			if !isInt {
				return notAnInteger
			}
			unit := time.Second
			if opt == "PXAT" {
				unit = time.Millisecond
			}
			deadline, valid := unixTime(n, unit)
			if !valid {
				return invalidExpireTime("set")
			}
			opts.ExpiresAt = deadline
		default:
			return syntaxError
		}
	}

	if !h.db.Set(key, value, opts) {
		// NX or XX prevented the write.
		return resp.NullBulkString()
	}
	return okReply
}

func (h *handlers) incr(args []string) resp.Value { return h.addToInteger(args[0], 1) }
func (h *handlers) decr(args []string) resp.Value { return h.addToInteger(args[0], -1) }

func (h *handlers) incrBy(args []string) resp.Value {
	delta, isInt := parseInt(args[1])
	if !isInt {
		return notAnInteger
	}
	return h.addToInteger(args[0], delta)
}

func (h *handlers) decrBy(args []string) resp.Value {
	delta, isInt := parseInt(args[1])
	if !isInt {
		return notAnInteger
	}
	// -math.MinInt64 does not fit in an int64, so it can't be negated.
	if delta == math.MinInt64 {
		return resp.NewError("ERR decrement would overflow")
	}
	return h.addToInteger(args[0], -delta)
}

func (h *handlers) addToInteger(key string, delta int64) resp.Value {
	n, err := h.db.IncrBy(key, delta)
	if err != nil {
		return errorReply(err)
	}
	return resp.NewInteger(n)
}

func parseInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

// toDuration converts n units into a time.Duration. It reports false if the
// result does not fit: time.Duration counts nanoseconds in an int64, which
// runs out at about 292 years.
func toDuration(n int64, unit time.Duration) (time.Duration, bool) {
	limit := int64(math.MaxInt64 / unit)
	if n > limit || n < -limit {
		return 0, false
	}
	return time.Duration(n) * unit, true
}

// unixTime turns n units since the epoch into a moment. Deadlines beyond
// what a snapshot can carry are rejected, so a key can never be given an
// expiry the server would fail to write back out.
func unixTime(n int64, unit time.Duration) (time.Time, bool) {
	ms := n
	if unit == time.Second {
		if n > store.MaxExpiryMillis/1000 || n < -store.MaxExpiryMillis/1000 {
			return time.Time{}, false
		}
		ms = n * 1000
	}
	if ms > store.MaxExpiryMillis {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}
