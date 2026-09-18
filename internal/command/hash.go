package command

import "github.com/alibastas/goredis/internal/resp"

// hset implements HSET key field value [field value ...].
func (h *handlers) hset(args []string) resp.Value {
	// The arity check guarantees at least one pair; the rest must be pairs too.
	if len(args[1:])%2 != 0 {
		return wrongArgCount("HSET")
	}
	return intReply(h.db.HSet(args[0], args[1:]...))
}

func (h *handlers) hget(args []string) resp.Value {
	val, ok, err := h.db.HGet(args[0], args[1])
	switch {
	case err != nil:
		return errorReply(err)
	case !ok:
		return resp.NullBulkString()
	}
	return resp.NewBulkString(val)
}

func (h *handlers) hdel(args []string) resp.Value {
	return intReply(h.db.HDel(args[0], args[1:]...))
}

func (h *handlers) hgetall(args []string) resp.Value {
	items, err := h.db.HGetAll(args[0])
	if err != nil {
		return errorReply(err)
	}
	return stringsReply(items)
}

func (h *handlers) hexists(args []string) resp.Value {
	ok, err := h.db.HExists(args[0], args[1])
	if err != nil {
		return errorReply(err)
	}
	return boolReply(ok)
}

func (h *handlers) hlen(args []string) resp.Value {
	return intReply(h.db.HLen(args[0]))
}
