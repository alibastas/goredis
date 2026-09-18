package command

import (
	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

func (h *handlers) lpush(args []string) resp.Value {
	return intReply(h.db.Push(args[0], store.Left, args[1:]...))
}

func (h *handlers) rpush(args []string) resp.Value {
	return intReply(h.db.Push(args[0], store.Right, args[1:]...))
}

func (h *handlers) lpop(args []string) resp.Value { return h.pop(args, store.Left, "LPOP") }
func (h *handlers) rpop(args []string) resp.Value { return h.pop(args, store.Right, "RPOP") }

// pop implements LPOP and RPOP key [count]. The reply shape depends on
// whether count was given: without it the reply is a single element (or
// nil), with it an array (or nil if the key doesn't exist).
func (h *handlers) pop(args []string, end store.End, name string) resp.Value {
	if len(args) > 2 {
		return wrongArgCount(name)
	}

	count, withCount := 1, len(args) == 2
	if withCount {
		n, isInt := parseInt(args[1])
		if !isInt || n < 0 {
			return resp.NewError("ERR value is out of range, must be positive")
		}
		count = int(n)
	}

	popped, found, err := h.db.Pop(args[0], end, count)
	switch {
	case err != nil:
		return errorReply(err)
	case withCount && !found:
		return resp.NullArray()
	case withCount:
		return stringsReply(popped)
	case len(popped) == 0:
		return resp.NullBulkString()
	}
	return resp.NewBulkString(popped[0])
}

func (h *handlers) lrange(args []string) resp.Value {
	start, ok1 := parseInt(args[1])
	stop, ok2 := parseInt(args[2])
	if !ok1 || !ok2 {
		return notAnInteger
	}
	items, err := h.db.LRange(args[0], int(start), int(stop))
	if err != nil {
		return errorReply(err)
	}
	return stringsReply(items)
}

func (h *handlers) lindex(args []string) resp.Value {
	index, isInt := parseInt(args[1])
	if !isInt {
		return notAnInteger
	}
	elem, ok, err := h.db.LIndex(args[0], int(index))
	switch {
	case err != nil:
		return errorReply(err)
	case !ok:
		return resp.NullBulkString()
	}
	return resp.NewBulkString(elem)
}

func (h *handlers) llen(args []string) resp.Value {
	return intReply(h.db.LLen(args[0]))
}
