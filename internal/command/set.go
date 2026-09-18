package command

import "github.com/alibastas/goredis/internal/resp"

func (h *handlers) sadd(args []string) resp.Value {
	return intReply(h.db.SAdd(args[0], args[1:]...))
}

func (h *handlers) srem(args []string) resp.Value {
	return intReply(h.db.SRem(args[0], args[1:]...))
}

func (h *handlers) smembers(args []string) resp.Value {
	members, err := h.db.SMembers(args[0])
	if err != nil {
		return errorReply(err)
	}
	return stringsReply(members)
}

func (h *handlers) sismember(args []string) resp.Value {
	ok, err := h.db.SIsMember(args[0], args[1])
	if err != nil {
		return errorReply(err)
	}
	return boolReply(ok)
}

func (h *handlers) scard(args []string) resp.Value {
	return intReply(h.db.SCard(args[0]))
}
