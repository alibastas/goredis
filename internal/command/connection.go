package command

import "github.com/alibastas/goredis/internal/resp"

// ping replies PONG, or echoes its single argument back as a bulk string.
//
// A subscribed client is answered with an array instead. Everything else it
// receives while in subscribe mode is an array too, so Redis keeps PING in
// the same shape there rather than making clients handle a simple string in
// the middle of a stream of messages.
func ping(s *Session, args []string) {
	if s.subscribed() {
		var message string
		if len(args) == 1 {
			message = args[0]
		}
		s.Send(resp.NewArray(resp.NewBulkString("pong"), resp.NewBulkString(message)))
		return
	}

	switch len(args) {
	case 0:
		s.Send(resp.NewSimpleString("PONG"))
	case 1:
		s.Send(resp.NewBulkString(args[0]))
	default:
		s.Send(wrongArgCount("PING"))
	}
}

func echo(args []string) resp.Value {
	return resp.NewBulkString(args[0])
}

// quit asks the server to hang up. The reply still goes out first, so a
// client can tell the difference between a clean goodbye and a connection
// that simply broke.
func quit(s *Session, args []string) {
	s.quit = true
	s.Send(okReply)
}
