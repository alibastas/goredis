package command

import "github.com/alibastas/goredis/internal/resp"

// ping replies PONG, or echoes its single argument back as a bulk string.
func ping(args []string) resp.Value {
	switch len(args) {
	case 0:
		return resp.NewSimpleString("PONG")
	case 1:
		return resp.NewBulkString(args[0])
	}
	return wrongArgCount("PING")
}

func echo(args []string) resp.Value {
	return resp.NewBulkString(args[0])
}
