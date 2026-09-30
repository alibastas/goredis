package command

import "github.com/alibastas/goredis/internal/resp"

// SUBSCRIBE and UNSUBSCRIBE answer once per channel rather than once per
// command: a client that subscribes to three channels gets three replies,
// each naming the channel and how many subscriptions the client has left.
// That is why these are session commands, which write their own replies,
// instead of handlers that return one value.

func subscribe(s *Session, args []string) {
	for _, channel := range args {
		s.subs = s.hub.Subscribe(s, channel)
		s.Send(subscriptionReply("subscribe", channel, s.subs))
	}
}

func unsubscribe(s *Session, args []string) {
	channels := args
	if len(channels) == 0 {
		// UNSUBSCRIBE without arguments means "all of them".
		channels = s.hub.Channels(s)
		if len(channels) == 0 {
			// A client that was not subscribed to anything still gets an
			// answer, with no channel to name.
			s.Send(subscriptionReplyValue("unsubscribe", resp.NullBulkString(), 0))
			return
		}
	}
	for _, channel := range channels {
		s.subs = s.hub.Unsubscribe(s, channel)
		s.Send(subscriptionReply("unsubscribe", channel, s.subs))
	}
}

func publish(s *Session, args []string) {
	s.Send(resp.NewInteger(int64(s.hub.Publish(args[0], args[1]))))
}

func subscriptionReply(kind, channel string, count int) resp.Value {
	return subscriptionReplyValue(kind, resp.NewBulkString(channel), count)
}

func subscriptionReplyValue(kind string, channel resp.Value, count int) resp.Value {
	return resp.NewArray(resp.NewBulkString(kind), channel, resp.NewInteger(int64(count)))
}
