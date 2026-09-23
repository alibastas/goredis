package aof

import (
	"strconv"

	"github.com/alibastas/goredis/internal/store"
)

// itemsPerCommand caps how many elements one generated command carries.
// Without a cap, a list of a million items would become a single RPUSH
// with a million arguments, which the RESP parser refuses to read back
// (it allows a million elements per array, and the command name counts
// too). Redis splits rewritten collections the same way, at 64.
const itemsPerCommand = 64

// Commands turns a copy of the keyspace into the shortest series of
// commands that rebuilds it. It is the heart of a rewrite: the log on disk
// is a history, while this is a recipe, so ten million increments of one
// counter collapse into a single SET.
//
// Deadlines come out as PEXPIREAT, absolute like everywhere else in the
// log, so a rewritten file does not hand keys a fresh lifetime either.
func Commands(records []store.Record) [][]string {
	var cmds [][]string
	for _, rec := range records {
		cmds = appendCommandsFor(cmds, rec)
	}
	return cmds
}

func appendCommandsFor(cmds [][]string, rec store.Record) [][]string {
	switch rec.Kind {
	case store.KindString:
		cmds = append(cmds, []string{"SET", rec.Key, rec.Str})
	case store.KindList:
		cmds = appendBatched(cmds, "RPUSH", rec.Key, rec.Elems, itemsPerCommand)
	case store.KindSet:
		cmds = appendBatched(cmds, "SADD", rec.Key, rec.Elems, itemsPerCommand)
	case store.KindHash:
		pairs := make([]string, 0, 2*len(rec.Fields))
		for field, val := range rec.Fields {
			pairs = append(pairs, field, val)
		}
		// A field and its value must stay in the same command, so batches
		// are counted in pairs.
		cmds = appendBatched(cmds, "HSET", rec.Key, pairs, 2*itemsPerCommand)
	default:
		// Export only produces the four kinds above. Anything else is not
		// something this build knows how to write down.
		return cmds
	}

	if !rec.ExpiresAt.IsZero() {
		cmds = append(cmds, []string{"PEXPIREAT", rec.Key, strconv.FormatInt(rec.ExpiresAt.UnixMilli(), 10)})
	}
	return cmds
}

// appendBatched writes elems as one or more "name key elems..." commands,
// none carrying more than perCommand elements.
func appendBatched(cmds [][]string, name, key string, elems []string, perCommand int) [][]string {
	for start := 0; start < len(elems); start += perCommand {
		batch := elems[start:min(start+perCommand, len(elems))]
		cmd := make([]string, 0, 2+len(batch))
		cmd = append(cmd, name, key)
		cmd = append(cmd, batch...)
		cmds = append(cmds, cmd)
	}
	return cmds
}
