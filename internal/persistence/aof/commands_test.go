package aof

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/store"
)

func joinCommands(cmds [][]string) []string {
	out := make([]string, len(cmds))
	for i, args := range cmds {
		out[i] = strings.Join(args, " ")
	}
	return out
}

func TestCommands(t *testing.T) {
	deadline := time.UnixMilli(1790110408000)

	tests := []struct {
		name   string
		record store.Record
		want   []string
	}{
		{
			"a string",
			store.Record{Key: "k", Kind: store.KindString, Str: "v"},
			[]string{"SET k v"},
		},
		{
			"a string with a deadline",
			store.Record{Key: "k", Kind: store.KindString, Str: "v", ExpiresAt: deadline},
			[]string{"SET k v", "PEXPIREAT k 1790110408000"},
		},
		{
			"a list keeps its order",
			store.Record{Key: "l", Kind: store.KindList, Elems: []string{"a", "b", "c"}},
			[]string{"RPUSH l a b c"},
		},
		{
			"a set",
			store.Record{Key: "s", Kind: store.KindSet, Elems: []string{"x"}},
			[]string{"SADD s x"},
		},
		{
			"a hash",
			store.Record{Key: "h", Kind: store.KindHash, Fields: map[string]string{"f": "v"}},
			[]string{"HSET h f v"},
		},
		{
			"an unknown kind is skipped",
			store.Record{Key: "?", Kind: store.Kind(99)},
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := joinCommands(Commands([]store.Record{tt.record}))
			if !slices.Equal(got, tt.want) {
				t.Errorf("Commands = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestCommandsSplitsLargeCollections guards the limit that makes a
// rewritten file readable at all: one RPUSH carrying a million arguments
// is past what the RESP parser accepts, so a rewrite would produce a log
// the server could not load back.
func TestCommandsSplitsLargeCollections(t *testing.T) {
	const n = itemsPerCommand*3 + 7
	elems := make([]string, n)
	for i := range elems {
		elems[i] = strconv.Itoa(i)
	}

	cmds := Commands([]store.Record{{Key: "l", Kind: store.KindList, Elems: elems}})
	if len(cmds) != 4 {
		t.Fatalf("a list of %d items became %d commands, want 4", n, len(cmds))
	}

	// Together the commands must rebuild the list in the original order.
	var rebuilt []string
	for _, args := range cmds {
		if args[0] != "RPUSH" || args[1] != "l" {
			t.Fatalf("unexpected command %q", args)
		}
		if got := len(args) - 2; got > itemsPerCommand {
			t.Fatalf("a command carries %d items, more than the %d allowed", got, itemsPerCommand)
		}
		rebuilt = append(rebuilt, args[2:]...)
	}
	if !slices.Equal(rebuilt, elems) {
		t.Fatal("the split commands do not rebuild the list")
	}
}

// TestCommandsKeepsHashPairsTogether checks the one place where splitting
// could corrupt data: a field and its value have to travel in the same
// command.
func TestCommandsKeepsHashPairsTogether(t *testing.T) {
	fields := make(map[string]string, 200)
	for i := range 200 {
		fields["f"+strconv.Itoa(i)] = "v" + strconv.Itoa(i)
	}

	for _, args := range Commands([]store.Record{{Key: "h", Kind: store.KindHash, Fields: fields}}) {
		pairs := args[2:]
		if len(pairs)%2 != 0 {
			t.Fatalf("a HSET carries %d arguments after the key, which is not whole pairs", len(pairs))
		}
		for i := 0; i < len(pairs); i += 2 {
			if want := "v" + strings.TrimPrefix(pairs[i], "f"); pairs[i+1] != want {
				t.Fatalf("field %q travelled with value %q, want %q", pairs[i], pairs[i+1], want)
			}
		}
	}
}
