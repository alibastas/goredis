// Package command maps incoming requests to the functions that implement
// them. It checks the request shape and argument count so individual
// handlers only have to deal with well-formed input.
package command

import (
	"errors"
	"fmt"
	"strings"

	"github.com/alibastas/goredis/internal/resp"
)

// Handler runs one command. args holds the arguments after the command
// name, so for "ECHO hello" it is []string{"hello"}.
type Handler func(args []string) resp.Value

type command struct {
	handler Handler
	// arity follows the Redis convention: it counts the command name too,
	// a positive value means exactly that many and a negative value means
	// at least -arity. PING has arity -1, ECHO has arity 2.
	arity int
}

func (c command) acceptsArgCount(n int) bool {
	if c.arity >= 0 {
		return n == c.arity
	}
	return n >= -c.arity
}

// Registry holds every command the server understands, keyed by its
// upper-case name.
type Registry struct {
	commands map[string]command
}

func NewRegistry() *Registry {
	r := &Registry{commands: make(map[string]command)}
	r.register("PING", -1, ping)
	r.register("ECHO", 2, echo)
	return r
}

func (r *Registry) register(name string, arity int, h Handler) {
	r.commands[name] = command{handler: h, arity: arity}
}

// Dispatch runs the command described by req and returns its reply.
// Problems with the request itself (unknown command, wrong number of
// arguments) are reported as RESP errors rather than Go errors, since they
// are normal replies from the client's point of view.
func (r *Registry) Dispatch(req resp.Value) resp.Value {
	args, err := requestArgs(req)
	if err != nil {
		return resp.NewError("ERR " + err.Error())
	}

	name := strings.ToUpper(args[0])
	cmd, ok := r.commands[name]
	if !ok {
		return unknownCommand(args)
	}
	if !cmd.acceptsArgCount(len(args)) {
		return wrongArgCount(name)
	}
	return cmd.handler(args[1:])
}

// requestArgs checks that req has the shape clients use for commands, a
// non-empty array of bulk strings, and returns those strings.
func requestArgs(req resp.Value) ([]string, error) {
	if req.Type != resp.Array || req.Null || len(req.Array) == 0 {
		return nil, errors.New("expected a non-empty array of bulk strings")
	}
	args := make([]string, len(req.Array))
	for i, v := range req.Array {
		if v.Type != resp.BulkString || v.Null {
			return nil, errors.New("expected a non-empty array of bulk strings")
		}
		args[i] = v.Str
	}
	return args, nil
}

// maxArgInError limits how much of a client's input is echoed back in an
// error message, so a huge argument doesn't turn into a huge reply.
const maxArgInError = 128

func unknownCommand(args []string) resp.Value {
	var b strings.Builder
	fmt.Fprintf(&b, "ERR unknown command '%s', with args beginning with: ", truncate(args[0]))
	for _, arg := range args[1:] {
		fmt.Fprintf(&b, "'%s' ", truncate(arg))
	}
	return resp.NewError(b.String())
}

func wrongArgCount(name string) resp.Value {
	return resp.NewError(fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(name)))
}

func truncate(s string) string {
	if len(s) > maxArgInError {
		return s[:maxArgInError]
	}
	return s
}
