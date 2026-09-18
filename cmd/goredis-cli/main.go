// Command goredis-cli is a small interactive client for goredis, modeled on
// redis-cli. It works against real Redis too.
//
//	goredis-cli                 start an interactive session
//	goredis-cli SET name ali    run one command and exit
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/alibastas/goredis/internal/resp"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	host := flag.String("h", "127.0.0.1", "server host")
	port := flag.Int("p", 6380, "server port")
	flag.Parse()

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("could not connect to goredis at %s: %w", addr, err)
	}
	defer conn.Close()
	c := newClient(conn)

	// With arguments: run a single command, like `redis-cli GET name`.
	if flag.NArg() > 0 {
		reply, err := c.do(flag.Args())
		if err != nil {
			return err
		}
		fmt.Println(formatReply(reply))
		return nil
	}
	return repl(c, addr, os.Stdin, os.Stdout)
}

type client struct {
	r *resp.Reader
	w *resp.Writer
}

func newClient(conn net.Conn) *client {
	return &client{r: resp.NewReader(conn), w: resp.NewWriter(conn)}
}

// do sends one command and waits for its reply. Commands go out in the
// same shape every Redis client uses: an array of bulk strings.
func (c *client) do(args []string) (resp.Value, error) {
	elems := make([]resp.Value, len(args))
	for i, a := range args {
		elems[i] = resp.NewBulkString(a)
	}
	if err := c.w.WriteValue(resp.NewArray(elems...)); err != nil {
		return resp.Value{}, err
	}
	if err := c.w.Flush(); err != nil {
		return resp.Value{}, fmt.Errorf("sending command: %w", err)
	}

	reply, err := c.r.ReadValue()
	if err != nil {
		return resp.Value{}, fmt.Errorf("reading reply: %w", err)
	}
	return reply, nil
}

// byteOrderMark is U+FEFF, an invisible character some Windows tools put in
// front of text to mark it as UTF-8.
var byteOrderMark = string(rune(0xFEFF))

// repl is the interactive loop: print a prompt, read a line, send it, print
// the reply. It stops at end of input (Ctrl+Z on Windows, Ctrl+D elsewhere),
// on "quit" or "exit", or when the connection breaks.
func repl(c *client, addr string, in io.Reader, out io.Writer) error {
	prompt := addr + "> "
	scanner := bufio.NewScanner(in)

	for first := true; ; first = false {
		fmt.Fprint(out, prompt)
		if !scanner.Scan() {
			fmt.Fprintln(out)
			return scanner.Err()
		}

		line := scanner.Text()
		if first {
			// Windows PowerShell prefixes text piped into a program with an
			// invisible byte order mark. Left in place, it would become part
			// of the first command name.
			line = strings.TrimPrefix(line, byteOrderMark)
		}

		args, err := splitArgs(line)
		if err != nil {
			fmt.Fprintln(out, err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		if cmd := strings.ToLower(args[0]); cmd == "quit" || cmd == "exit" {
			return nil
		}

		reply, err := c.do(args)
		if err != nil {
			return fmt.Errorf("connection to %s lost: %w", addr, err)
		}
		fmt.Fprintln(out, formatReply(reply))
	}
}
