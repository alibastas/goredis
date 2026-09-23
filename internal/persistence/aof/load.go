package aof

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/alibastas/goredis/internal/resp"
)

// ErrCorrupt reports a log that cannot be replayed: not a broken last
// command, which is normal after a crash, but nonsense in the middle of
// the file.
var ErrCorrupt = errors.New("append-only file is corrupt")

// Load replays the log at path, handing every command to apply.
//
// It does not run the commands itself. Doing so would mean importing the
// command table, which already has to know about this package to append
// to it, and two packages importing each other is not allowed in Go.
// Passing the function in keeps the arrow pointing one way.
//
// A log whose last command is cut short is expected rather than
// exceptional: the process can die at any point during a write, including
// halfway through one. Everything up to that point is replayed and the
// file is truncated to the last complete command, so the next append
// starts from a clean boundary instead of gluing itself onto a fragment.
// Damage anywhere earlier is reported as ErrCorrupt, because a server
// that starts up with a hole in its data would then happily write that
// state back out as the truth.
//
// A missing file is reported as os.ErrNotExist, which is what a first
// start looks like.
func Load(path string, apply func(args []string) error, logger *slog.Logger) (replayed int, err error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// counter tracks how many bytes have been pulled out of the file so
	// that a truncation point can be worked out below.
	counter := &countingReader{r: f}
	// Everything in this file was written by the server, so the inline
	// command shorthand has no business here: it would turn a line of
	// damage into a command instead of reporting it.
	r := resp.NewReader(counter, resp.WithoutInlineCommands())

	var good int64 // offset just past the last complete command
	for {
		v, err := r.ReadValue()
		if errors.Is(err, io.EOF) {
			break
		}
		if isIncomplete(err) {
			// The file ends in the middle of a command.
			logger.Warn("the append-only file ends in an incomplete command, truncating it",
				"path", path, "keeping", good, "discarding", counter.n-good)
			if err := f.Truncate(good); err != nil {
				return replayed, fmt.Errorf("truncating %s: %w", path, err)
			}
			break
		}
		if err != nil {
			return replayed, fmt.Errorf("%w: at byte %d: %v", ErrCorrupt, good, err)
		}

		args, err := commandArgs(v)
		if err != nil {
			return replayed, fmt.Errorf("%w: at byte %d: %v", ErrCorrupt, good, err)
		}
		if err := apply(args); err != nil {
			return replayed, fmt.Errorf("replaying %s at byte %d: %w", args[0], good, err)
		}
		replayed++

		// The reader buffers ahead, so the file offset is past what has
		// actually been consumed. What is left in its buffer is exactly
		// the difference.
		good = counter.n - int64(r.Buffered())
	}
	return replayed, nil
}

// isIncomplete reports whether err means the file simply stops early,
// which is what a crash during a write looks like.
func isIncomplete(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF)
}

func commandArgs(v resp.Value) ([]string, error) {
	if v.Type != resp.Array || v.Null || len(v.Array) == 0 {
		return nil, fmt.Errorf("expected a command as a non-empty array, got %q", v.Type)
	}
	args := make([]string, len(v.Array))
	for i, elem := range v.Array {
		if elem.Type != resp.BulkString || elem.Null {
			return nil, fmt.Errorf("expected bulk string arguments, got %q", elem.Type)
		}
		args[i] = elem.Str
	}
	return args, nil
}

// countingReader remembers how many bytes have been read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
