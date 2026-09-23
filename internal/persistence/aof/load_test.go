package aof

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// writeLog puts raw bytes in a log file, so a test can describe exactly
// what a crash left behind.
func writeLog(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const (
	setKV   = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
	setKW   = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nw\r\n"
	delK    = "*2\r\n$3\r\nDEL\r\n$1\r\nk\r\n"
	halfSet = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n"
)

func TestLoadReplaysInOrder(t *testing.T) {
	path := writeLog(t, setKV+setKW+delK)

	got := readBack(t, path)
	if len(got) != 3 {
		t.Fatalf("replayed %d commands, want 3", len(got))
	}
	if got[0][2] != "v" || got[1][2] != "w" || got[2][0] != "DEL" {
		t.Fatalf("commands came back as %q", got)
	}
}

func TestLoadOfAnEmptyFile(t *testing.T) {
	got := readBack(t, writeLog(t, ""))
	if len(got) != 0 {
		t.Fatalf("replayed %d commands from an empty file", len(got))
	}
}

// TestLoadTruncatesAnIncompleteTail is the case a crash actually produces.
// Writing a command is not atomic, so the process can die in the middle of
// one. Everything before it has to survive, and the fragment has to be cut
// off, or the next append would glue itself onto half a command and make
// the whole file unreadable.
func TestLoadTruncatesAnIncompleteTail(t *testing.T) {
	tests := []struct {
		name string
		tail string
	}{
		{"nothing but an array header", "*3\r\n"},
		{"a header with no body", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n"},
		{"a value cut in half", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nab"},
		{"a missing final newline", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeLog(t, setKV+delK+tt.tail)
			complete := int64(len(setKV + delK))

			got := readBack(t, path)
			if len(got) != 2 {
				t.Fatalf("replayed %d commands, want the 2 complete ones", len(got))
			}
			if size := fileSize(t, path); size != complete {
				t.Fatalf("file is %d bytes after loading, want it truncated to %d", size, complete)
			}

			// After truncating, appending must produce a file that still
			// replays cleanly.
			l, err := Open(path, FsyncNever)
			if err != nil {
				t.Fatal(err)
			}
			l.Append([]string{"SET", "after", "crash"})
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if got := readBack(t, path); len(got) != 3 || got[2][1] != "after" {
				t.Fatalf("after appending, replayed %q", got)
			}
		})
	}
}

// TestLoadRejectsDamageInTheMiddle draws the other half of the line: a
// broken last command is a crash, but nonsense before the end means the
// file cannot be trusted, and a server that started anyway would write
// that gap back out as the truth.
func TestLoadRejectsDamageInTheMiddle(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{"junk between commands", setKV + "this is not RESP\r\n" + delK},
		{"a reply instead of a command", setKV + "+OK\r\n" + delK},
		{"an empty command", setKV + "*0\r\n" + delK},
		{"a nil argument", setKV + "*2\r\n$3\r\nDEL\r\n$-1\r\n" + delK},
		{"a negative length", setKV + "*-2\r\n" + delK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeLog(t, tt.content)
			before := fileSize(t, path)

			var applied int
			_, err := Load(path, func([]string) error { applied++; return nil }, discardLogger())
			if err == nil {
				t.Fatal("Load accepted a damaged file")
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error %v does not wrap ErrCorrupt", err)
			}
			if size := fileSize(t, path); size != before {
				t.Errorf("a rejected file was modified: %d bytes, was %d", size, before)
			}
		})
	}
}

// TestLoadStopsWhenApplyFails covers a log holding a command this server
// refuses, which means the file does not belong to it.
func TestLoadStopsWhenApplyFails(t *testing.T) {
	path := writeLog(t, setKV+setKW+delK)

	boom := errors.New("unknown command")
	applied := 0
	n, err := Load(path, func([]string) error {
		applied++
		if applied == 2 {
			return boom
		}
		return nil
	}, discardLogger())

	if !errors.Is(err, boom) {
		t.Fatalf("Load = %v, want the apply error", err)
	}
	if n != 1 {
		t.Errorf("Load reported %d replayed commands, want 1", n)
	}
}

// TestLoadTracksTheOffsetPastTheBuffer guards the subtle part of finding
// the truncation point: the reader buffers ahead, so the file offset is
// well past the last command that was actually consumed. With a log long
// enough to fill that buffer several times, a naive offset would cut the
// file in the wrong place.
func TestLoadTracksTheOffsetPastTheBuffer(t *testing.T) {
	var content string
	const commands = 2000
	for range commands {
		content += setKV
	}
	path := writeLog(t, content+halfSet)
	complete := int64(len(content))

	got := readBack(t, path)
	if len(got) != commands {
		t.Fatalf("replayed %d commands, want %d", len(got), commands)
	}
	if size := fileSize(t, path); size != complete {
		t.Fatalf("truncated to %d bytes, want %d", size, complete)
	}
}
