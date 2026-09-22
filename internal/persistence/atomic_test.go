package persistence

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func write(s string) func(io.Writer) error {
	return func(w io.Writer) error {
		_, err := io.WriteString(w, s)
		return err
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// filesIn lists a directory, so a test can prove no temporary file was
// left behind.
func filesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")

	if err := WriteFileAtomic(path, write("first")); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != "first" {
		t.Fatalf("file holds %q, want %q", got, "first")
	}

	if err := WriteFileAtomic(path, write("second")); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, path); got != "second" {
		t.Fatalf("after replacing, file holds %q, want %q", got, "second")
	}
	if got := filesIn(t, dir); len(got) != 1 {
		t.Fatalf("directory holds %v, want only the target file", got)
	}
}

// TestWriteFileAtomicKeepsTheOldFile is the reason the helper exists: a
// failure halfway through must leave the previous version readable rather
// than a half-written one.
func TestWriteFileAtomicKeepsTheOldFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	if err := WriteFileAtomic(path, write("good")); err != nil {
		t.Fatal(err)
	}

	boom := errors.New("ran out of data")
	err := WriteFileAtomic(path, func(w io.Writer) error {
		io.WriteString(w, "half of the new file")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("WriteFileAtomic = %v, want the writer's own error", err)
	}

	if got := readFile(t, path); got != "good" {
		t.Fatalf("file holds %q after a failed write, want the previous content", got)
	}
	if got := filesIn(t, dir); len(got) != 1 {
		t.Fatalf("directory holds %v, want the temporary file to be cleaned up", got)
	}
}

func TestWriteFileAtomicNeedsAnExistingDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "data")
	if err := WriteFileAtomic(path, write("x")); err == nil {
		t.Fatal("WriteFileAtomic into a missing directory succeeded")
	}
}
