// Package persistence holds the plumbing shared by the snapshot and
// append-only file implementations in its subpackages.
package persistence

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
)

// WriteFileAtomic writes a file in a way that a crash cannot leave half
// finished. Readers only ever see the complete old file or the complete
// new one, never a mixture of the two.
//
// The trick is that renaming a file is atomic in the operating system,
// while writing one is not. So the data goes to a temporary file in the
// same directory first, is forced out to the physical disk with fsync,
// and only then takes over the real name:
//
//	write data -> path.tmp-1234
//	fsync      -> the bytes are really on disk, not just in the OS cache
//	rename     -> path.tmp-1234 becomes path, in one indivisible step
//	fsync dir  -> the rename itself survives a power cut
//
// Without the first fsync the rename could complete while the contents
// are still in the operating system's write cache, which would leave an
// empty or truncated file behind after a power cut. The temporary file
// must live in the same directory, because renaming across file systems
// is a copy and not atomic.
func WriteFileAtomic(path string, write func(io.Writer) error) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	// Anything that fails from here on leaves the previous file untouched.
	// Both calls are no-ops once the happy path below has run.
	defer func() {
		f.Close()
		os.Remove(tmp)
	}()

	bw := bufio.NewWriter(f)
	if err := write(bw); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}
