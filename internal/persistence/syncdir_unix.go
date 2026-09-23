//go:build !windows

package persistence

import "os"

// SyncDir flushes a directory's own entries to disk. A rename is recorded
// in the directory, so without this the new file can be complete on disk
// while the directory still points at the old one after a power cut.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
