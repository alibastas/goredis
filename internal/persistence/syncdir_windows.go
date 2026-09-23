//go:build windows

package persistence

// SyncDir does nothing on Windows: directories cannot be opened for
// syncing there, and NTFS journals the rename itself.
//
// The two files with the syncdir prefix show Go's build constraints: the
// //go:build line at the top of a file decides whether it is compiled at
// all, so each platform gets exactly one definition of syncDir.
func SyncDir(string) error { return nil }
