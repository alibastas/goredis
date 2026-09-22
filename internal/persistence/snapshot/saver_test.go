package snapshot

import (
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/store"
)

func newTestSaver(t *testing.T) (*Saver, *store.Store) {
	t.Helper()
	db := store.New()
	path := filepath.Join(t.TempDir(), "dump.goredis")
	s := NewSaver(db, path, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(s.Wait)
	return s, db
}

func TestSaverSave(t *testing.T) {
	s, db := newTestSaver(t)
	db.Set("k", "v", store.SetOptions{})

	before := s.LastSave()
	if before.IsZero() {
		t.Fatal("LastSave before any save is zero, want the server start time")
	}

	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	records, err := ReadFile(s.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(records) != 1 || records[0].Key != "k" || records[0].Str != "v" {
		t.Fatalf("snapshot holds %+v, want the one key", records)
	}
	// Only "not earlier than before": two saves in a row can land on the
	// same instant, since Windows's clock advances in steps of ~0.4 ms.
	if s.LastSave().Before(before) {
		t.Errorf("LastSave went backwards: %v then %v", before, s.LastSave())
	}
}

func TestSaverBackgroundSave(t *testing.T) {
	s, db := newTestSaver(t)
	db.Set("k", "before", store.SetOptions{})

	if err := s.BackgroundSave(); err != nil {
		t.Fatalf("BackgroundSave: %v", err)
	}
	// BackgroundSave copies the keyspace before it returns, so a write
	// landing now must not change what ends up in the file.
	db.Set("k", "after", store.SetOptions{})
	s.Wait()

	records, err := ReadFile(s.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(records) != 1 || records[0].Str != "before" {
		t.Fatalf("snapshot holds %+v, want the value from when the save started", records)
	}
}

// TestSaverRefusesOverlappingSaves covers the guard that keeps two saves
// from writing the same file at once.
func TestSaverRefusesOverlappingSaves(t *testing.T) {
	s, db := newTestSaver(t)
	for i := range 200 {
		db.Set("key:"+strconv.Itoa(i), "value", store.SetOptions{})
	}

	// Hold the save "in progress" by taking the flag by hand, which is
	// what a long running background write does.
	if err := s.begin(); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(); !errors.Is(err, ErrSaveInProgress) {
		t.Errorf("Save during another save = %v, want ErrSaveInProgress", err)
	}
	if err := s.BackgroundSave(); !errors.Is(err, ErrSaveInProgress) {
		t.Errorf("BackgroundSave during another save = %v, want ErrSaveInProgress", err)
	}
	s.end()

	if err := s.Save(); err != nil {
		t.Errorf("Save once the other one finished = %v, want nil", err)
	}
}

// TestSaverConcurrentSaves hammers the saver from several goroutines: at
// most one may run at a time, and the file must always be readable.
func TestSaverConcurrentSaves(t *testing.T) {
	s, db := newTestSaver(t)
	db.Set("k", "v", store.SetOptions{})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				if err := s.Save(); err != nil && !errors.Is(err, ErrSaveInProgress) {
					t.Errorf("Save: %v", err)
					return
				}
				if err := s.BackgroundSave(); err != nil && !errors.Is(err, ErrSaveInProgress) {
					t.Errorf("BackgroundSave: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	s.Wait()

	records, err := ReadFile(s.Path())
	if err != nil {
		t.Fatalf("ReadFile after concurrent saves: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("snapshot holds %d records, want 1", len(records))
	}
	if time.Since(s.LastSave()) > time.Minute {
		t.Error("LastSave was not updated")
	}
}
