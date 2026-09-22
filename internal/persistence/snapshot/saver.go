package snapshot

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/alibastas/goredis/internal/store"
)

// ErrSaveInProgress is returned when a save is asked for while another
// one is still running. The message matches Redis's.
var ErrSaveInProgress = errors.New("Background save already in progress")

// Saver owns the snapshot file: it decides when a copy of the keyspace
// is taken and writes it out, making sure only one save runs at a time.
//
// Redis takes a background snapshot by calling fork(), which hands the
// child process a frozen copy of memory that the operating system keeps
// cheap through copy-on-write. Go has no usable fork: the runtime's
// threads do not survive it. The equivalent here is to copy the keyspace
// under its own read locks and hand the copy to a goroutine, which then
// spends the slow part, the disk, holding no locks at all. Writers wait
// only for the copy, not for the write, and readers never wait.
//
// The trade-off is memory: while a background save runs, the copy lives
// next to the real keyspace. Strings are shared rather than duplicated,
// so the extra cost is the maps and slices holding them, not the data.
type Saver struct {
	db     *store.Store
	path   string
	logger *slog.Logger

	mu      sync.Mutex
	running bool
	last    time.Time

	// wg tracks background saves so shutdown can wait for them.
	wg sync.WaitGroup
}

func NewSaver(db *store.Store, path string, logger *slog.Logger) *Saver {
	return &Saver{
		db:     db,
		path:   path,
		logger: logger,
		// Like Redis, a server that has never saved reports its start
		// time rather than 1970.
		last: time.Now(),
	}
}

func (s *Saver) Path() string { return s.path }

// Save writes a snapshot and waits for it to be on disk. This is the SAVE
// command: simple, and blocks every client for as long as it takes.
func (s *Saver) Save() error {
	if err := s.begin(); err != nil {
		return err
	}
	defer s.end()

	start := time.Now()
	if err := s.write(s.db.Export()); err != nil {
		return err
	}
	s.logger.Info("snapshot written", "path", s.path, "took", time.Since(start))
	return nil
}

// BackgroundSave copies the keyspace and returns as soon as the copy is
// taken, leaving the disk write to a goroutine. This is BGSAVE.
func (s *Saver) BackgroundSave() error {
	if err := s.begin(); err != nil {
		return err
	}

	start := time.Now()
	records := s.db.Export()
	copied := time.Since(start)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.end()
		if err := s.write(records); err != nil {
			s.logger.Error("background save failed", "path", s.path, "err", err)
			return
		}
		s.logger.Info("background save finished",
			"path", s.path, "keys", len(records), "copy", copied, "took", time.Since(start))
	}()
	return nil
}

// LastSave reports when the last snapshot was completed.
func (s *Saver) LastSave() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// Wait blocks until no background save is running. The server calls it on
// shutdown so it doesn't exit in the middle of writing a file.
func (s *Saver) Wait() { s.wg.Wait() }

func (s *Saver) write(records []store.Record) error {
	if err := WriteFile(s.path, records); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = time.Now()
	return nil
}

func (s *Saver) begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return ErrSaveInProgress
	}
	s.running = true
	return nil
}

func (s *Saver) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
}
