package command

import (
	"errors"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/resp"
	"github.com/alibastas/goredis/internal/store"
)

// fakePersister stands in for the snapshot saver: it records what it was
// asked to do without touching the disk.
type fakePersister struct {
	saves      int
	background int
	err        error
	last       time.Time
}

func (p *fakePersister) Save() error {
	p.saves++
	return p.err
}

func (p *fakePersister) BackgroundSave() error {
	p.background++
	return p.err
}

func (p *fakePersister) LastSave() time.Time { return p.last }

func newPersistenceHarness(t *testing.T, p Persister) *harness {
	h := &harness{t: t, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.r = NewRegistry(store.NewWithClock(func() time.Time { return h.now }), WithPersistence(p))
	return h
}

func TestSaveCommands(t *testing.T) {
	p := &fakePersister{last: time.Unix(1767225600, 0)}
	h := newPersistenceHarness(t, p)

	h.expect(OK, "SAVE")
	h.expect(resp.NewSimpleString("Background saving started"), "BGSAVE")
	h.expect(integer(1767225600), "LASTSAVE")

	if p.saves != 1 || p.background != 1 {
		t.Fatalf("persister saw %d saves and %d background saves, want 1 and 1", p.saves, p.background)
	}

	h.expect(errReply("ERR wrong number of arguments for 'save' command"), "SAVE", "now")
	h.expect(errReply("ERR wrong number of arguments for 'bgsave' command"), "BGSAVE", "now")
	if p.saves != 1 {
		t.Fatalf("a malformed SAVE reached the persister")
	}
}

func TestSaveReportsFailures(t *testing.T) {
	p := &fakePersister{err: errors.New("disk is full")}
	h := newPersistenceHarness(t, p)

	h.expect(errReply("ERR disk is full"), "SAVE")
	h.expect(errReply("ERR disk is full"), "BGSAVE")
}

// TestPersistenceCommandsWithoutAPersister covers a server started with
// -snapshot=false: the commands exist so clients get a clear answer
// rather than "unknown command".
func TestPersistenceCommandsWithoutAPersister(t *testing.T) {
	h := newHarness(t)
	want := errReply("ERR persistence is disabled on this server")

	h.expect(want, "SAVE")
	h.expect(want, "BGSAVE")
	h.expect(want, "LASTSAVE")
}
