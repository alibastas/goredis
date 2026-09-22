package command

import (
	"time"

	"github.com/alibastas/goredis/internal/resp"
)

// Persister is the slice of the persistence layer the command table
// needs. Declaring it here as an interface, instead of importing the
// snapshot package, keeps dependencies pointing one way and lets tests
// pass a stub that writes nothing.
type Persister interface {
	// Save writes a snapshot and returns once it is on disk.
	Save() error
	// BackgroundSave starts a save and returns as soon as the keyspace
	// has been copied.
	BackgroundSave() error
	// LastSave reports when the last snapshot completed.
	LastSave() time.Time
}

var errPersistenceDisabled = resp.NewError("ERR persistence is disabled on this server")

func (h *handlers) save(args []string) resp.Value {
	if h.persister == nil {
		return errPersistenceDisabled
	}
	if err := h.persister.Save(); err != nil {
		return resp.NewError("ERR " + err.Error())
	}
	return okReply
}

func (h *handlers) bgsave(args []string) resp.Value {
	if h.persister == nil {
		return errPersistenceDisabled
	}
	if err := h.persister.BackgroundSave(); err != nil {
		return resp.NewError("ERR " + err.Error())
	}
	return resp.NewSimpleString("Background saving started")
}

func (h *handlers) lastsave(args []string) resp.Value {
	if h.persister == nil {
		return errPersistenceDisabled
	}
	return resp.NewInteger(h.persister.LastSave().Unix())
}
