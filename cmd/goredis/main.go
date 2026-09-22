// Command goredis runs the goredis server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/persistence/snapshot"
	"github.com/alibastas/goredis/internal/server"
	"github.com/alibastas/goredis/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "goredis:", err)
		os.Exit(1)
	}
}

// run holds the real startup logic. Keeping it out of main means deferred
// calls still run on error, which os.Exit would skip.
func run() error {
	addr := flag.String("addr", "127.0.0.1:6380", "address to listen on")
	debug := flag.Bool("debug", false, "enable debug logging")
	shards := flag.Int("shards", store.DefaultShards, "number of keyspace shards (1 means a single global lock)")
	dir := flag.String("dir", ".", "directory holding the data files")
	dbfilename := flag.String("dbfilename", "dump.goredis", "snapshot file name")
	useSnapshot := flag.Bool("snapshot", true, "load a snapshot at startup and write one on shutdown")
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	// ctx is cancelled on Ctrl+C or SIGTERM, which starts a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db := store.NewWithOptions(store.Options{Shards: *shards})

	// Load the previous snapshot before accepting anyone, so no client can
	// see an empty keyspace that is about to fill up.
	var saver *snapshot.Saver
	var registryOpts []command.Option
	if *useSnapshot {
		var err error
		if saver, err = openSnapshot(db, *dir, *dbfilename, logger); err != nil {
			return err
		}
		registryOpts = append(registryOpts, command.WithPersistence(saver))
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	logger.Info("goredis is ready to accept connections", "addr", ln.Addr().String())

	// Active expiry runs next to the server for as long as ctx lives.
	expiryDone := make(chan struct{})
	go func() {
		defer close(expiryDone)
		db.RunActiveExpiry(ctx)
	}()

	srv := server.New(command.NewRegistry(db, registryOpts...), logger)
	serveErr := srv.Serve(ctx, ln)

	// Serve can also return because accepting failed, with ctx still live.
	// Cancel it either way so the expiry goroutine stops, then wait for it.
	stop()
	<-expiryDone

	if saver != nil {
		// Let any BGSAVE finish before replacing the file under it.
		saver.Wait()
		if err := saver.Save(); err != nil {
			logger.Error("could not write the final snapshot", "err", err)
		}
	}

	if serveErr != nil {
		return serveErr
	}
	logger.Info("goredis shut down")
	return nil
}

// openSnapshot prepares the snapshot file and loads whatever it already
// holds. A missing file is how a first start looks, so it is not an
// error; a corrupt one is, because starting with silently incomplete data
// is worse than refusing to start.
func openSnapshot(db *store.Store, dir, name string, logger *slog.Logger) (*snapshot.Saver, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, name)

	records, err := snapshot.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		logger.Info("no snapshot to load, starting with an empty keyspace", "path", path)
	case err != nil:
		return nil, fmt.Errorf("loading %s: %w", path, err)
	default:
		if err := db.Restore(records); err != nil {
			return nil, fmt.Errorf("loading %s: %w", path, err)
		}
		logger.Info("snapshot loaded", "path", path, "keys", db.Len())
	}
	return snapshot.NewSaver(db, path, logger), nil
}
