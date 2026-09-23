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
	"time"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/persistence/aof"
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
	appendOnly := flag.Bool("appendonly", false, "log every write to an append-only file and replay it at startup")
	appendFsync := flag.String("appendfsync", "everysec", "how often to force the log to disk: always, everysec or no")
	appendFilename := flag.String("appendfilename", "appendonly.aof", "append-only file name")
	rewritePercentage := flag.Int("auto-aof-rewrite-percentage", 100,
		"rewrite the append-only file once it has grown this much since the last rewrite (0 disables it)")
	rewriteMinSize := byteSize(64 << 20)
	flag.Var(&rewriteMinSize, "auto-aof-rewrite-min-size",
		"never rewrite the append-only file below this size")
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
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return err
	}

	// Whatever was saved last time is loaded before anyone is let in, so no
	// client can see an empty keyspace that is about to fill up. With the
	// append-only file enabled it is the only source of truth, the way
	// Redis does it: mixing a snapshot with a log that covers a different
	// stretch of time is a good way to resurrect deleted keys.
	var saver *snapshot.Saver
	var appendLog *aof.Log
	var registryOpts []command.Option

	if *useSnapshot {
		path := filepath.Join(*dir, *dbfilename)
		if !*appendOnly {
			if err := loadSnapshot(db, path, logger); err != nil {
				return err
			}
		}
		saver = snapshot.NewSaver(db, path, logger)
		registryOpts = append(registryOpts, command.WithPersistence(saver))
	}

	if *appendOnly {
		policy, err := aof.ParseFsyncPolicy(*appendFsync)
		if err != nil {
			return err
		}
		path := filepath.Join(*dir, *appendFilename)
		if err := replayLog(db, path, logger); err != nil {
			return err
		}
		appendLog, err = aof.OpenWithOptions(path, aof.Options{
			Policy:            policy,
			RewritePercentage: *rewritePercentage,
			RewriteMinSize:    int64(rewriteMinSize),
			Logger:            logger,
		})
		if err != nil {
			return err
		}
		// Closing flushes and fsyncs whatever is still buffered, on every
		// way out of this function.
		defer func() {
			if err := appendLog.Close(); err != nil {
				logger.Error("could not close the append-only file", "err", err)
			}
		}()
		logger.Info("append-only file is on",
			"path", path, "appendfsync", policy,
			"auto-rewrite-percentage", *rewritePercentage, "auto-rewrite-min-size", rewriteMinSize)
		registryOpts = append(registryOpts, command.WithAppendOnly(appendLog))
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

	registry := command.NewRegistry(db, registryOpts...)
	if appendLog != nil {
		// A rewrite copies the keyspace through the command table rather
		// than straight from the store. Only the command table can take
		// that copy at a moment when no command sits between changing the
		// keyspace and being written to the log.
		appendLog.SetExport(registry.ExportForRewrite)
	}

	srv := server.New(registry, logger)
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

// loadSnapshot fills db from the snapshot at path. A missing file is how
// a first start looks, so it is not an error; a corrupt one is, because
// starting with silently incomplete data is worse than refusing to start.
func loadSnapshot(db *store.Store, path string, logger *slog.Logger) error {
	records, err := snapshot.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		logger.Info("no snapshot to load, starting with an empty keyspace", "path", path)
	case err != nil:
		return fmt.Errorf("loading %s: %w", path, err)
	default:
		if err := db.Restore(records); err != nil {
			return fmt.Errorf("loading %s: %w", path, err)
		}
		logger.Info("snapshot loaded", "path", path, "keys", db.Len())
	}
	return nil
}

// replayLog rebuilds db by running the append-only file through a command
// table of its own. That table has no log attached, which is the point:
// replaying through the live one would append every command right back to
// the file it came from.
func replayLog(db *store.Store, path string, logger *slog.Logger) error {
	replay := command.NewRegistry(db)
	start := time.Now()

	n, err := aof.Load(path, replay.Replay, logger)
	switch {
	case errors.Is(err, os.ErrNotExist):
		logger.Info("no append-only file to replay, starting with an empty keyspace", "path", path)
		return nil
	case err != nil:
		return fmt.Errorf("replaying %s: %w", path, err)
	}
	logger.Info("append-only file replayed",
		"path", path, "commands", n, "keys", db.Len(), "took", time.Since(start))
	return nil
}
