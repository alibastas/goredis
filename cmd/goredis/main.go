// Command goredis runs the goredis server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/alibastas/goredis/internal/command"
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
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	// ctx is cancelled on Ctrl+C or SIGTERM, which starts a clean shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	logger.Info("goredis is ready to accept connections", "addr", ln.Addr().String())

	db := store.NewWithOptions(store.Options{Shards: *shards})

	// Active expiry runs next to the server for as long as ctx lives.
	expiryDone := make(chan struct{})
	go func() {
		defer close(expiryDone)
		db.RunActiveExpiry(ctx)
	}()

	srv := server.New(command.NewRegistry(db), logger)
	serveErr := srv.Serve(ctx, ln)

	// Serve can also return because accepting failed, with ctx still live.
	// Cancel it either way so the expiry goroutine stops, then wait for it.
	stop()
	<-expiryDone

	if serveErr != nil {
		return serveErr
	}
	logger.Info("goredis shut down")
	return nil
}
