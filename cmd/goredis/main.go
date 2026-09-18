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

	srv := server.New(command.NewRegistry(store.New()), logger)
	if err := srv.Serve(ctx, ln); err != nil {
		return err
	}
	logger.Info("goredis shut down")
	return nil
}
