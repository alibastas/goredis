package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/server"
	"github.com/alibastas/goredis/internal/store"
)

func TestPercentile(t *testing.T) {
	ms := func(n int) time.Duration { return time.Duration(n) * time.Millisecond }
	samples := []time.Duration{ms(1), ms(2), ms(3), ms(4), ms(5), ms(6), ms(7), ms(8), ms(9), ms(10)}

	tests := []struct {
		p    float64
		want time.Duration
	}{
		{0, ms(1)},
		{10, ms(1)},
		{50, ms(5)},
		{90, ms(9)},
		{99, ms(10)},
		{100, ms(10)},
	}
	for _, tt := range tests {
		if got := percentile(samples, tt.p); got != tt.want {
			t.Errorf("percentile(p%v) = %v, want %v", tt.p, got, tt.want)
		}
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile of no samples = %v, want 0", got)
	}
}

// TestRunTest runs every benchmark against a real in-process server with
// pipelining, and checks that each request got exactly one reply.
func TestRunTest(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(command.NewRegistry(store.New()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.Serve(ctx, ln)
		close(done)
	}()
	t.Cleanup(func() { cancel(); <-done })

	cfg := config{addr: ln.Addr().String(), clients: 7, requests: 500, pipeline: 8, keyspace: 50, value: "xyz"}
	for name, mk := range tests {
		res, err := runTest(cfg, mk)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(res.latencies) != cfg.requests || res.errors != 0 {
			t.Fatalf("%s: %d latencies and %d errors, want %d and 0", name, len(res.latencies), res.errors, cfg.requests)
		}
	}
}
