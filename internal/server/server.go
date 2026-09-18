// Package server accepts TCP connections and runs the request/reply loop
// for each client.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/alibastas/goredis/internal/command"
	"github.com/alibastas/goredis/internal/resp"
)

// Server serves RESP clients. Each connection is handled by its own
// goroutine.
type Server struct {
	registry *command.Registry
	logger   *slog.Logger

	// wg counts running connection goroutines so shutdown can wait for
	// all of them to finish.
	wg sync.WaitGroup

	// mu protects the fields below. They are touched by the accept loop,
	// every connection goroutine and the shutdown path, which may all run
	// at the same time.
	mu           sync.Mutex
	conns        map[net.Conn]struct{}
	shuttingDown bool
}

func New(registry *command.Registry, logger *slog.Logger) *Server {
	return &Server{
		registry: registry,
		logger:   logger,
		conns:    make(map[net.Conn]struct{}),
	}
}

// Serve accepts connections on ln until ctx is cancelled or accepting
// fails. Before returning it closes the listener and every open client
// connection, then waits for their goroutines to exit. It returns nil
// after a shutdown triggered by ctx.
//
// A Server is meant to be served once.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	// Accept blocks until a client connects, so it can't check ctx on its
	// own. Instead, once ctx is cancelled, close the listener: that makes
	// the blocked Accept return an error and the loop below ends.
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	defer s.shutdown(ln)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if !s.trackConn(conn) {
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrackConn(conn)
			s.handleConn(conn)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	log := s.logger.With("remote", conn.RemoteAddr().String())
	log.Debug("client connected")
	defer log.Debug("client disconnected")

	r := resp.NewReader(conn)
	w := resp.NewWriter(conn)

	for {
		// Replies are buffered and only sent once there is nothing left to
		// read, i.e. right before we would block waiting for the client.
		// A client that pipelines 100 commands in one packet therefore gets
		// all 100 replies in a single write instead of 100 small ones.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				log.Debug("write failed", "err", err)
				return
			}
		}

		req, err := r.ReadValue()
		if err != nil {
			s.handleReadError(log, w, err)
			return
		}

		// Redis silently ignores an empty command array; so do we.
		if req.Type == resp.Array && len(req.Array) == 0 {
			continue
		}

		if err := w.WriteValue(s.registry.Dispatch(req)); err != nil {
			// Only happens if a handler returns a malformed Value, which is
			// a bug on our side, not the client's.
			log.Error("cannot encode reply", "err", err)
			return
		}
	}
}

func (s *Server) handleReadError(log *slog.Logger, w *resp.Writer, err error) {
	switch {
	case errors.Is(err, resp.ErrProtocol):
		// Once the stream is malformed there is no way to find where the
		// next command starts, so tell the client why and hang up.
		w.WriteValue(resp.NewError("ERR " + err.Error()))
		w.Flush()
		log.Debug("closing connection after protocol error", "err", err)
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		// The client hung up, or we closed the connection during shutdown.
	default:
		log.Debug("read failed", "err", err)
	}
}

// trackConn records an open connection. It returns false if the server is
// already shutting down, in which case the caller must close conn itself.
func (s *Server) trackConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}

func (s *Server) untrackConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, conn)
}

// shutdown stops accepting new clients, closes the ones that are still
// connected and waits until all their goroutines have returned.
func (s *Server) shutdown(ln net.Listener) {
	ln.Close()

	s.mu.Lock()
	s.shuttingDown = true
	for conn := range s.conns {
		// Closing the connection unblocks a goroutine waiting in
		// ReadValue, which then returns net.ErrClosed and exits.
		conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
}
