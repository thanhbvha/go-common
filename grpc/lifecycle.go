package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"google.golang.org/grpc"
)

// Serve runs a standalone server, handling SIGINT/SIGTERM and bounded shutdown.
// It no longer registers with the deprecated utils/graceful global registry.
// Applications with multiple components should use ListenAndServe or ServeContext
// with their application context, and let one application own signal handling.
func (s *Server) Serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return s.ListenAndServe(ctx)
}

// ListenAndServe binds Address (or :Port) with ctx, then serves until shutdown.
// It owns the listener and does not install signal handlers or global callbacks.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	addr := fmt.Sprintf(":%d", s.config.Port)
	if s.config.Address != "" {
		addr = s.config.Address
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}

	logger.Info("gRPC Server is running", "address", lis.Addr().String())
	return s.ServeContext(ctx, lis)
}

// Shutdown marks health as not serving and drains active RPCs. On deadline it
// requests forced transport closure and returns the context error. Concurrent
// calls share one drain and at most one forced stop. The first forced-shutdown
// error is retained for later callers and the serving result. Handlers must honor
// cancellation; Go cannot forcibly terminate an application goroutine. Native
// stop calls run asynchronously because user handlers/options can block them.
func (s *Server) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.shutdownTimeout())
	defer cancel()
	s.shutdownOnce.Do(func() {
		close(s.shutdownStarted)
		if s.Health != nil {
			s.Health.Shutdown()
		}
		go func() {
			s.Server.GracefulStop()
			close(s.shutdownDone)
		}()
	})
	select {
	case <-s.shutdownDone:
		return s.shutdownResult()
	case <-s.shutdownForced:
		return s.shutdownResult()
	case <-ctx.Done():
		select {
		case <-s.shutdownDone:
			return s.shutdownResult()
		default:
		}
		s.forceOnce.Do(func() {
			s.shutdownMu.Lock()
			s.shutdownErr = ctx.Err()
			s.shutdownMu.Unlock()
			go s.Server.Stop()
			close(s.shutdownForced)
		})
		return s.shutdownResult()
	}
}

func (s *Server) shutdownResult() error {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	return s.shutdownErr
}

func (s *Server) shutdownTimeout() time.Duration {
	if s.config.ShutdownTimeout > 0 {
		return s.config.ShutdownTimeout
	}
	return 30 * time.Second
}

// ServeContext serves an owned listener without process-global shutdown hooks.
// Use in Component.Run; register Shutdown in Component.Stop. The caller should
// cancel ctx on SIGTERM/SIGINT. ServeContext waits for draining before returning.
func (s *Server) ServeContext(ctx context.Context, lis net.Listener) error {
	if err := ctx.Err(); err != nil {
		_ = lis.Close()
		return err
	}
	done := make(chan struct{})
	stopped := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			stopped <- s.Shutdown(context.Background())
		case <-done:
			stopped <- nil
		}
	}()
	served := make(chan error, 1)
	go func() { served <- s.Server.Serve(lis) }()
	var err error
	select {
	case err = <-served:
	case <-s.shutdownForced:
		// Native Serve may itself wait for stop to finish. A handler that ignores
		// cancellation must not extend this wrapper's shutdown deadline.
		_ = lis.Close()
		select {
		case err = <-served:
		default:
		}
	}
	close(done)
	shutdownErr := <-stopped
	// Native Serve can return as soon as listeners close, while an independently
	// initiated Shutdown is still draining. Preserve its terminal result too.
	select {
	case <-s.shutdownStarted:
		select {
		case <-s.shutdownDone:
		case <-s.shutdownForced:
		}
		if terminal := s.shutdownResult(); terminal != nil {
			shutdownErr = terminal
		}
	default:
	}
	if errors.Is(err, grpc.ErrServerStopped) {
		err = nil
	}
	return errors.Join(err, shutdownErr)
}
