// Package grpc adapts gRPC servers to foundation components.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/thanhbvha/go-common/v2/foundation"
	grpcframework "google.golang.org/grpc"
)

// Component manages listener creation, serving, and graceful shutdown for server.
func Component(name, address string, server *grpcframework.Server) foundation.Component {
	return ComponentWithShutdown(name, address, server, nil)
}

// ComponentWithShutdown uses an application's shutdown callback instead of the
// native stop policy. For go-common/grpc pass srv.Server and srv.Shutdown so
// health status, drain timeout and terminal failures stay with the wrapper.
// A nil callback preserves Component's native GracefulStop/Stop behavior.
func ComponentWithShutdown(name, address string, server *grpcframework.Server, shutdown func(context.Context) error) foundation.Component {
	var listener net.Listener
	var closeOnce sync.Once
	var closeErr error
	closeListener := func() error {
		closeOnce.Do(func() {
			if listener != nil {
				closeErr = listener.Close()
			}
		})
		if errors.Is(closeErr, net.ErrClosed) {
			return nil
		}
		return closeErr
	}
	return foundation.Component{
		Name: name,
		Start: func(ctx context.Context) error {
			if server == nil {
				return errors.New("foundation/grpc: server must not be nil")
			}
			if address == "" {
				return errors.New("foundation/grpc: address is required")
			}
			bound, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
			if err != nil {
				return fmt.Errorf("foundation/grpc: listen on %s: %w", address, err)
			}
			listener = bound
			return nil
		},
		Run: func(ctx context.Context) error {
			if listener == nil {
				return errors.New("foundation/grpc: server was not started")
			}
			err := server.Serve(listener)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		},
		Stop: func(ctx context.Context) error {
			if server == nil {
				return nil
			}
			if shutdown != nil {
				return errors.Join(shutdown(ctx), closeListener())
			}
			done := make(chan struct{})
			go func() {
				server.GracefulStop()
				close(done)
			}()
			select {
			case <-done:
				return closeListener()
			case <-ctx.Done():
				server.Stop()
				return errors.Join(ctx.Err(), closeListener())
			}
		},
	}
}
