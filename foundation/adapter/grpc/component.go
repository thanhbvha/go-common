// Package grpc adapts gRPC servers to foundation components.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/thanhbvha/go-common/foundation"
	grpcframework "google.golang.org/grpc"
)

// Component manages listener creation, serving, and graceful shutdown for server.
func Component(name, address string, server *grpcframework.Server) foundation.Component {
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
