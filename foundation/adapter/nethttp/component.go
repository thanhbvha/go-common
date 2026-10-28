// Package nethttp adapts net/http servers to foundation components.
// Gin and Echo engines can be supplied as the server Handler because both
// implement http.Handler.
package nethttp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/thanhbvha/go-common/foundation"
)

type settings struct{ allowUnsafeTimeouts bool }

// Option customizes net/http adapter validation.
type Option func(*settings)

// AllowUnsafeTimeouts permits a server without an effective header read
// timeout. Use only when an upstream proxy enforces an equivalent limit or the
// application has a documented transport-specific requirement.
func AllowUnsafeTimeouts() Option {
	return func(settings *settings) { settings.allowUnsafeTimeouts = true }
}

// Component manages listener creation, serving, and graceful shutdown for server.
// server.Addr must be set; when it uses port 0, Start replaces it with the bound
// listener address.
func Component(name string, server *http.Server, options ...Option) foundation.Component {
	config := settings{}
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
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
				return errors.New("foundation/nethttp: server must not be nil")
			}
			if server.Addr == "" {
				return errors.New("foundation/nethttp: server address is required")
			}
			if server.Handler == nil {
				return errors.New("foundation/nethttp: server handler is required; DefaultServeMux is not allowed")
			}
			hasHeaderTimeout := server.ReadHeaderTimeout > 0 ||
				(server.ReadHeaderTimeout == 0 && server.ReadTimeout > 0)
			if !config.allowUnsafeTimeouts && !hasHeaderTimeout {
				return errors.New("foundation/nethttp: positive ReadHeaderTimeout or ReadTimeout is required")
			}
			bound, err := (&net.ListenConfig{}).Listen(ctx, "tcp", server.Addr)
			if err != nil {
				return fmt.Errorf("foundation/nethttp: listen on %s: %w", server.Addr, err)
			}
			listener = bound
			server.Addr = bound.Addr().String()
			return nil
		},
		Run: func(ctx context.Context) error {
			if listener == nil {
				return errors.New("foundation/nethttp: server was not started")
			}
			err := server.Serve(listener)
			if ctx.Err() != nil && errors.Is(err, http.ErrServerClosed) {
				return ctx.Err()
			}
			return err
		},
		Stop: func(ctx context.Context) error {
			if server == nil {
				return nil
			}
			return errors.Join(server.Shutdown(ctx), closeListener())
		},
	}
}
