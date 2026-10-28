// Package fiber adapts Fiber applications to foundation components.
package fiber

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	fiberframework "github.com/gofiber/fiber/v2"
	"github.com/thanhbvha/go-common/foundation"
)

type settings struct{ allowUnsafeTimeouts bool }

// Option customizes Fiber adapter validation.
type Option func(*settings)

// AllowUnsafeTimeouts permits a Fiber app with no ReadTimeout. Use only when an
// upstream proxy enforces an equivalent connection limit.
func AllowUnsafeTimeouts() Option {
	return func(settings *settings) { settings.allowUnsafeTimeouts = true }
}

// Component manages listener creation, serving, and graceful shutdown for app.
func Component(name, address string, app *fiberframework.App, options ...Option) foundation.Component {
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
			if app == nil {
				return errors.New("foundation/fiber: app must not be nil")
			}
			if address == "" {
				return errors.New("foundation/fiber: address is required")
			}
			if !config.allowUnsafeTimeouts && app.Config().ReadTimeout <= 0 {
				return errors.New("foundation/fiber: positive ReadTimeout is required")
			}
			bound, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
			if err != nil {
				return fmt.Errorf("foundation/fiber: listen on %s: %w", address, err)
			}
			listener = bound
			return nil
		},
		Run: func(ctx context.Context) error {
			if listener == nil {
				return errors.New("foundation/fiber: app was not started")
			}
			err := app.Listener(listener)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		},
		Stop: func(ctx context.Context) error {
			if app == nil {
				return nil
			}
			return errors.Join(app.ShutdownWithContext(ctx), closeListener())
		},
	}
}
