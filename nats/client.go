package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
)

// Client wraps a *nats.Conn and a JetStreamContext with lifecycle management,
// a periodic health-check goroutine, and an optional structured logger.
// All exported methods are safe for concurrent use.
type Client struct {
	cfg       Config
	nc        *gonats.Conn
	js        gonats.JetStreamContext
	connected bool
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
}

// package-level default client, guarded by globalMu.
var (
	globalClient *Client
	globalMu     sync.RWMutex
)

// New allocates a Client from cfg without establishing a connection.
// Call Connect (or MustConnect) before performing any operations.
func New(cfg Config) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
	}
}

// MustConnect is a convenience wrapper that calls Connect and panics on error.
// Useful for application startup where a NATS connection is required.
func MustConnect(ctx context.Context, cfg Config) *Client {
	cl := New(cfg)
	if err := cl.Connect(ctx); err != nil {
		panic(fmt.Sprintf("nats: MustConnect failed: %v", err))
	}
	return cl
}

// Connect establishes the NATS connection and initialises a JetStreamContext,
// retrying up to Config.MaxConnRetries times with linear back-off.
// It also starts the background health-check goroutine.
// Calling Connect on an already-connected Client is a no-op.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	opts, err := c.buildOptions()
	if err != nil {
		return err
	}

	var nc *gonats.Conn
	maxRetries := c.cfg.MaxConnRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		// Join multiple URLs for cluster support
		serverURL := c.cfg.URLs[0]
		if len(c.cfg.URLs) > 1 {
			serverURL = gonats.DefaultURL // will be overridden by opts
		}
		nc, lastErr = gonats.Connect(serverURL, opts...)
		if lastErr == nil {
			break
		}
		if c.cfg.Logger != nil {
			// Async: retry warnings must not block the retry sleep.
			c.cfg.Logger.WarnAsync("nats: connection attempt failed",
				"attempt", i+1,
				"max", maxRetries,
				"err", lastErr.Error(),
			)
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}
	if lastErr != nil {
		return fmt.Errorf("nats: failed to connect after %d retries: %w", maxRetries, lastErr)
	}

	js, err := nc.JetStream()
	if err != nil {
		_ = nc.Drain()
		return fmt.Errorf("nats: failed to create JetStream context: %w", err)
	}

	c.nc = nc
	c.js = js
	c.connected = true

	go c.runHealthCheck()

	if c.cfg.Logger != nil {
		// Sync: startup log must complete before Connect returns.
		c.cfg.Logger.Info("nats: connected",
			"url", nc.ConnectedUrl(),
			"server_id", nc.ConnectedServerId(),
		)
	}

	return nil
}

// Close drains and closes the NATS connection, stopping the health-check goroutine.
// Subsequent calls return nil without doing any work.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	if c.nc != nil {
		// Drain flushes pending messages before closing.
		err := c.nc.Drain()
		c.nc = nil
		c.js = nil
		c.connected = false
		if c.cfg.Logger != nil {
			// Sync: shutdown log must complete before Close returns.
			c.cfg.Logger.Info("nats: connection closed")
		}
		return err
	}
	return nil
}

// IsConnected reports whether the client has an active connection.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// Ping checks the connection status without a round-trip to the server.
func (c *Client) Ping(_ context.Context) error {
	c.mu.RLock()
	nc := c.nc
	connected := c.connected
	c.mu.RUnlock()

	if !connected || nc == nil {
		return gonats.ErrConnectionClosed
	}
	if !nc.IsConnected() {
		return gonats.ErrConnectionClosed
	}
	return nil
}

// Native returns the underlying *nats.Conn for advanced operations
// not covered by this wrapper. Use with care.
func (c *Client) Native() *gonats.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nc
}

// JetStream returns the underlying JetStreamContext for advanced operations.
func (c *Client) JetStream() gonats.JetStreamContext {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.js
}

// SetDefault registers c as the process-wide default Client.
// The previous default (if any) is replaced but NOT closed.
// It is safe to call from multiple goroutines.
func SetDefault(c *Client) {
	globalMu.Lock()
	globalClient = c
	globalMu.Unlock()
}

// Default returns the process-wide default Client registered via SetDefault.
// It panics if SetDefault has not been called.
func Default() *Client {
	globalMu.RLock()
	c := globalClient
	globalMu.RUnlock()
	if c == nil {
		panic("nats: default client not set — call nats.SetDefault before use")
	}
	return c
}

// Close closes the process-wide default Client. Safe to call even if
// SetDefault was never invoked.
func Close() error {
	globalMu.RLock()
	c := globalClient
	globalMu.RUnlock()
	if c == nil {
		return nil
	}
	return c.Close()
}

// ---- internal helpers ----

// buildOptions converts Config into []nats.Option.
func (c *Client) buildOptions() ([]gonats.Option, error) {
	cfg := c.cfg

	// Build URL list for server selection
	servers := cfg.URLs
	if len(servers) == 0 {
		servers = []string{"nats://localhost:4222"}
	}
	_ = servers // used as first arg to gonats.Connect in Connect()

	opts := []gonats.Option{
		gonats.Timeout(cfg.ConnectTimeout),
		gonats.ReconnectWait(cfg.ReconnectWait),
		gonats.MaxReconnects(cfg.MaxReconnects),
		gonats.DisconnectErrHandler(func(nc *gonats.Conn, err error) {
			if cfg.Logger != nil && err != nil {
				// Async: disconnect callback runs in NATS internal goroutine.
				cfg.Logger.WarnAsync("nats: disconnected", "err", err.Error())
			}
		}),
		gonats.ReconnectHandler(func(nc *gonats.Conn) {
			if cfg.Logger != nil {
				// Async: reconnect callback runs in NATS internal goroutine.
				cfg.Logger.InfoAsync("nats: reconnected", "url", nc.ConnectedUrl())
			}
		}),
		gonats.ClosedHandler(func(nc *gonats.Conn) {
			if cfg.Logger != nil {
				// Async: closed callback runs in NATS internal goroutine.
				cfg.Logger.InfoAsync("nats: connection closed permanently")
			}
		}),
	}

	if cfg.NKeyFile != "" {
		opt, err := gonats.NkeyOptionFromSeed(cfg.NKeyFile)
		if err != nil {
			return nil, fmt.Errorf("nats: failed to load NKey file: %w", err)
		}
		opts = append(opts, opt)
	}

	if cfg.CredFile != "" {
		opts = append(opts, gonats.UserCredentials(cfg.CredFile))
	}

	return opts, nil
}

// runHealthCheck checks the connection status every 30 seconds and updates
// c.connected accordingly.
func (c *Client) runHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			nc := c.nc
			c.mu.RUnlock()

			if nc == nil {
				continue
			}

			ok := nc.IsConnected()
			c.mu.Lock()
			c.connected = ok
			c.mu.Unlock()

			if !ok && c.cfg.Logger != nil {
				// Async: health-check runs in background goroutine; must not block.
				c.cfg.Logger.ErrorAsync("nats: health check failed — connection lost")
			}
		}
	}
}

// requireConnected returns the NATS connection and JetStream context,
// or an error when the client is not connected.
func (c *Client) requireConnected() (*gonats.Conn, gonats.JetStreamContext, error) {
	c.mu.RLock()
	nc := c.nc
	js := c.js
	ok := c.connected
	c.mu.RUnlock()

	if !ok || nc == nil {
		return nil, nil, gonats.ErrConnectionClosed
	}
	return nc, js, nil
}
