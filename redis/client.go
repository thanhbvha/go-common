package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Client wraps a redis.UniversalClient with lifecycle management, a periodic
// health-check goroutine, and an optional structured logger.
// All exported methods are safe for concurrent use.
type Client struct {
	cfg       Config
	rc        goredis.UniversalClient
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

// Connect establishes the Redis connection, retrying up to Config.MaxConnRetries
// times with an exponential back-off. It also starts the background
// health-check goroutine. Calling Connect on an already-connected Client is a
// no-op.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	rc, err := c.buildClient()
	if err != nil {
		return err
	}

	if err := c.pingWithRetry(ctx, rc, c.cfg.MaxConnRetries); err != nil {
		_ = rc.Close()
		return fmt.Errorf("redis: failed to connect after %d retries: %w", c.cfg.MaxConnRetries, err)
	}

	c.rc = rc
	c.connected = true

	go c.runHealthCheck()

	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("redis: connected",
			"mode", c.modeName(),
			"pool_size", c.cfg.PoolSize,
		)
	}

	return nil
}

// MustConnect is a convenience wrapper that calls Connect and panics on error.
// Useful for application startup where a Redis connection is required.
func MustConnect(ctx context.Context, cfg Config) *Client {
	cl := New(cfg)
	if err := cl.Connect(ctx); err != nil {
		panic(fmt.Sprintf("redis: MustConnect failed: %v", err))
	}
	return cl
}

// Close stops the health-check goroutine and closes all underlying connections.
// Subsequent calls return nil without doing any work.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	if c.rc != nil {
		err := c.rc.Close()
		c.rc = nil
		c.connected = false
		if c.cfg.Logger != nil {
			c.cfg.Logger.Info("redis: connection closed")
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

// Ping sends a PING command and returns the server response.
func (c *Client) Ping(ctx context.Context) error {
	c.mu.RLock()
	rc := c.rc
	c.mu.RUnlock()

	if rc == nil {
		return goredis.ErrClosed
	}
	_, err := rc.Ping(ctx).Result()
	return err
}

// Native returns the underlying redis.UniversalClient for advanced operations
// not covered by this wrapper. Use with care.
func (c *Client) Native() goredis.UniversalClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rc
}

// PoolStats returns current connection-pool statistics or nil if not connected.
func (c *Client) PoolStats() *goredis.PoolStats {
	c.mu.RLock()
	rc := c.rc
	c.mu.RUnlock()
	if rc == nil {
		return nil
	}
	return rc.PoolStats()
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
// It panics if SetDefault has not been called, because proceeding without a
// configured client would produce confusing errors further down the call stack.
func Default() *Client {
	globalMu.RLock()
	c := globalClient
	globalMu.RUnlock()
	if c == nil {
		panic("redis: default client not set — call redis.SetDefault before use")
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

// buildClient constructs the appropriate goredis client based on Config.Mode.
func (c *Client) buildClient() (goredis.UniversalClient, error) {
	cfg := c.cfg
	switch cfg.Mode {
	case ModeCluster:
		if len(cfg.ClusterAddrs) == 0 {
			return nil, fmt.Errorf("redis: ClusterAddrs must not be empty for ModeCluster")
		}
		return goredis.NewClusterClient(&goredis.ClusterOptions{
			Addrs:           cfg.ClusterAddrs,
			Password:        cfg.Password,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			MaxRetries:      cfg.MaxRetries,
			MinRetryBackoff: cfg.MinRetryBackoff,
			MaxRetryBackoff: cfg.MaxRetryBackoff,
		}), nil

	case ModeSentinel:
		if len(cfg.SentinelAddrs) == 0 {
			return nil, fmt.Errorf("redis: SentinelAddrs must not be empty for ModeSentinel")
		}
		return goredis.NewFailoverClient(&goredis.FailoverOptions{
			MasterName:      cfg.MasterName,
			SentinelAddrs:   cfg.SentinelAddrs,
			Password:        cfg.Password,
			DB:              cfg.DB,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			MaxRetries:      cfg.MaxRetries,
			MinRetryBackoff: cfg.MinRetryBackoff,
			MaxRetryBackoff: cfg.MaxRetryBackoff,
		}), nil

	default: // ModeSingle
		return goredis.NewClient(&goredis.Options{
			Addr:            fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
			Password:        cfg.Password,
			DB:              cfg.DB,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxIdleTime: cfg.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.ConnMaxLifetime,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			MaxRetries:      cfg.MaxRetries,
			MinRetryBackoff: cfg.MinRetryBackoff,
			MaxRetryBackoff: cfg.MaxRetryBackoff,
		}), nil
	}
}

// pingWithRetry sends a PING to rc, retrying maxRetries times with linear
// back-off (1s, 2s, …) between attempts.
func (c *Client) pingWithRetry(ctx context.Context, rc goredis.UniversalClient, maxRetries int) error {
	if maxRetries <= 0 {
		maxRetries = 1
	}
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := rc.Ping(pingCtx).Result()
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		if c.cfg.Logger != nil {
			c.cfg.Logger.Warn("redis: connection attempt failed",
				"attempt", i+1,
				"max", maxRetries,
				"err", err.Error(),
			)
		}
		if i < maxRetries-1 {
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}
	return lastErr
}

// runHealthCheck pings the server every 30 seconds and updates c.connected.
func (c *Client) runHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			rc := c.rc
			connected := c.connected
			c.mu.RUnlock()

			if !connected || rc == nil {
				continue
			}

			hcCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := rc.Ping(hcCtx).Result()
			cancel()

			if err != nil {
				c.mu.Lock()
				c.connected = false
				c.mu.Unlock()
				if c.cfg.Logger != nil {
					c.cfg.Logger.Error("redis: health check failed", "err", err.Error())
				}
			}
		}
	}
}

// modeName returns a human-readable label for the current connection mode.
func (c *Client) modeName() string {
	switch c.cfg.Mode {
	case ModeCluster:
		return "cluster"
	case ModeSentinel:
		return "sentinel"
	default:
		return "single"
	}
}
