package kafka

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Client wraps a franz-go kgo.Client with lifecycle management, a periodic
// health-check goroutine, and an optional structured logger.
// All exported methods are safe for concurrent use.
type Client struct {
	cfg       Config
	kgo       *kgo.Client
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
// Suitable for application startup where a Kafka connection is mandatory.
func MustConnect(ctx context.Context, cfg Config) *Client {
	cl := New(cfg)
	if err := cl.Connect(ctx); err != nil {
		panic(fmt.Sprintf("kafka: MustConnect failed: %v", err))
	}
	return cl
}

// Connect establishes connections to the Kafka brokers, applies TLS and SASL
// configuration, and starts the background health-check goroutine.
// It retries up to Config.MaxConnRetries times with linear back-off.
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

	maxRetries := c.cfg.MaxConnRetries
	if maxRetries <= 0 {
		maxRetries = 1
	}

	var (
		kgoClient *kgo.Client
		lastErr   error
	)

	for i := 0; i < maxRetries; i++ {
		kgoClient, lastErr = kgo.NewClient(opts...)
		if lastErr == nil {
			// Perform a metadata ping to verify actual broker connectivity.
			pingCtx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
			lastErr = kgoClient.Ping(pingCtx)
			cancel()
			if lastErr == nil {
				break
			}
			kgoClient.Close()
			kgoClient = nil
		}

		if c.cfg.Logger != nil {
			c.cfg.Logger.WarnAsync("kafka: connection attempt failed",
				"attempt", i+1,
				"max", maxRetries,
				"err", lastErr.Error(),
			)
		}

		if i < maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(i+1) * time.Second):
			}
		}
	}

	if lastErr != nil {
		return fmt.Errorf("kafka: failed to connect after %d retries: %w", maxRetries, lastErr)
	}

	c.kgo = kgoClient
	c.connected = true

	go c.runHealthCheck()

	if c.cfg.Logger != nil {
		c.cfg.Logger.Info("kafka: connected",
			"brokers", c.cfg.Brokers,
			"client_id", c.cfg.ClientID,
		)
	}

	return nil
}

// Close flushes pending produce records, commits pending offsets, and closes
// the underlying Kafka client. Subsequent calls return nil without doing work.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	if c.kgo != nil {
		c.kgo.Close()
		c.kgo = nil
		c.connected = false

		if c.cfg.Logger != nil {
			c.cfg.Logger.Info("kafka: connection closed")
		}
	}

	return nil
}

// IsConnected reports whether the client has an active connection to at least
// one broker.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

// Ping sends a metadata request to the broker cluster to verify connectivity.
// Returns an error if the client is not connected or the request fails.
func (c *Client) Ping(ctx context.Context) error {
	kc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return kc.Ping(ctx)
}

// Config returns a copy of the configuration used to create this client.
func (c *Client) Config() Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cfg := c.cfg
	cfg.Brokers = slices.Clone(c.cfg.Brokers)
	if c.cfg.TLSConfig != nil {
		cfg.TLSConfig = c.cfg.TLSConfig.Clone()
	}
	return cfg
}

// Native returns the underlying *kgo.Client for advanced operations not
// covered by this wrapper. Use with care and avoid closing the raw client
// directly.
func (c *Client) Native() *kgo.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.kgo
}

// SetDefault registers c as the process-wide default Client.
// The previous default (if any) is replaced but NOT closed.
// Safe for concurrent use.
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
		panic("kafka: default client not set — call kafka.SetDefault before use")
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

// buildOptions converts Config into a []kgo.Opt, applying TLS and SASL
// settings based on SecurityProtocol and SASLMechanism.
func (c *Client) buildOptions() ([]kgo.Opt, error) {
	factory, err := NewNativeClientFactory(c.cfg)
	if err != nil {
		return nil, err
	}
	opts := append(slices.Clone(factory.base), factory.producer...)
	if c.cfg.ConsumerGroup != "" {
		opts = append(opts, factory.consumer...)
		opts = append(opts, kgo.ConsumerGroup(c.cfg.ConsumerGroup))
	}
	return opts, nil
}

// runHealthCheck pings the broker cluster every 30 seconds and updates
// c.connected accordingly. It exits when c.ctx is cancelled.
func (c *Client) runHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.RLock()
			kc := c.kgo
			c.mu.RUnlock()

			if kc == nil {
				continue
			}

			pingCtx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			err := kc.Ping(pingCtx)
			cancel()

			c.mu.Lock()
			c.connected = (err == nil)
			c.mu.Unlock()

			if err != nil && c.cfg.Logger != nil {
				c.cfg.Logger.ErrorAsync("kafka: health check failed — connection lost", "err", err.Error())
			}
		}
	}
}

// requireConnected returns the underlying kgo.Client or an error when the
// client is not yet connected. All public operation methods call this guard.
func (c *Client) requireConnected() (*kgo.Client, error) {
	c.mu.RLock()
	kc := c.kgo
	ok := c.connected
	c.mu.RUnlock()

	if !ok || kc == nil {
		return nil, fmt.Errorf("kafka: client is not connected — call Connect first")
	}
	return kc, nil
}

// buildCompressionOpt converts the ProducerCompression string into a kgo.Opt.
func buildCompressionOpt(codec string) (kgo.Opt, error) {
	switch codec {
	case "", "none":
		return kgo.ProducerBatchCompression(kgo.NoCompression()), nil
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), nil
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), nil
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), nil
	case "zstd":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), nil
	default:
		return nil, fmt.Errorf("kafka: unsupported compression codec %q (use none|gzip|snappy|lz4|zstd)", codec)
	}
}

// exponentialBackoff returns a retry back-off function that grows from base up
// to max using binary exponential back-off.
func exponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		backoff := base * (1 << attempt)
		if backoff > max {
			backoff = max
		}
		return backoff
	}
}
