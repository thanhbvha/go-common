package nats

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
)

// ErrClosed identifies a terminal client lifecycle. Construct a new Client to reconnect.
var ErrClosed = errors.New("nats: client is closed")

// Client owns one native connection. Native reconnects retain that connection;
// Close/Drain are terminal and never allow a second connection lifecycle.
type Client struct {
	cfg       Config
	lifecycle sync.Mutex
	mu        sync.RWMutex
	nc        *gonats.Conn
	js        gonats.JetStreamContext
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	doneOnce  sync.Once
	drainErr  error
}

var (
	globalClient *Client
	globalMu     sync.RWMutex
)

func New(cfg Config) *Client {
	cfg.URLs = slices.Clone(cfg.URLs)
	if len(cfg.URLs) == 0 {
		cfg.URLs = []string{"nats://localhost:4222"}
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 5 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{cfg: cfg, ctx: ctx, cancel: cancel, done: make(chan struct{})}
}
func MustConnect(ctx context.Context, cfg Config) *Client {
	c := New(cfg)
	if err := c.Connect(ctx); err != nil {
		panic(fmt.Sprintf("nats: MustConnect failed: %v", err))
	}
	return c
}

// Connect bounds every startup dial, handshake, and retry by ctx. The native
// client's own reconnect policy takes over after a successful startup.
func (c *Client) Connect(ctx context.Context) error {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	c.mu.RLock()
	closed, initialized := c.closed, c.nc != nil
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if initialized {
		return nil
	}
	startup, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	if err := startup.Err(); err != nil {
		return err
	}
	opts, err := c.buildOptions()
	if err != nil {
		return err
	}
	dialer := &startupDialer{ctx: startup, timeout: c.cfg.ConnectTimeout}
	defer dialer.finish()
	opts = append(opts, gonats.SetCustomDialer(dialer), gonats.DrainTimeout(c.cfg.DrainTimeout))
	var nc *gonats.Conn
	attempts := max(c.cfg.MaxConnRetries, 1)
	for i := 0; i < attempts; i++ {
		if err := startup.Err(); err != nil {
			return err
		}
		nc, err = gonats.Connect(strings.Join(c.cfg.URLs, ","), opts...)
		if err == nil {
			break
		}
		if c.cfg.Logger != nil {
			c.cfg.Logger.WarnAsync("nats: connection attempt failed", "attempt", i+1, "error", err)
		}
		if i < attempts-1 {
			timer := time.NewTimer(time.Duration(i+1) * time.Second)
			select {
			case <-startup.Done():
				timer.Stop()
				return startup.Err()
			case <-timer.C:
			}
		}
	}
	if err != nil {
		if startup.Err() != nil {
			return startup.Err()
		}
		return fmt.Errorf("nats: connect after %d attempts: %w", attempts, err)
	}
	if err := startup.Err(); err != nil {
		nc.Close()
		return err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return err
	}
	dialer.finish()
	c.mu.Lock()
	c.nc, c.js = nc, js
	c.mu.Unlock()
	return nil
}

// startupDialer interrupts a canceled initial handshake as well as TCP dialing.
// finish detaches startup cancellation before native background reconnects begin.
type startupDialer struct {
	mu      sync.Mutex
	ctx     context.Context
	timeout time.Duration
	stops   []func() bool
}

func (d *startupDialer) Dial(network, address string) (net.Conn, error) {
	d.mu.Lock()
	ctx := d.ctx
	d.mu.Unlock()
	conn, err := (&net.Dialer{Timeout: d.timeout}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.ctx == ctx && ctx.Done() != nil {
		d.stops = append(d.stops, context.AfterFunc(ctx, func() { _ = conn.Close() }))
	}
	d.mu.Unlock()
	return conn, nil
}
func (d *startupDialer) finish() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, stop := range d.stops {
		stop()
	}
	d.stops = nil
	d.ctx = context.Background()
}

// Drain rejects new wrapper operations and waits for all native subscriptions
// and publishers to drain. Deadline expiry force-closes the socket. Every caller
// can bound its own wait; Close and Drain are terminal and idempotent.
func (c *Client) Drain(ctx context.Context) error {
	c.cancel()
	c.lifecycle.Lock()
	c.mu.Lock()
	first := !c.closed
	c.closed = true
	nc := c.nc
	c.mu.Unlock()
	if first {
		if nc == nil {
			c.doneOnce.Do(func() { close(c.done) })
		} else {
			err := nc.Drain()
			if errors.Is(err, gonats.ErrConnectionClosed) {
				err = nil
			}
			c.mu.Lock()
			c.drainErr = err
			c.mu.Unlock()
			if err != nil {
				nc.Close()
			}
		}
	}
	c.lifecycle.Unlock()
	select {
	case <-c.done:
		c.mu.RLock()
		err := c.drainErr
		c.mu.RUnlock()
		return err
	default:
	}
	select {
	case <-c.done:
		c.mu.RLock()
		err := c.drainErr
		c.mu.RUnlock()
		return err
	case <-ctx.Done():
		if nc != nil {
			nc.Close()
		}
		return ctx.Err()
	}
}
func (c *Client) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.DrainTimeout)
	defer cancel()
	return c.Drain(ctx)
}
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.closed && c.nc != nil && c.nc.IsConnected()
}

// Ping performs a server round trip with the caller's deadline.
func (c *Client) Ping(ctx context.Context) error {
	nc, _, err := c.requireConnected()
	if err != nil {
		return err
	}
	check, cancel := context.WithTimeout(ctx, c.cfg.ConnectTimeout)
	defer cancel()
	return nc.FlushWithContext(check)
}
func (c *Client) Native() *gonats.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
	return c.nc
}

// JetStream retains the legacy context API for native integrations such as eventbus.
func (c *Client) JetStream() gonats.JetStreamContext {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
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

// LookupDefault returns the process-wide default Client and whether it has
// been configured. It is the non-panicking alternative to Default.
func LookupDefault() (*Client, bool) {
	globalMu.RLock()
	c := globalClient
	globalMu.RUnlock()
	return c, c != nil
}

// Default returns the process-wide default Client registered via SetDefault.
// It panics if SetDefault has not been called.
func Default() *Client {
	c, ok := LookupDefault()
	if !ok {
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

func (c *Client) buildOptions() ([]gonats.Option, error) {
	cfg := c.cfg

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
			c.mu.Lock()
			active := c.nc == nc
			if active {
				c.closed = true
			}
			c.mu.Unlock()
			if active {
				c.doneOnce.Do(func() { close(c.done) })
			}
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

func (c *Client) requireConnected() (*gonats.Conn, gonats.JetStreamContext, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, nil, ErrClosed
	}
	if c.nc == nil {
		return nil, nil, gonats.ErrConnectionClosed
	}
	return c.nc, c.js, nil
}

// A scoped legacy context propagates ctx into every admin request and iterator.
func (c *Client) requireContext(ctx context.Context) (*gonats.Conn, gonats.JetStreamContext, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	nc, _, err := c.requireConnected()
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream(gonats.Context(ctx))
	return nc, js, err
}
