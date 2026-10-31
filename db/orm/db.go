package orm

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"

	"github.com/thanhbvha/go-common/db/orm/backend"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

// Client owns one pool. Do not copy. Close is terminal and retryable after its
// waiting context expires; admitted repository operations and transactions drain
// before the pool closes.
type Client struct {
	db           *gorm.DB
	pool         *sql.DB
	cfg          Config
	mu           sync.Mutex
	closing      bool
	active       int
	drained      chan struct{}
	done         chan struct{}
	closeErr     error
	inFlight     int
	queued       int
	changed      chan struct{}
	admission    AdmissionStats
	transactions TransactionStats
	measurements *measurements
	tracer       trace.Tracer
	backend      backend.Backend
	capabilities backend.Capabilities
}

func Open(ctx context.Context, cfg Config) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = cfg.MaxOpenConns
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.StartupTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, normalize("open", err)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	dialector, err := cfg.Backend.Prepare(ctx)
	if err != nil {
		return nil, normalizeBackend("prepare", err, cfg.Backend)
	}
	published := false
	if owned, ok := dialector.(backend.PreparedPool); ok {
		defer func() {
			if !published && owned.PreparedPool() != nil {
				_ = owned.PreparedPool().Close()
			}
		}()
	}
	if dialector == nil || dialector.Name() != cfg.Backend.Name() {
		return nil, invalid("backend_dialector_mismatch")
	}
	db, err := gorm.Open(dialector, &gorm.Config{DisableAutomaticPing: true,
		Logger:                safeLogger{logger: logger, slow: cfg.SlowThreshold, backend: cfg.Backend},
		DefaultContextTimeout: cfg.QueryTimeout, DefaultTransactionTimeout: cfg.TransactionTimeout})
	if err != nil {
		if db != nil {
			if p, e := db.DB(); e == nil {
				_ = p.Close()
			}
		}
		return nil, normalizeBackend("open", err, cfg.Backend)
	}
	pool, err := db.DB()
	if err != nil {
		return nil, normalize("pool", err)
	}
	pool.SetMaxOpenConns(cfg.MaxOpenConns)
	pool.SetMaxIdleConns(cfg.MaxIdleConns)
	pool.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	pool.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	if err := pool.PingContext(ctx); err != nil {
		_ = pool.Close()
		return nil, normalizeBackend("ping", err, cfg.Backend)
	}
	capabilities, err := cfg.Backend.Inspect(ctx, db)
	if err != nil {
		_ = pool.Close()
		return nil, normalizeBackend("inspect", err, cfg.Backend)
	}
	if capabilities.Name != cfg.Backend.Name() {
		_ = pool.Close()
		return nil, invalid("backend_capability_mismatch")
	}
	if err := cfg.Require.Check(capabilities); err != nil {
		_ = pool.Close()
		return nil, err
	}
	var tracer trace.Tracer
	var measured *measurements
	if cfg.EnableTelemetry {
		plugin := newTelemetry(cfg.TracerProvider)
		tracer = plugin.tracer
		if err := db.Use(plugin); err != nil {
			_ = pool.Close()
			return nil, normalize("telemetry", err)
		}
		measured, err = newMeasurements(cfg.MeterProvider, pool)
		if err != nil {
			_ = pool.Close()
			return nil, normalize("telemetry", err)
		}
	}
	if err := ctx.Err(); err != nil {
		if measured != nil {
			_ = measured.close()
		}
		_ = pool.Close()
		return nil, normalize("open", err)
	}
	published = true
	return &Client{db: db, pool: pool, cfg: cfg, drained: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}), tracer: tracer, measurements: measured, backend: cfg.Backend, capabilities: capabilities}, nil
}

// DB is a trusted escape hatch for migrations and application-specific queries.
// Native work bypasses repository limits and transaction admission/scope guards.
// Set a context and stop such work before closing the client.
func (c *Client) DB() *gorm.DB       { return c.db.Session(&gorm.Session{}) }
func (c *Client) Stats() sql.DBStats { return c.pool.Stats() }

// Capabilities returns the verified backend profile, including runtime limits.
func (c *Client) Capabilities() backend.Capabilities {
	s := c.capabilities
	s.IsolationLevels = append([]sql.IsolationLevel(nil), s.IsolationLevels...)
	return s
}
func (c *Client) normalize(op string, err error) error { return normalizeBackend(op, err, c.backend) }
func (c *Client) resultError(op string, db *gorm.DB) error {
	return normalizeBackend(op, resultError(op, db), c.backend)
}
func (c *Client) setupTransaction(db *gorm.DB) error {
	return c.normalize("transaction_setup", c.backend.SetupTransaction(db.Statement.Context, db))
}
func (c *Client) Ping(ctx context.Context) error {
	c.mu.Lock()
	closed := c.closing
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.QueryTimeout)
	defer cancel()
	return c.normalize("ping", c.pool.PingContext(ctx))
}
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	if !c.closing {
		c.closing = true
		c.notifyLocked()
		if c.inFlight == 0 {
			close(c.drained)
		}
		go func() {
			<-c.drained
			var metricErr error
			if c.measurements != nil {
				metricErr = c.measurements.close()
			}
			err := normalize("close", errors.Join(metricErr, c.pool.Close()))
			c.mu.Lock()
			c.closeErr = err
			close(c.done)
			c.mu.Unlock()
		}()
	}
	done := c.done
	c.mu.Unlock()
	select {
	case <-done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.closeErr
	case <-ctx.Done():
		return normalize("close", ctx.Err())
	}
}
func (c *Client) baseDB() *gorm.DB { return c.db }
func (c *Client) owner() *Client   { return c }
func (c *Client) session(ctx context.Context) (*gorm.DB, context.CancelFunc, error) {
	if ctx.Value(transactionKey{}) != nil {
		return nil, nil, ErrTransactionContext
	}
	ctx, cancel := context.WithTimeout(ctx, c.cfg.QueryTimeout)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, nil, normalize("query", err)
	}
	release, err := c.acquire(ctx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	var once sync.Once
	return c.db.WithContext(ctx), func() { once.Do(func() { cancel(); release() }) }, nil
}
