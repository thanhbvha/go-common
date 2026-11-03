package orm

import (
	"log/slog"
	"reflect"
	"time"

	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Config owns a newly opened pool. Start with DefaultConfig and set Backend.
// Backend adapters must be immutable and must honor the startup context.
type Config struct {
	Backend            backend.Backend
	Require            backend.Requirements
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetime    time.Duration
	ConnMaxIdleTime    time.Duration
	StartupTimeout     time.Duration
	QueryTimeout       time.Duration
	TransactionTimeout time.Duration
	SlowThreshold      time.Duration
	Logger             *slog.Logger
	EnableTelemetry    bool
	TracerProvider     trace.TracerProvider
	MeterProvider      metric.MeterProvider
	// Zero MaxInFlight uses MaxOpenConns; MaxQueued=0 rejects rather than queues.
	MaxInFlight int
	MaxQueued   int
	Monitor     *Monitor
}

func DefaultConfig() Config {
	return Config{MaxOpenConns: 20, MaxIdleConns: 5, ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute, StartupTimeout: 5 * time.Second, QueryTimeout: 10 * time.Second,
		TransactionTimeout: 30 * time.Second, SlowThreshold: 200 * time.Millisecond,
		MaxQueued: 100}
}
func (c Config) Validate() error {
	switch {
	case c.Backend == nil:
		return invalid("backend")
	case reflect.ValueOf(c.Backend).Kind() == reflect.Pointer && reflect.ValueOf(c.Backend).IsNil():
		return invalid("backend")
	case c.MaxOpenConns <= 0:
		return invalid("max_open_conns")
	case c.MaxIdleConns < 0 || c.MaxIdleConns > c.MaxOpenConns:
		return invalid("max_idle_conns")
	case c.ConnMaxLifetime <= 0:
		return invalid("conn_max_lifetime")
	case c.ConnMaxIdleTime < 0:
		return invalid("conn_max_idle_time")
	case c.StartupTimeout <= 0:
		return invalid("startup_timeout")
	case c.QueryTimeout <= 0:
		return invalid("query_timeout")
	case c.TransactionTimeout <= 0:
		return invalid("transaction_timeout")
	case c.SlowThreshold <= 0:
		return invalid("slow_threshold")
	case c.MaxInFlight < 0 || c.MaxInFlight > 100000:
		return invalid("max_in_flight")
	case c.MaxQueued < 0 || c.MaxQueued > 100000:
		return invalid("max_queued")
	}
	if err := c.Backend.Validate(); err != nil {
		return normalize("backend_config", err)
	}
	return c.Require.Check(c.Backend.Capabilities())
}
