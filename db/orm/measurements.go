package orm

import (
	"context"
	"crypto/rand"
	"database/sql"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type EventKind string

const (
	EventAdmission   EventKind = "admission"
	EventTransaction EventKind = "transaction"
)

// Event contains fixed operation metadata only; no identifiers or raw errors.
type Event struct {
	Kind         EventKind
	Phase        TransactionPhase
	Outcome      string
	Duration     time.Duration
	WaitDuration time.Duration
}

// Monitor callbacks are synchronous, fast and concurrency-safe. Do not invoke
// client/manager lifecycle methods from them. Panics are isolated from DB work.
type Monitor struct{ Event func(Event) }
type TransactionStats struct {
	Started    uint64
	Committed  uint64
	RolledBack uint64
	NotStarted uint64
	Unknown    uint64
	Failed     uint64
	Active     int
	Duration   time.Duration
}

func (c *Client) TransactionStats() TransactionStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.transactions
	s.Active = c.active
	return s
}

type measurements struct {
	wait         metric.Float64Histogram
	duration     metric.Float64Histogram
	transactions metric.Int64Counter
	rejected     metric.Int64Counter
	registration metric.Registration
	client       attribute.KeyValue
}

func newMeasurements(provider metric.MeterProvider, pool *sql.DB) (*measurements, error) {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}
	meter := provider.Meter("github.com/thanhbvha/go-common/db/orm")
	m := &measurements{}
	m.client = attribute.String("orm.client.id", rand.Text())
	var err error
	// Explicit seconds-scale buckets retain useful latency resolution; SDK views
	// may override this advisory aggregation for a deployment's own SLOs.
	buckets := metric.WithExplicitBucketBoundaries(.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60)
	m.wait, err = meter.Float64Histogram("orm.admission.wait", metric.WithUnit("s"), buckets)
	if err != nil {
		return nil, err
	}
	m.duration, err = meter.Float64Histogram("orm.transaction.duration", metric.WithUnit("s"), buckets)
	if err != nil {
		return nil, err
	}
	m.transactions, err = meter.Int64Counter("orm.transactions")
	if err != nil {
		return nil, err
	}
	m.rejected, err = meter.Int64Counter("orm.admission.rejected")
	if err != nil {
		return nil, err
	}
	connections, err := meter.Int64ObservableGauge("orm.pool.connections")
	if err != nil {
		return nil, err
	}
	waits, err := meter.Int64ObservableCounter("orm.pool.waits")
	if err != nil {
		return nil, err
	}
	waitTime, err := meter.Float64ObservableCounter("orm.pool.wait_time", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	m.registration, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		s := pool.Stats()
		observer.ObserveInt64(connections, int64(s.InUse), metric.WithAttributes(m.client, attribute.String("state", "in_use")))
		observer.ObserveInt64(connections, int64(s.Idle), metric.WithAttributes(m.client, attribute.String("state", "idle")))
		observer.ObserveInt64(waits, s.WaitCount, metric.WithAttributes(m.client))
		observer.ObserveFloat64(waitTime, s.WaitDuration.Seconds(), metric.WithAttributes(m.client))
		return nil
	}, connections, waits, waitTime)
	if err != nil {
		return nil, err
	}
	return m, nil
}
func (m *measurements) close() error {
	if m.registration == nil {
		return nil
	}
	return m.registration.Unregister()
}
func (c *Client) observe(ctx context.Context, event Event) {
	if m := c.measurements; m != nil {
		attrs := metric.WithAttributes(m.client, attribute.String("outcome", event.Outcome))
		switch event.Kind {
		case EventAdmission:
			if event.Outcome != "rejected" {
				m.wait.Record(ctx, event.WaitDuration.Seconds(), attrs)
			}
			if event.Outcome == "rejected" {
				m.rejected.Add(ctx, 1, metric.WithAttributes(m.client))
			}
		case EventTransaction:
			m.duration.Record(ctx, event.Duration.Seconds(), attrs)
			m.transactions.Add(ctx, 1, attrs, metric.WithAttributes(attribute.String("phase", string(event.Phase))))
		}
	}
	if c.cfg.Monitor != nil && c.cfg.Monitor.Event != nil {
		func() { defer func() { _ = recover() }(); c.cfg.Monitor.Event(event) }()
	}
}
