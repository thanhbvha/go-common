// Package otel exports bounded transactional messaging metrics. The application
// owns the meter provider/exporter and schedules Store.Stats sampling as needed.
package otel

import (
	"context"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type Observer struct {
	operations              metric.Int64Counter
	duration                metric.Float64Histogram
	pending, inFlight, dead metric.Int64Gauge
	age                     metric.Float64Gauge
}

func New(meter metric.Meter) (*Observer, error) {
	if meter == nil {
		return nil, messaging.ErrInvalidConfig
	}
	o := new(Observer)
	var err error
	if o.operations, err = meter.Int64Counter("messaging.operations"); err != nil {
		return nil, err
	}
	if o.duration, err = meter.Float64Histogram("messaging.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if o.pending, err = meter.Int64Gauge("messaging.outbox.pending"); err != nil {
		return nil, err
	}
	if o.inFlight, err = meter.Int64Gauge("messaging.outbox.in_flight"); err != nil {
		return nil, err
	}
	if o.dead, err = meter.Int64Gauge("messaging.outbox.dead"); err != nil {
		return nil, err
	}
	if o.age, err = meter.Float64Gauge("messaging.outbox.oldest_pending_age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	return o, nil
}
func (o *Observer) Observe(ctx context.Context, e messaging.Observation) {
	if e.Stats != nil {
		o.pending.Record(ctx, e.Stats.Pending)
		o.inFlight.Record(ctx, e.Stats.InFlight)
		o.dead.Record(ctx, e.Stats.Dead)
		o.age.Record(ctx, e.Stats.OldestPendingAge.Seconds())
		return
	}
	// Reject arbitrary caller-supplied label values as well as payload metadata.
	if e.Operation != "claim" && e.Operation != "publish" && e.Operation != "settle" && e.Operation != "inbox" {
		return
	}
	switch e.Outcome {
	case "error", "published", "retry", "dead", "lease_lost", "applied", "duplicate", "conflict":
	default:
		return
	}
	attrs := metric.WithAttributes(attribute.String("operation", e.Operation), attribute.String("outcome", e.Outcome))
	o.operations.Add(ctx, 1, attrs)
	if e.Duration > 0 {
		o.duration.Record(ctx, e.Duration.Seconds(), attrs)
	}
}

var _ messaging.Observer = (*Observer)(nil)
