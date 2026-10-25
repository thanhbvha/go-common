package demo

import (
	"context"
	"fmt"
	"sort"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type OTelMetrics struct {
	publish         metric.Int64Counter
	delivery        metric.Int64Counter
	completion      metric.Int64Counter
	handlerDuration metric.Float64Histogram
	handlerFailure  metric.Int64Counter
	settlement      metric.Float64Histogram
	reconnect       metric.Int64Counter
	saturation      metric.Float64Histogram
	queueAge        metric.Float64Histogram
	stats           metric.Int64Gauge
	statsOldestAge  metric.Float64Gauge
}

func NewOTelMetrics() (*OTelMetrics, error) {
	meter := otel.Meter("github.com/thanhbvha/go-common/eventbus")
	m := &OTelMetrics{}
	var err error
	if m.publish, err = meter.Int64Counter("eventbus.publish"); err != nil {
		return nil, err
	}
	if m.delivery, err = meter.Int64Counter("eventbus.delivery"); err != nil {
		return nil, err
	}
	if m.completion, err = meter.Int64Counter("eventbus.completion"); err != nil {
		return nil, err
	}
	if m.handlerDuration, err = meter.Float64Histogram("eventbus.handler.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.handlerFailure, err = meter.Int64Counter("eventbus.handler.failure"); err != nil {
		return nil, err
	}
	if m.settlement, err = meter.Float64Histogram("eventbus.settlement.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.reconnect, err = meter.Int64Counter("eventbus.reconnect"); err != nil {
		return nil, err
	}
	if m.saturation, err = meter.Float64Histogram("eventbus.saturation.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.queueAge, err = meter.Float64Histogram("eventbus.queue.age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if m.stats, err = meter.Int64Gauge("eventbus.topology.messages"); err != nil {
		return nil, err
	}
	if m.statsOldestAge, err = meter.Float64Gauge("eventbus.topology.oldest.age", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	return m, nil
}

func topologyAttr(name string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("eventbus.topology", name))
}

func (m *OTelMetrics) IncEnqueue(name, status string) {
	m.publish.Add(context.Background(), 1, topologyAttr(name), metric.WithAttributes(attribute.String("eventbus.outcome", status)))
}
func (m *OTelMetrics) IncDequeue(name string) {
	m.delivery.Add(context.Background(), 1, topologyAttr(name))
}
func (m *OTelMetrics) IncComplete(name, status string) {
	m.completion.Add(context.Background(), 1, topologyAttr(name), metric.WithAttributes(attribute.String("eventbus.decision", status)))
}
func (m *OTelMetrics) ObserveDuration(name string, seconds float64) {
	m.handlerDuration.Record(context.Background(), seconds, topologyAttr(name))
}
func (m *OTelMetrics) IncHandlerFailure(name, outcome string) {
	m.handlerFailure.Add(context.Background(), 1, topologyAttr(name), metric.WithAttributes(attribute.String("eventbus.outcome", outcome)))
}
func (m *OTelMetrics) ObserveSettlement(name, decision, outcome string, seconds float64) {
	m.settlement.Record(context.Background(), seconds, topologyAttr(name), metric.WithAttributes(
		attribute.String("eventbus.decision", decision), attribute.String("eventbus.outcome", outcome),
	))
}
func (m *OTelMetrics) IncReconnect(name string) {
	m.reconnect.Add(context.Background(), 1, topologyAttr(name))
}
func (m *OTelMetrics) ObserveSaturation(name string, seconds float64) {
	m.saturation.Record(context.Background(), seconds, topologyAttr(name))
}
func (m *OTelMetrics) ObserveQueueAge(name string, seconds float64) {
	m.queueAge.Record(context.Background(), seconds, topologyAttr(name))
}
func (m *OTelMetrics) ObserveStats(name string, stats transport.TopologyStats) {
	values := map[string]int64{
		"pending": stats.Pending, "in_flight": stats.InFlight, "delayed": stats.Delayed, "dlq": stats.DLQ,
	}
	if stats.LagKnown {
		values["lag"] = stats.Lag
	}
	for state, value := range values {
		m.stats.Record(context.Background(), value, topologyAttr(name), metric.WithAttributes(attribute.String("eventbus.state", state)))
	}
	if stats.OldestAgeKnown {
		m.statsOldestAge.Record(context.Background(), stats.OldestAge.Seconds(), topologyAttr(name))
	}
}

type OTelTracer struct{ tracer trace.Tracer }

func NewOTelTracer() *OTelTracer {
	return &OTelTracer{tracer: otel.Tracer("github.com/thanhbvha/go-common/eventbus")}
}

func (t *OTelTracer) Start(ctx context.Context, operation string, carrier eventbus.Headers) (context.Context, func()) {
	if len(carrier) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
	}
	kind := trace.SpanKindInternal
	if operation == "eventbus.publish" {
		kind = trace.SpanKindProducer
	}
	if operation == "eventbus.process" {
		kind = trace.SpanKindConsumer
	}
	ctx, span := t.tracer.Start(ctx, operation, trace.WithSpanKind(kind))
	return ctx, func() { span.End() }
}

func (*OTelTracer) Inject(ctx context.Context, carrier eventbus.Headers) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(carrier))
}

func (*OTelTracer) RecordEvent(ctx context.Context, name string, values map[string]string) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		attrs = append(attrs, attribute.String("eventbus."+key, values[key]))
	}
	trace.SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(attrs...))
}

var (
	_ eventbus.Metrics        = (*OTelMetrics)(nil)
	_ eventbus.RuntimeMetrics = (*OTelMetrics)(nil)
	_ eventbus.Tracer         = (*OTelTracer)(nil)
	_ eventbus.TraceInjector  = (*OTelTracer)(nil)
	_ eventbus.TraceRecorder  = (*OTelTracer)(nil)
)

func OTelMetricNames() []string {
	return []string{
		"eventbus.publish", "eventbus.delivery", "eventbus.completion",
		"eventbus.handler.duration", "eventbus.handler.failure", "eventbus.settlement.duration",
		"eventbus.reconnect", "eventbus.saturation.duration", "eventbus.queue.age",
		"eventbus.topology.messages", "eventbus.topology.oldest.age",
	}
}

func ValidateOTelMetricNames() error {
	seen := make(map[string]struct{})
	for _, name := range OTelMetricNames() {
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate metric name %q", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}
