package eventbus

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Logger is the standard logging interface required by the Event Bus.
// It matches the shape of go-common/logger.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	InfoAsync(msg string, args ...any)
	WarnAsync(msg string, args ...any)
	ErrorAsync(msg string, args ...any)
}

// Tracer defines the interface for tracing job execution.
type Tracer interface {
	// Start starts a new span. Returns context with span and a function to end it.
	Start(ctx context.Context, operationName string, carrier Headers) (context.Context, func())
}

// Metrics defines the interface for collecting operational metrics.
type Metrics interface {
	IncEnqueue(jobType string, status string) // status: "success", "error", "duplicate"
	IncDequeue(jobType string)
	IncComplete(jobType string, status string) // status: "ack", "retry", "dlq"
	ObserveDuration(jobType string, d float64)
}

// RuntimeMetrics is an optional extension to Metrics. Implementations must be
// concurrency-safe and nonblocking. Labels are registered topology/subscriber
// names and fixed outcomes; payloads, IDs and headers are never metric labels.
type RuntimeMetrics interface {
	IncHandlerFailure(jobType, outcome string)                            // timeout, panic
	ObserveSettlement(jobType, decision, outcome string, seconds float64) // success, error
	IncReconnect(jobType string)
	ObserveSaturation(jobType string, seconds float64)
	ObserveQueueAge(jobType string, seconds float64) // current delivery age since ready time
	ObserveStats(jobType string, stats transport.TopologyStats)
}

// TraceInjector optionally injects the current producer span into carrier.
// It must write only propagation headers, never credentials or payload data.
type TraceInjector interface {
	Inject(context.Context, Headers)
}

// TraceRecorder optionally records fixed, sanitized span events. Values passed
// by the core contain no original handler errors, panic values or payloads.
type TraceRecorder interface {
	RecordEvent(ctx context.Context, name string, attributes map[string]string)
}

func cloneHeaders(headers Headers) Headers {
	if headers == nil {
		return nil
	}
	copy := make(Headers, len(headers))
	for k, v := range headers {
		copy[k] = v
	}
	return copy
}

func (e *engine) traceOutcome(ctx context.Context, name, outcome string) {
	if recorder, ok := e.cfg.Tracer.(TraceRecorder); ok {
		safeTraceRecord(recorder, ctx, name, map[string]string{"outcome": outcome})
	}
}

func (e *engine) traceSettlement(ctx context.Context, decision, outcome string) {
	if recorder, ok := e.cfg.Tracer.(TraceRecorder); ok {
		safeTraceRecord(recorder, ctx, "settlement", map[string]string{"decision": decision, "outcome": outcome})
	}
}

func metricsName(name, subscriber string) string {
	if subscriber != "" {
		return name + "_" + subscriber
	}
	return name
}

// Attach propagation metadata without mutating driver-owned delivery state.
// The original envelope remains the durable source of headers for DLQ storage.
type observedDelivery struct {
	transport.Delivery
	message transport.Message
	ctx     context.Context
}

type sessionDelivery struct {
	transport.Delivery
	cancel context.CancelFunc
}

type sessionLeaseDelivery struct {
	*sessionDelivery
	lease transport.LeaseDelivery
}

func (d *sessionLeaseDelivery) HeartbeatInterval() time.Duration {
	return d.lease.HeartbeatInterval()
}

func withSessionCancel(delivery transport.Delivery, cancel context.CancelFunc) transport.Delivery {
	wrapped := &sessionDelivery{Delivery: delivery, cancel: cancel}
	if lease, ok := delivery.(transport.LeaseDelivery); ok {
		return &sessionLeaseDelivery{sessionDelivery: wrapped, lease: lease}
	}
	return wrapped
}

func (d *sessionDelivery) cancelSession() {
	if d.cancel != nil {
		d.cancel()
	}
}

type sessionCanceler interface {
	cancelSession()
}

func (d *observedDelivery) Message() transport.Message {
	msg := d.message
	msg.Headers = cloneHeaders(msg.Headers)
	return msg
}

func (d *observedDelivery) cancelSession() {
	if canceler, ok := d.Delivery.(sessionCanceler); ok {
		canceler.cancelSession()
	}
}

// Unknown broker-wide age/lag must not be replaced with engine-local samples.
func (e *engine) observeQueueAge(name string, env *Envelope) {
	readyAt := env.CreatedAt
	if env.RunAt > readyAt {
		readyAt = env.RunAt
	}
	if readyAt > 0 {
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			safeRuntimeMetric(func() { metrics.ObserveQueueAge(name, max(0, time.Since(time.UnixMilli(readyAt)).Seconds())) })
		}
	}
}

func safeMetric(call func()) {
	guardCall(panicScopeMetrics, "metrics hook", nil, call)
}

func safeRuntimeMetric(call func()) {
	guardCall(panicScopeMetrics, "runtime metrics hook", nil, call)
}

func safeTraceStart(tracer Tracer, ctx context.Context, operation string, carrier Headers) (context.Context, func()) {
	if tracer == nil {
		return ctx, noopRelease
	}
	type result struct {
		ctx context.Context
		end func()
	}
	started, err := guardValue(panicScopeTracer, "trace start", nil, func() (result, error) {
		spanCtx, end := tracer.Start(ctx, operation, carrier)
		return result{ctx: spanCtx, end: end}, nil
	})
	if err != nil || started.ctx == nil {
		return ctx, noopRelease
	}
	if started.end == nil {
		started.end = noopRelease
	}
	return started.ctx, func() {
		guardCall(panicScopeTracer, "trace end", nil, started.end)
	}
}

func safeTraceInject(injector TraceInjector, ctx context.Context, carrier Headers) {
	guardCall(panicScopeTracer, "trace inject", nil, func() { injector.Inject(ctx, carrier) })
}

func safeTraceRecord(recorder TraceRecorder, ctx context.Context, name string, attributes map[string]string) {
	guardCall(panicScopeTracer, "trace record", nil, func() { recorder.RecordEvent(ctx, name, attributes) })
}

type safeLogger struct{ base Logger }

func wrapSafeLogger(logger Logger) Logger {
	if logger == nil {
		return noopLogger{}
	}
	if _, ok := logger.(safeLogger); ok {
		return logger
	}
	return safeLogger{base: logger}
}

func (l safeLogger) Info(msg string, args ...any) {
	guardCall(panicScopeLogger, "logger info", nil, func() { l.base.Info(msg, args...) })
}
func (l safeLogger) Warn(msg string, args ...any) {
	guardCall(panicScopeLogger, "logger warn", nil, func() { l.base.Warn(msg, args...) })
}
func (l safeLogger) Error(msg string, args ...any) {
	guardCall(panicScopeLogger, "logger error", nil, func() { l.base.Error(msg, args...) })
}
func (l safeLogger) InfoAsync(msg string, args ...any) {
	guardCall(panicScopeLogger, "logger info async", nil, func() { l.base.InfoAsync(msg, args...) })
}
func (l safeLogger) WarnAsync(msg string, args ...any) {
	guardCall(panicScopeLogger, "logger warn async", nil, func() { l.base.WarnAsync(msg, args...) })
}
func (l safeLogger) ErrorAsync(msg string, args ...any) {
	guardCall(panicScopeLogger, "logger error async", nil, func() { l.base.ErrorAsync(msg, args...) })
}

// noop components
type noopLogger struct{}

func (noopLogger) Info(string, ...any)       {}
func (noopLogger) Warn(string, ...any)       {}
func (noopLogger) Error(string, ...any)      {}
func (noopLogger) InfoAsync(string, ...any)  {}
func (noopLogger) WarnAsync(string, ...any)  {}
func (noopLogger) ErrorAsync(string, ...any) {}

type noopMetrics struct{}

func (noopMetrics) IncEnqueue(string, string)       {}
func (noopMetrics) IncDequeue(string)               {}
func (noopMetrics) IncComplete(string, string)      {}
func (noopMetrics) ObserveDuration(string, float64) {}

type noopTracer struct{}

func (noopTracer) Start(ctx context.Context, _ string, _ Headers) (context.Context, func()) {
	return ctx, func() {}
}
