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
		recorder.RecordEvent(ctx, name, map[string]string{"outcome": outcome})
	}
}

func (e *engine) traceSettlement(ctx context.Context, decision, outcome string) {
	if recorder, ok := e.cfg.Tracer.(TraceRecorder); ok {
		recorder.RecordEvent(ctx, "settlement", map[string]string{"decision": decision, "outcome": outcome})
	}
}

func (e *engine) traceEvent(ctx context.Context, name string, attributes map[string]string) {
	if recorder, ok := e.cfg.Tracer.(TraceRecorder); ok {
		recorder.RecordEvent(ctx, name, attributes)
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

func (d *observedDelivery) Message() transport.Message {
	msg := d.message
	msg.Headers = cloneHeaders(msg.Headers)
	return msg
}

// Unknown broker-wide age/lag must not be replaced with engine-local samples.
func (e *engine) observeQueueAge(name string, env *Envelope) {
	readyAt := env.CreatedAt
	if env.RunAt > readyAt {
		readyAt = env.RunAt
	}
	if readyAt > 0 {
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			metrics.ObserveQueueAge(name, max(0, time.Since(time.UnixMilli(readyAt)).Seconds()))
		}
	}
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
