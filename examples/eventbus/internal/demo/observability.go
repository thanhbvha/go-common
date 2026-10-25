package demo

import (
	"context"
	"io"
	"sync"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonlogger "github.com/thanhbvha/go-common/logger"
)

type Logger struct {
	base   *commonlogger.Logger
	closer *commonlogger.Logger
	silent bool
}

func NewLogger(component string) *Logger {
	opts := commonlogger.DefaultOptions()
	// opts.TextFormat = true
	opts.NumWorkers = 1
	base := commonlogger.New(opts)
	commonlogger.SetDefault(base)
	return &Logger{base: base.With("component", component), closer: base}
}

func NewLoggerTo(component string, output io.Writer) *Logger {
	if output == io.Discard {
		return &Logger{silent: true}
	}
	return NewLogger(component)
}

func (l *Logger) write(level, message string, args ...any) {
	if l == nil || l.silent {
		return
	}
	switch level {
	case "WARN":
		l.base.Warn(message, args...)
	case "ERROR":
		l.base.Error(message, args...)
	default:
		l.base.Info(message, args...)
	}
}

func (l *Logger) Info(message string, args ...any)       { l.write("INFO", message, args...) }
func (l *Logger) Warn(message string, args ...any)       { l.write("WARN", message, args...) }
func (l *Logger) Error(message string, args ...any)      { l.write("ERROR", message, args...) }
func (l *Logger) InfoAsync(message string, args ...any)  { l.Info(message, args...) }
func (l *Logger) WarnAsync(message string, args ...any)  { l.Warn(message, args...) }
func (l *Logger) ErrorAsync(message string, args ...any) { l.Error(message, args...) }

func (l *Logger) Close() {
	if l != nil && l.closer != nil {
		l.closer.Close()
	}
}

type Metrics struct {
	mu       sync.Mutex
	counters map[string]int64
}

func NewMetrics() *Metrics { return &Metrics{counters: make(map[string]int64)} }

func (m *Metrics) add(name string) {
	m.mu.Lock()
	m.counters[name]++
	m.mu.Unlock()
}

func (m *Metrics) IncEnqueue(jobType, status string) { m.add("enqueue." + jobType + "." + status) }
func (m *Metrics) IncDequeue(jobType string)         { m.add("dequeue." + jobType) }
func (m *Metrics) IncComplete(jobType, status string) {
	m.add("complete." + jobType + "." + status)
}
func (m *Metrics) ObserveDuration(string, float64) {}
func (m *Metrics) IncHandlerFailure(jobType, outcome string) {
	m.add("handler_failure." + jobType + "." + outcome)
}
func (m *Metrics) ObserveSettlement(jobType, decision, outcome string, _ float64) {
	m.add("settlement." + jobType + "." + decision + "." + outcome)
}
func (m *Metrics) IncReconnect(jobType string)                            { m.add("reconnect." + jobType) }
func (m *Metrics) ObserveSaturation(string, float64)                      {}
func (m *Metrics) ObserveQueueAge(string, float64)                        {}
func (m *Metrics) ObserveStats(jobType string, _ transport.TopologyStats) { m.add("stats." + jobType) }

func (m *Metrics) Snapshot() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := make(map[string]int64, len(m.counters))
	for key, value := range m.counters {
		copy[key] = value
	}
	return copy
}

type Tracer struct {
	logger *Logger
}

func NewTracer(logger *Logger) *Tracer { return &Tracer{logger: logger} }

func (t *Tracer) Start(ctx context.Context, operation string, _ eventbus.Headers) (context.Context, func()) {
	t.logger.Info("trace started", "operation", operation)
	return ctx, func() { t.logger.Info("trace finished", "operation", operation) }
}

func (t *Tracer) Inject(_ context.Context, headers eventbus.Headers) {
	if _, exists := headers["traceparent"]; !exists {
		headers["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	}
}

func (t *Tracer) RecordEvent(_ context.Context, name string, attributes map[string]string) {
	t.logger.Info("trace event", "name", name, "attributes", attributes)
}
