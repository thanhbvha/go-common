package eventbus

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type settlementTestDelivery struct {
	guard    transport.SettlementGuard
	mu       sync.Mutex
	failures int
	calls    int
}

func (*settlementTestDelivery) Message() transport.Message { return transport.Message{ID: "job-id"} }
func (*settlementTestDelivery) DeliveryAttempt() int       { return 1 }
func (*settlementTestDelivery) Heartbeat(context.Context) error {
	return nil
}
func (d *settlementTestDelivery) Settle(_ context.Context, decision transport.Decision) error {
	return d.guard.Settle(decision, func() error {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.calls++
		if d.failures > 0 {
			d.failures--
			return errors.New("settlement failed")
		}
		return nil
	})
}

type settlementTestMetrics struct {
	mu       sync.Mutex
	complete map[string]int
}

func (*settlementTestMetrics) IncEnqueue(string, string)       {}
func (*settlementTestMetrics) IncDequeue(string)               {}
func (*settlementTestMetrics) ObserveDuration(string, float64) {}
func (m *settlementTestMetrics) IncComplete(_ string, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.complete[status]++
}

type settlementTestLogger struct {
	noopLogger
	mu     sync.Mutex
	errors int
}

func (l *settlementTestLogger) ErrorAsync(string, ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors++
}

func TestEngineRecordsMetricsOnlyAfterSettlementSuccess(t *testing.T) {
	metrics := &settlementTestMetrics{complete: make(map[string]int)}
	logger := &settlementTestLogger{}
	cfg := DefaultConfig()
	cfg.Metrics = metrics
	cfg.Logger = logger
	eng := newEngine(cfg, nil, NewCodec(nil))
	delivery := &settlementTestDelivery{failures: 1}
	decision := transport.Decision{Kind: transport.DecisionRetry}

	eng.settleDelivery(delivery, decision, "task", "job-id")
	if got := metrics.complete["retry"]; got != 0 {
		t.Fatalf("retry success metrics after failed settlement = %d, want 0", got)
	}
	if logger.errors != 1 {
		t.Fatalf("settlement error logs = %d, want 1", logger.errors)
	}

	eng.settleDelivery(delivery, decision, "task", "job-id")
	if got := metrics.complete["retry"]; got != 1 {
		t.Fatalf("retry success metrics after confirmed settlement = %d, want 1", got)
	}
	if delivery.calls != 2 {
		t.Fatalf("settlement attempts = %d, want 2", delivery.calls)
	}
}
