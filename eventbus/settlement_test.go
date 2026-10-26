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

type panickingSettlementDelivery struct {
	calls int
}

func (*panickingSettlementDelivery) Message() transport.Message {
	return transport.Message{ID: "panic"}
}
func (*panickingSettlementDelivery) DeliveryAttempt() int { return 1 }
func (*panickingSettlementDelivery) Heartbeat(context.Context) error {
	return nil
}
func (d *panickingSettlementDelivery) Settle(context.Context, transport.Decision) error {
	d.calls++
	panic("settlement")
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

func TestSettlementPanicIsNotRetriedOrCompleted(t *testing.T) {
	metrics := &settlementTestMetrics{complete: make(map[string]int)}
	cfg := DefaultConfig()
	cfg.Metrics = metrics
	e := newEngine(cfg, nil, NewCodec(nil))
	defer e.cancel()
	defer e.workerCancel()
	base := &panickingSettlementDelivery{}
	canceled := false
	delivery := &observedDelivery{Delivery: &sessionDelivery{Delivery: base, cancel: func() { canceled = true }}, ctx: context.Background()}
	e.settleDelivery(delivery, transport.Decision{Kind: transport.DecisionAck}, "task", "panic")
	if base.calls != 1 {
		t.Fatalf("settlement calls = %d, want 1", base.calls)
	}
	if metrics.complete[transport.DecisionAck.String()] != 0 {
		t.Fatal("panicking settlement was counted as complete")
	}
	if !canceled || !e.isDegraded() {
		t.Fatal("settlement panic did not cancel and degrade its session")
	}
}
