package eventbus

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

type timeoutOutcomeMetrics struct {
	noopMetrics
	mu       sync.Mutex
	failures map[string]int
}

func (m *timeoutOutcomeMetrics) IncHandlerFailure(_, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failures[outcome]++
}
func (*timeoutOutcomeMetrics) ObserveSettlement(string, string, string, float64) {}
func (*timeoutOutcomeMetrics) IncReconnect(string)                               {}
func (*timeoutOutcomeMetrics) ObserveSaturation(string, float64)                 {}
func (*timeoutOutcomeMetrics) ObserveQueueAge(string, float64)                   {}
func (*timeoutOutcomeMetrics) ObserveStats(string, transport.TopologyStats)      {}

func TestProcessDeliveryHandlerResultAfterDeadline(t *testing.T) {
	type want struct {
		acks, retries, dlq int
		dlqReason          string
		timeoutMetric      int
	}
	tests := []struct {
		name    string
		waitCtx bool // handler blocks until procCtx is done before returning
		result  func(ctx context.Context) error
		want    want
		// needsA marks cases that require decision A (docs/refactor-plan.md
		// 0.5'.1). They contradict contracttest handler_timeout_retries_then_dlq,
		// so they stay skipped until that contract change is approved.
		needsA bool
	}{
		{
			name:   "control: nil before deadline acks",
			result: func(context.Context) error { return nil },
			want:   want{acks: 1},
		},
		{
			name:    "nil after deadline keeps success",
			waitCtx: true,
			result:  func(context.Context) error { return nil },
			want:    want{acks: 1},
			needsA:  true,
		},
		{
			name:    "skip after deadline keeps ack",
			waitCtx: true,
			result:  func(context.Context) error { return MarkSkip("done") },
			want:    want{acks: 1},
			needsA:  true,
		},
		{
			name:    "permanent after deadline goes to DLQ",
			waitCtx: true,
			result:  func(context.Context) error { return MarkPermanent(errors.New("bad input")) },
			want:    want{dlq: 1, dlqReason: "permanent_error"},
			needsA:  true,
		},
		{
			name:    "retry-after after deadline keeps handler's choice",
			waitCtx: true,
			result:  func(context.Context) error { return MarkRetryAfter(errors.New("later"), time.Second) },
			want:    want{retries: 1},
			needsA:  true,
		},
		{
			name:    "ctx error after deadline retries and counts timeout",
			waitCtx: true,
			result:  func(ctx context.Context) error { return ctx.Err() },
			want:    want{retries: 1, timeoutMetric: 1},
		},
		{
			name:    "plain error after deadline retries and counts timeout",
			waitCtx: true,
			result:  func(context.Context) error { return errors.New("boom") },
			want:    want{retries: 1, timeoutMetric: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.needsA && os.Getenv("EVENTBUS_H1_STRICT") == "" {
				t.Skip("pending decision A: conflicts with contracttest handler_timeout_retries_then_dlq")
			}
			metrics := &timeoutOutcomeMetrics{failures: make(map[string]int)}
			cfg := DefaultConfig()
			cfg.ProcessTimeout = 50 * time.Millisecond
			cfg.RetryPolicy = ConstantBackoff{}
			cfg.Metrics = metrics
			eng := newEngine(cfg, nil, NewCodec(nil))
			defer eng.cancel()
			defer eng.workerCancel()

			payload, err := json.Marshal(Envelope{
				V: 3, ID: "job-id", Kind: TaskTopology, Type: "task",
				Payload: json.RawMessage(`{"value":1}`), Attempt: 1, MaxAttempts: 3,
				CreatedAt: time.Now().UnixMilli(),
			})
			if err != nil {
				t.Fatalf("marshal envelope: %v", err)
			}
			delivery := &attemptDelivery{
				message:       transport.Message{ID: "job-id", Type: "task", Payload: payload},
				deliveryCount: 1,
			}

			eng.processDelivery(TaskTopology, "task", "", func(ctx context.Context, _ Job) error {
				if tt.waitCtx {
					<-ctx.Done()
				}
				return tt.result(ctx)
			}, delivery, 0)

			got := want{acks: delivery.acks, retries: len(delivery.retries), dlq: len(delivery.deadLetters)}
			if len(delivery.deadLetters) > 0 {
				got.dlqReason = delivery.deadLetters[0].Reason
			}
			metrics.mu.Lock()
			got.timeoutMetric = metrics.failures["timeout"]
			metrics.mu.Unlock()

			if got == tt.want {
				return
			}
			t.Fatalf("outcome = %+v, want %+v", got, tt.want)
		})
	}
}
