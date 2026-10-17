package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

type attemptDelivery struct {
	message       transport.Message
	deliveryCount int
	acks          int
	retries       []transport.Message
	deadLetters   []transport.DeadLetter
	settled       transport.SettlementGuard
}

func (d *attemptDelivery) Message() transport.Message { return d.message }
func (d *attemptDelivery) DeliveryAttempt() int       { return d.deliveryCount }
func (d *attemptDelivery) Heartbeat(context.Context) error {
	return nil
}
func (d *attemptDelivery) Settle(_ context.Context, decision transport.Decision) error {
	return d.settled.Settle(decision, func() error {
		switch decision.Kind {
		case transport.DecisionAck:
			d.acks++
		case transport.DecisionRetry:
			d.retries = append(d.retries, decision.Next)
		case transport.DecisionDeadLetter:
			lastError := ""
			if decision.Cause != nil {
				lastError = decision.Cause.Error()
			}
			d.deadLetters = append(d.deadLetters, transport.DeadLetter{
				Message: d.message, Reason: decision.Reason, LastError: lastError,
				Attempts: decision.Attempts, FailedAt: decision.FailedAt,
			})
		}
		return nil
	})
}

func TestProcessDeliveryAttemptBounds(t *testing.T) {
	tests := []struct {
		name             string
		envelopeAttempt  int
		deliveryCount    int
		maxAttempts      int
		wantHandlerCalls int
		wantJobAttempt   int
		wantRetries      int
		wantNextAttempt  int
		wantDLQReason    string
	}{
		{name: "retry increments before scheduling", envelopeAttempt: 1, deliveryCount: 1, maxAttempts: 2, wantHandlerCalls: 1, wantJobAttempt: 1, wantRetries: 1, wantNextAttempt: 2},
		{name: "final failure goes directly to DLQ", envelopeAttempt: 2, deliveryCount: 1, maxAttempts: 2, wantHandlerCalls: 1, wantJobAttempt: 2, wantDLQReason: "max_attempts_exceeded"},
		{name: "single allowed execution goes directly to DLQ", envelopeAttempt: 1, deliveryCount: 1, maxAttempts: 1, wantHandlerCalls: 1, wantJobAttempt: 1, wantDLQReason: "max_attempts_exceeded"},
		{name: "exhausted envelope never invokes handler", envelopeAttempt: 3, deliveryCount: 1, maxAttempts: 2, wantHandlerCalls: 0, wantDLQReason: "poison_message"},
		{name: "broker redelivery advances effective attempt", envelopeAttempt: 1, deliveryCount: 2, maxAttempts: 2, wantHandlerCalls: 1, wantJobAttempt: 2, wantDLQReason: "max_attempts_exceeded"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ProcessTimeout = time.Hour
			cfg.RetryPolicy = ConstantBackoff{}
			eng := newEngine(cfg, nil, NewCodec(nil))

			payload, err := json.Marshal(Envelope{
				V:           3,
				ID:          "job-id",
				Kind:        TaskTopology,
				Type:        "task",
				Payload:     json.RawMessage(`{"value":1}`),
				Attempt:     tt.envelopeAttempt,
				MaxAttempts: tt.maxAttempts,
			})
			if err != nil {
				t.Fatalf("marshal envelope: %v", err)
			}
			delivery := &attemptDelivery{
				message:       transport.Message{ID: "job-id", Type: "task", Payload: payload},
				deliveryCount: tt.deliveryCount,
			}

			handlerCalls := 0
			observedAttempt := 0
			eng.processDelivery(TaskTopology, "task", "", func(_ context.Context, job Job) error {
				handlerCalls++
				observedAttempt = job.Attempt()
				return errors.New("temporary failure")
			}, delivery, tt.maxAttempts)

			if handlerCalls != tt.wantHandlerCalls {
				t.Fatalf("handler calls = %d, want %d", handlerCalls, tt.wantHandlerCalls)
			}
			if handlerCalls > 0 && observedAttempt != tt.wantJobAttempt {
				t.Fatalf("job attempt = %d, want %d", observedAttempt, tt.wantJobAttempt)
			}
			if len(delivery.retries) != tt.wantRetries {
				t.Fatalf("retries = %d, want %d", len(delivery.retries), tt.wantRetries)
			}
			if tt.wantNextAttempt > 0 {
				next, err := NewCodec(nil).Decode(delivery.retries[0].Payload)
				if err != nil {
					t.Fatalf("decode retry: %v", err)
				}
				if next.Attempt != tt.wantNextAttempt {
					t.Fatalf("next attempt = %d, want %d", next.Attempt, tt.wantNextAttempt)
				}
			}
			if tt.wantDLQReason == "" {
				if len(delivery.deadLetters) != 0 {
					t.Fatalf("unexpected DLQ records: %d", len(delivery.deadLetters))
				}
			} else if len(delivery.deadLetters) != 1 || delivery.deadLetters[0].Reason != tt.wantDLQReason {
				t.Fatalf("DLQ = %+v, want reason %q", delivery.deadLetters, tt.wantDLQReason)
			}
		})
	}
}
