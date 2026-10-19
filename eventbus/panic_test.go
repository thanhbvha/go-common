package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type panicLogger struct {
	noopLogger
	entries []string
	stacks  []string
}

func (l *panicLogger) ErrorAsync(msg string, args ...any) {
	l.entries = append(l.entries, fmt.Sprint(append([]any{msg}, args...)...))
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "stack" {
			l.stacks = append(l.stacks, args[i+1].(string))
		}
	}
}

type unformattablePanic struct{}

func (unformattablePanic) Error() string { panic("must not format panic value") }

// DS-35/20/24: panics use normal retry and final-attempt DLQ classification,
// regardless of their value. Diagnostic stacks never enter broker metadata.
func TestPanicRecoveryDefaultsAndAttemptBounds(t *testing.T) {
	for _, value := range []struct {
		name  string
		value any
	}{
		{"string", strings.Repeat("synthetic-sensitive-value", 10000)},
		{"error", errors.New("synthetic-sensitive-value")},
		{"nil", nil},
		{"unformattable", unformattablePanic{}},
		{"permanent_error_value", MarkPermanent(errors.New("synthetic-sensitive-value"))},
	} {
		for _, max := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/max_%d", value.name, max), func(t *testing.T) {
				logger := &panicLogger{}
				cfg := DefaultConfig()
				cfg.Logger = logger
				cfg.RetryPolicy = ConstantBackoff{}
				e := newEngine(cfg, nil, NewCodec(nil))
				defer e.cancel()
				defer e.workerCancel()
				payload, _, err := e.codec.EncodeTopology(TaskTopology, "panic", 1, max)
				if err != nil {
					t.Fatal(err)
				}
				d := &attemptDelivery{message: transport.Message{Payload: payload}, deliveryCount: 1}
				e.processDelivery(TaskTopology, "panic", "", func(context.Context, Job) error { panic(value.value) }, d, 0)
				if d.acks != 0 {
					t.Fatal("panic was ACKed")
				}
				if max == 2 {
					if len(d.retries) != 1 || len(d.deadLetters) != 0 {
						t.Fatal("panic did not request retry")
					}
					env, err := e.codec.Decode(d.retries[0].Payload)
					if err != nil || env.Attempt != 2 {
						t.Fatalf("retry envelope = %+v, %v", env, err)
					}
				} else {
					if len(d.deadLetters) != 1 || len(d.retries) != 0 {
						t.Fatal("final panic did not request DLQ")
					}
					record := d.deadLetters[0]
					if record.Reason != "max_attempts_exceeded" || record.LastError != "handler_panic" {
						t.Fatalf("DLQ reason/error = %q/%q", record.Reason, record.LastError)
					}
				}
				if len(logger.stacks) != 1 || len(logger.stacks[0]) > 8*1024 || !strings.Contains(logger.stacks[0], "panic_test.go:") {
					t.Fatal("missing bounded source stack")
				}
				if strings.Contains(strings.Join(logger.entries, "\n"), "synthetic-sensitive-value") {
					t.Fatal("panic value exposed in logs")
				}
			})
		}
	}
}

// DS-35/74: recovery must keep the fixed worker alive, for tasks and event
// subscribers alike, and allow graceful shutdown after a middleware panic.
func TestPanicRecoveryKeepsWorkerAlive(t *testing.T) {
	for _, kind := range []TopologyKind{TaskTopology, EventTopology} {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.Concurrency = 1
				codec := NewCodec(nil)
				var deliveries []transport.Delivery
				for range 2 {
					payload, _, err := codec.EncodeTopology(kind, "panic", 1, 1)
					if err != nil {
						t.Fatal(err)
					}
					deliveries = append(deliveries, &workerPoolDelivery{message: transport.Message{Payload: payload}, settled: make(chan struct{})})
				}
				backend := &workerPoolTransport{deliveries: deliveries, started: make(chan struct{}), accepted: make(chan int, 2)}
				b, err := Open(cfg, WithTransport(backend))
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				mw := func(next Handler) Handler {
					return func(ctx context.Context, job Job) error {
						calls++
						if calls == 1 {
							panic("middleware failure")
						}
						return next(ctx, job)
					}
				}
				h := Handle(func(context.Context, Job, int) error { return nil }, mw)
				if kind == TaskTopology {
					err = b.Register("panic", h)
				} else {
					err = b.Subscribe("panic", "subscriber", h)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := b.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				for _, d := range deliveries {
					<-d.(*workerPoolDelivery).settled
				}
				if err := b.Drain(context.Background()); err != nil {
					t.Fatal(err)
				}
				if calls != 2 {
					t.Fatalf("worker processed %d deliveries, want 2", calls)
				}
			})
		})
	}
}

func TestRecovererCompatibilityAndExplicitClassification(t *testing.T) {
	logger := &panicLogger{}
	h := Recoverer(logger)(func(context.Context, Job) error { panic("synthetic-sensitive-value") })
	err := invokeHandler(context.Background(), Job{env: &Envelope{ID: "test", Type: "panic"}}, h, logger)
	if !errors.Is(err, ErrHandlerPanic) || len(logger.stacks) != 1 {
		t.Fatalf("recovery = %v, logs = %d", err, len(logger.stacks))
	}
	// An outer middleware may explicitly classify a recovered panic, rather
	// than letting the core treat an arbitrary panicked error as authority.
	cfg := DefaultConfig()
	e := newEngine(cfg, nil, NewCodec(nil))
	defer e.cancel()
	defer e.workerCancel()
	payload, _, err := e.codec.EncodeTopology(TaskTopology, "panic", 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	d := &attemptDelivery{message: transport.Message{Payload: payload}, deliveryCount: 1}
	e.processDelivery(TaskTopology, "panic", "", func(ctx context.Context, job Job) error { return MarkPermanent(h(ctx, job)) }, d, 0)
	if len(d.deadLetters) != 1 || d.deadLetters[0].Reason != "permanent_error" {
		t.Fatal("explicit permanent classification was not preserved")
	}
}
