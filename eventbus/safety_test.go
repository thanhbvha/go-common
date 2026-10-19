package eventbus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

type hostileError struct{}

func (hostileError) Error() string { panic("must not render hostile error") }
func (hostileError) Unwrap() error { panic("must not unwrap hostile error") }

type panicRetryPolicy struct{}

func (panicRetryPolicy) NextBackoff(int) time.Duration { panic("must recover retry policy panic") }

type fixedRetryPolicy time.Duration

func (p fixedRetryPolicy) NextBackoff(int) time.Duration { return time.Duration(p) }

func TestEnvelopeValidationRejectsHostileRoutesAndMetadata(t *testing.T) {
	codec := NewCodec(nil)
	basePayload, base, err := codec.EncodeTopology(TaskTopology, "task", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	_ = basePayload
	tests := []struct {
		name   string
		mutate func(*Envelope)
		kind   TopologyKind
		type_  string
		reason string
	}{
		{name: "empty ID", mutate: func(e *Envelope) { e.ID = "" }, kind: TaskTopology, type_: "task", reason: "invalid_envelope_id"},
		{name: "oversized ID", mutate: func(e *Envelope) { e.ID = strings.Repeat("a", maxEnvelopeIDBytes+1) }, kind: TaskTopology, type_: "task", reason: "invalid_envelope_id"},
		{name: "wrong kind", mutate: func(*Envelope) {}, kind: EventTopology, type_: "task", reason: "topology_kind_mismatch"},
		{name: "wrong type", mutate: func(*Envelope) {}, kind: TaskTopology, type_: "other", reason: "topology_type_mismatch"},
		{name: "missing timestamp", mutate: func(e *Envelope) { e.CreatedAt = 0 }, kind: TaskTopology, type_: "task", reason: "invalid_timestamps"},
		{name: "run before creation", mutate: func(e *Envelope) { e.RunAt = e.CreatedAt - 1 }, kind: TaskTopology, type_: "task", reason: "invalid_timestamps"},
		{name: "invalid headers", mutate: func(e *Envelope) { e.Headers = Headers{"traceparent": "bad\nvalue"} }, kind: TaskTopology, type_: "task", reason: "invalid_headers"},
		{name: "invalid dedupe", mutate: func(e *Envelope) { e.DedupeTTL = int64(time.Second) }, kind: TaskTopology, type_: "task", reason: "invalid_dedupe"},
		{name: "oversized key", mutate: func(e *Envelope) { e.OrderingKey = strings.Repeat("k", maxEnvelopeKeyBytes+1) }, kind: TaskTopology, type_: "task", reason: "invalid_keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			e := newEngine(cfg, nil, codec)
			defer e.cancel()
			defer e.workerCancel()
			env := *base
			env.Headers = cloneHeaders(base.Headers)
			tt.mutate(&env)
			payload, err := json.Marshal(env)
			if err != nil {
				t.Fatal(err)
			}
			d := &metadataDelivery{message: transport.Message{ID: "broker-id", Payload: payload}}
			called := false
			e.processDelivery(tt.kind, tt.type_, "", func(context.Context, Job) error { called = true; return nil }, d, 0)
			if called || d.decision.Kind != transport.DecisionDeadLetter || d.decision.Reason != tt.reason || d.decision.ErrorCode != tt.reason {
				t.Fatalf("called=%v decision=%+v", called, d.decision)
			}
		})
	}
}

func TestHostileErrorsAndRetryPoliciesCannotPanicProcessing(t *testing.T) {
	tests := []struct {
		name       string
		policy     RetryPolicy
		handlerErr error
		wantDelay  time.Duration
		wantCode   string
	}{
		{name: "hostile error", policy: ConstantBackoff{Delay: time.Second}, handlerErr: hostileError{}, wantDelay: time.Second, wantCode: "error_render_panic"},
		{name: "policy panic", policy: panicRetryPolicy{}, handlerErr: errors.New("synthetic"), wantDelay: 0, wantCode: "retry_policy_panic"},
		{name: "negative retry after", policy: fixedRetryPolicy(time.Hour), handlerErr: MarkRetryAfter(errors.New("synthetic"), -time.Second), wantDelay: 0, wantCode: "handler_error"},
		{name: "oversized policy delay", policy: fixedRetryPolicy(time.Duration(1<<63 - 1)), handlerErr: errors.New("synthetic"), wantDelay: maxRetryDelay, wantCode: "handler_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.RetryPolicy = tt.policy
			e := newEngine(cfg, nil, NewCodec(nil))
			defer e.cancel()
			defer e.workerCancel()
			payload, _, err := e.codec.EncodeTopology(TaskTopology, "task", 1, 2)
			if err != nil {
				t.Fatal(err)
			}
			d := &metadataDelivery{message: transport.Message{Payload: payload}}
			started := time.Now()
			e.processDelivery(TaskTopology, "task", "", func(context.Context, Job) error { return tt.handlerErr }, d, 0)
			if d.decision.Kind != transport.DecisionRetry || d.decision.ErrorCode != tt.wantCode {
				t.Fatalf("decision = %+v", d.decision)
			}
			delay := d.decision.RetryAt.Sub(started)
			if delay < tt.wantDelay || delay > tt.wantDelay+time.Second {
				t.Fatalf("retry delay = %v, want %v", delay, tt.wantDelay)
			}
		})
	}
}

func TestOutcomeErrorsAreNilSafeAndSanitized(t *testing.T) {
	var permanent *PermanentError
	var retry *RetryAfterError
	if permanent.Error() != "eventbus: permanent error" || permanent.Unwrap() != nil {
		t.Fatal("nil permanent error is unsafe")
	}
	if retry.Error() != "eventbus: retry requested" || retry.Unwrap() != nil {
		t.Fatal("nil retry-after error is unsafe")
	}
	if got := (&SkipError{Reason: "secret"}).Error(); strings.Contains(got, "secret") {
		t.Fatal("skip reason leaked through Error")
	}
}
