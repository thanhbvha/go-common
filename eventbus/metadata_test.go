package eventbus

import (
	"context"
	"errors"
	"github.com/goccy/go-json"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type metadataDelivery struct {
	message  transport.Message
	decision transport.Decision
}

func (d *metadataDelivery) Message() transport.Message    { return d.message }
func (*metadataDelivery) DeliveryAttempt() int            { return 1 }
func (*metadataDelivery) Heartbeat(context.Context) error { return nil }
func (d *metadataDelivery) Settle(_ context.Context, decision transport.Decision) error {
	d.decision = decision
	return nil
}

func TestDecodedMetadataReachesRetryAndDLQ(t *testing.T) {
	for _, outcome := range []string{"retry", "permanent", "exhausted", "poison", "ack"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.RetryPolicy = ConstantBackoff{}
			e := newEngine(cfg, nil, NewCodec(nil))
			defer e.cancel()
			defer e.workerCancel()
			payload, env, err := e.codec.EncodeTopology(TaskTopology, "task", 1, 3, WithOrderingKey("ordering"), WithDedupeKey("dedupe"), WithHeader("correlation-id", "synthetic"))
			if err != nil {
				t.Fatal(err)
			}
			d := &metadataDelivery{message: transport.Message{ID: "stale", Type: "wrong", Payload: payload, OrderingKey: "stale", DedupeKey: "stale", Headers: map[string]string{"wrong": "stale"}}}
			limit := 0
			if outcome == "exhausted" {
				limit = 1
			}
			if outcome == "poison" {
				env.Attempt = 4
				d.message.Payload, err = json.Marshal(env)
				if err != nil {
					t.Fatal(err)
				}
			}
			called := false
			e.processDelivery(TaskTopology, "task", "", func(context.Context, Job) error {
				called = true
				if outcome == "ack" {
					return nil
				}
				if outcome == "permanent" {
					return MarkPermanent(errors.New("synthetic"))
				}
				return errors.New("synthetic")
			}, d, limit)
			if outcome == "poison" && called {
				t.Fatal("poison invoked handler")
			}
			if d.decision.Source == nil {
				t.Fatal("driver did not receive normalized source")
			}
			source := d.decision.Source
			if source.ID != env.ID || source.Type != env.Type || source.OrderingKey != "ordering" || source.DedupeKey != "dedupe" || source.Headers["correlation-id"] != "synthetic" || string(source.Payload) != string(d.message.Payload) {
				t.Fatal("source metadata not restored from envelope")
			}
			if outcome == "retry" {
				next := d.decision.Next
				if next.ID != env.ID || next.Type != env.Type || next.OrderingKey != "ordering" || next.DedupeKey != "dedupe" || next.Headers["correlation-id"] != "synthetic" {
					t.Fatal("retry lost normalized metadata")
				}
			}
			source.Headers["correlation-id"] = "changed"
			if d.message.Headers["wrong"] != "stale" {
				t.Fatal("normalization mutated driver-owned headers")
			}
		})
	}
}

func TestDecodeFailurePreservesRawSource(t *testing.T) {
	e := newEngine(DefaultConfig(), nil, NewCodec(nil))
	defer e.cancel()
	defer e.workerCancel()
	d := &metadataDelivery{message: transport.Message{ID: "broker-id", Payload: []byte("invalid")}}
	e.processDelivery(TaskTopology, "task", "", func(context.Context, Job) error { t.Fatal("malformed envelope reached handler"); return nil }, d, 0)
	if d.decision.Source != nil || d.decision.Reason != "decode_error" {
		t.Fatal("decode failure replaced raw source")
	}
}
