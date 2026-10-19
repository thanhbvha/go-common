package redis

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

var errSyntheticResponseLoss = errors.New("synthetic response loss after Redis success")

// postSuccessErrorHook models a connection dying after Redis has committed a
// command but before the caller receives the response. It never prevents the
// command from reaching Redis.
type postSuccessErrorHook struct {
	commands map[string]bool
	fired    atomic.Bool
}

func (h *postSuccessErrorHook) DialHook(next goredis.DialHook) goredis.DialHook {
	return next
}

func (h *postSuccessErrorHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && h.commands[strings.ToLower(cmd.Name())] && h.fired.CompareAndSwap(false, true) {
			return errSyntheticResponseLoss
		}
		return err
	}
}

func (h *postSuccessErrorHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

// DS-10/20: a publish response can be ambiguous, but an executed atomic
// capacity-check-and-XADD script remains durable. The caller receives an error
// and may retry, so duplication is allowed; silent loss is not.
func TestPublishResponseLossLeavesAcceptedSource(t *testing.T) {
	backend, rc := delayedBackend(t)
	rc.AddHook(&postSuccessErrorHook{commands: map[string]bool{"eval": true, "evalsha": true}})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "fault", JobType: "publish-response"}
	err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("accepted")})
	if !errors.Is(err, errSyntheticResponseLoss) {
		t.Fatalf("publish error = %v", err)
	}
	entries, readErr := rc.XRange(ctx, backend.streamKey(topo), "-", "+").Result()
	if readErr != nil || len(entries) != 1 || entries[0].Values["payload"] != "accepted" {
		t.Fatalf("accepted publish lost: entries=%v err=%v", entries, readErr)
	}
}

// A connection failure before Redis executes a command is the other side of
// the ambiguity window: publish must not claim acceptance, and failed ACK must
// leave the pending source available for redelivery.
func TestConnectionFailureBeforePublishAndAckKeepsState(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		backend, rc := delayedBackend(t)
		observer := goredis.NewClient(rc.Options())
		t.Cleanup(func() { _ = observer.Close() })
		ctx := context.Background()
		topo := transport.Topology{Namespace: "fault", JobType: "publish-before"}
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
		if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("not-accepted")}); err == nil {
			t.Fatal("publish succeeded through a closed connection")
		}
		if count, err := observer.XLen(ctx, backend.streamKey(topo)).Result(); err != nil || count != 0 {
			t.Fatalf("failed publish mutated stream: count=%d err=%v", count, err)
		}
	})

	t.Run("ack", func(t *testing.T) {
		backend, rc := delayedBackend(t)
		observer := goredis.NewClient(rc.Options())
		t.Cleanup(func() { _ = observer.Close() })
		ctx := context.Background()
		topo := transport.Topology{Namespace: "fault", JobType: "ack-before"}
		delivery := pendingDelivery(t, backend, rc, topo)
		delivery.consumer = "test"
		if err := rc.Close(); err != nil {
			t.Fatal(err)
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err == nil {
			t.Fatal("ACK succeeded through a closed connection")
		}
		pending, err := observer.XPending(ctx, delivery.stream, delivery.group).Result()
		if err != nil || pending.Count != 1 {
			t.Fatalf("failed ACK lost source: pending=%v err=%v", pending, err)
		}
	})
}

// DS-40/42/44: retry and DLQ scripts may commit before their response is lost.
// Retrying must observe the missing pending source and must not write a second
// destination record.
func TestTransferResponseLossIsIdempotent(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(kind.String(), func(t *testing.T) {
			backend, rc := delayedBackend(t)
			ctx := context.Background()
			topo := transport.Topology{Namespace: "fault", JobType: "transfer-response-" + kind.String()}
			delivery := pendingDelivery(t, backend, rc, topo)
			rc.AddHook(&postSuccessErrorHook{commands: map[string]bool{"eval": true, "evalsha": true}})
			decision := transferDecision(kind)
			if err := delivery.Settle(ctx, decision); !errors.Is(err, errSyntheticResponseLoss) {
				t.Fatalf("ambiguous settlement = %v", err)
			}
			if pending, err := rc.XPending(ctx, delivery.stream, delivery.group).Result(); err != nil || pending.Count != 0 {
				t.Fatalf("source not committed: pending=%v err=%v", pending, err)
			}
			if err := delivery.Settle(ctx, decision); !errors.Is(err, transport.ErrDeliverySettled) {
				t.Fatalf("ambiguous retry = %v", err)
			}
			if kind == transport.DecisionRetry {
				if count, err := rc.ZCard(ctx, backend.delayedKey(topo)).Result(); err != nil || count != 1 {
					t.Fatalf("retry destinations = %d, %v", count, err)
				}
			} else if count, err := rc.XLen(ctx, backend.dlqKey(topo)).Result(); err != nil || count != 1 {
				t.Fatalf("DLQ destinations = %d, %v", count, err)
			}
		})
	}
}

// An ACK response can also be lost after the ownership-checked Lua command.
// A retry reports lease loss because the pending source is already gone; it
// cannot accidentally settle another consumer's work.
func TestAckResponseLossDoesNotRestoreOrStealSource(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "fault", JobType: "ack-response"}
	delivery := pendingDelivery(t, backend, rc, topo)
	delivery.consumer = "test"
	rc.AddHook(&postSuccessErrorHook{commands: map[string]bool{"eval": true, "evalsha": true}})
	decision := transport.Decision{Kind: transport.DecisionAck}
	if err := delivery.Settle(ctx, decision); !errors.Is(err, errSyntheticResponseLoss) {
		t.Fatalf("ambiguous ACK = %v", err)
	}
	if pending, err := rc.XPending(ctx, delivery.stream, delivery.group).Result(); err != nil || pending.Count != 0 {
		t.Fatalf("ACK did not commit: pending=%v err=%v", pending, err)
	}
	if err := delivery.Settle(ctx, decision); !errors.Is(err, transport.ErrLeaseLost) {
		t.Fatalf("ACK retry = %v", err)
	}
}
