package eventbus

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type leaseTestDelivery struct {
	metadataDelivery
	beats  int
	failAt int
}

type completionRaceDelivery struct {
	metadataDelivery
	beats int
}

func (*completionRaceDelivery) HeartbeatInterval() time.Duration { return 10 * time.Millisecond }
func (d *completionRaceDelivery) Heartbeat(ctx context.Context) error {
	d.beats++
	if d.beats == 1 {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (*leaseTestDelivery) HeartbeatInterval() time.Duration { return 10 * time.Millisecond }
func (d *leaseTestDelivery) Heartbeat(context.Context) error {
	d.beats++
	if d.beats >= d.failAt {
		return transport.ErrLeaseLost
	}
	return nil
}

func TestHandlerCompletionCancellationIsNotLeaseFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := newEngine(DefaultConfig(), nil, NewCodec(nil))
		defer e.cancel()
		defer e.workerCancel()
		payload, _, err := e.codec.EncodeTopology(TaskTopology, "lease", 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		d := &completionRaceDelivery{metadataDelivery: metadataDelivery{message: transport.Message{Payload: payload}}}
		e.processDelivery(TaskTopology, "lease", "", func(context.Context, Job) error {
			time.Sleep(15 * time.Millisecond)
			return nil
		}, d, 0)
		if d.decision.Kind != transport.DecisionAck || d.beats != 2 {
			t.Fatalf("decision/beats = %s/%d, want ack/2", d.decision.Kind, d.beats)
		}
	})
}

func TestLeaseFailurePreventsHandlerOrCancelsExecution(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		synctest.Test(t, func(t *testing.T) {
			e := newEngine(DefaultConfig(), nil, NewCodec(nil))
			defer e.cancel()
			defer e.workerCancel()
			payload, _, err := e.codec.EncodeTopology(TaskTopology, "lease", 1, 3)
			if err != nil {
				t.Fatal(err)
			}
			d := &leaseTestDelivery{metadataDelivery: metadataDelivery{message: transport.Message{Payload: payload}}, failAt: failAt}
			called := false
			e.processDelivery(TaskTopology, "lease", "", func(ctx context.Context, _ Job) error { called = true; <-ctx.Done(); return nil }, d, 0)
			if called != (failAt == 2) || d.decision.Kind != 0 || d.beats != failAt {
				t.Fatal("lease failure executed/settled stale work or ignored short cadence")
			}
		})
	}
}
