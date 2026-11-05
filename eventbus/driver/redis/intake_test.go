package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

type intakeCommandHook struct {
	before   func(context.Context, goredis.Cmder) error
	pipeline func(context.Context, []goredis.Cmder) error
}

func (h intakeCommandHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }
func (h intakeCommandHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if h.before != nil {
			if err := h.before(ctx, cmd); err != nil {
				return err
			}
		}
		return next(ctx, cmd)
	}
}
func (h intakeCommandHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		if h.pipeline != nil {
			if err := h.pipeline(ctx, cmds); err != nil {
				return err
			}
		}
		return next(ctx, cmds)
	}
}
func intakeFixture(t *testing.T) (*streamIntake, *goredis.Client) {
	t.Helper()
	backend, rc := delayedBackend(t)
	topo := targetedEventTopology("intake", "fair")
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "router"}
	if err := backend.EnsureSubscription(context.Background(), sub); err != nil {
		t.Fatal(err)
	}
	readerClient := goredis.NewClient(rc.Options())
	t.Cleanup(func() { _ = readerClient.Close() })
	return &streamIntake{client: readerClient, streams: []string{backend.streamKey(topo), backend.subscriberRetryStreamKey(topo, backend.groupName(sub))}, group: backend.groupName(sub), consumer: "test", block: time.Second}, readerClient
}
func addIntake(t *testing.T, rc goredis.UniversalClient, stream string, n int) {
	t.Helper()
	for i := range n {
		if err := rc.XAdd(context.Background(), &goredis.XAddArgs{Stream: stream, Values: map[string]any{"payload": fmt.Sprint(i)}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
}
func assertIntakePending(t *testing.T, r *streamIntake, want int64) {
	t.Helper()
	var got int64
	for _, stream := range r.streams {
		p, err := r.client.XPending(context.Background(), stream, r.group).Result()
		if err != nil {
			t.Fatal(err)
		}
		got += p.Count
	}
	if got != want {
		t.Fatalf("pending=%d, want %d", got, want)
	}
}

// This catches the old blocking-empty-lane behavior without a timing threshold.
func TestEventIntakeSkipsEmptyLaneWithinReservedBudget(t *testing.T) {
	for _, budget := range []int64{1, 2, 3, 30} {
		for ready := range 2 {
			t.Run(fmt.Sprintf("budget%d/ready%d", budget, ready), func(t *testing.T) {
				r, rc := intakeFixture(t)
				r.preferred = 1 - ready
				addIntake(t, rc, r.streams[ready], int(budget)+2)
				rc.AddHook(intakeCommandHook{before: func(_ context.Context, cmd goredis.Cmder) error {
					if cmd.Name() == "xreadgroup" {
						for _, arg := range cmd.Args() {
							if arg == "block" {
								t.Error("event intake blocked on an individual lane")
							}
						}
					}
					return nil
				}})
				got, err := r.read(context.Background(), budget)
				if err != nil || len(got) != 1 || got[0].Stream != r.streams[ready] || len(got[0].Messages) != int(budget) {
					t.Fatalf("read=%v err=%v", got, err)
				}
				assertIntakePending(t, r, budget)
			})
		}
	}
}
func TestEventIntakeBothBusyAreFair(t *testing.T) {
	r, rc := intakeFixture(t)
	for _, s := range r.streams {
		addIntake(t, rc, s, 3)
	}
	for i := range 6 {
		got, err := r.read(context.Background(), 1)
		if err != nil || len(got) != 1 || got[0].Stream != r.streams[i%2] {
			t.Fatalf("read %d: %v %v", i, got, err)
		}
	}
	assertIntakePending(t, r, 6)
}

// Append precisely between the empty sweep and the blocking waiter. Both lanes
// may wake it, but a read-only wake-up must NOT acquire two PEL entries with a
// one-delivery budget, or lose the append by using the special '$' ID.
func TestEventIntakeWakeupHasNoLostAppendOrUnbudgetedOwnership(t *testing.T) {
	r, rc := intakeFixture(t)
	observer := goredis.NewClient(rc.Options())
	defer observer.Close()
	fired := false
	rc.AddHook(intakeCommandHook{before: func(_ context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "xread" && !fired {
			fired = true
			for _, s := range r.streams {
				addIntake(t, observer, s, 1)
			}
		}
		return nil
	}})
	if _, err := r.read(context.Background(), 1); err != goredis.Nil || !fired {
		t.Fatalf("wake=%v fired=%v", err, fired)
	}
	assertIntakePending(t, r, 0)
	for range 2 {
		got, err := r.read(context.Background(), 1)
		if err != nil || len(got) != 1 || len(got[0].Messages) != 1 {
			t.Fatalf("read=%v %v", got, err)
		}
	}
	assertIntakePending(t, r, 2)
}
func TestEventIntakeSecondSweepFindsAppendBeforeTailSnapshot(t *testing.T) {
	r, rc := intakeFixture(t)
	observer := goredis.NewClient(rc.Options())
	defer observer.Close()
	rc.AddHook(intakeCommandHook{pipeline: func(ctx context.Context, cmds []goredis.Cmder) error {
		if len(cmds) > 0 && cmds[0].Name() == "xinfo" {
			addIntake(t, observer, r.streams[1], 1)
		}
		return nil
	}, before: func(_ context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "xread" {
			t.Error("blocked despite append before tail snapshot")
		}
		return nil
	}})
	got, err := r.read(context.Background(), 1)
	if err != nil || len(got) != 1 || got[0].Stream != r.streams[1] {
		t.Fatalf("read=%v %v", got, err)
	}
}
func TestEventIntakePropagatesWaitAndSnapshotFailures(t *testing.T) {
	for _, where := range []string{"snapshot", "wait", "cancel"} {
		t.Run(where, func(t *testing.T) {
			r, rc := intakeFixture(t)
			want := errors.New("synthetic intake failure")
			if where == "cancel" {
				want = context.Canceled
			}
			rc.AddHook(intakeCommandHook{pipeline: func(context.Context, []goredis.Cmder) error {
				if where == "snapshot" {
					return want
				}
				return nil
			}, before: func(_ context.Context, cmd goredis.Cmder) error {
				if cmd.Name() == "xread" {
					return want
				}
				return nil
			}})
			if _, err := r.read(context.Background(), 1); !errors.Is(err, want) {
				t.Fatalf("err=%v, want %v", err, want)
			}
			assertIntakePending(t, r, 0)
		})
	}
}
func TestTaskIntakeKeepsSingleStreamBlockingRead(t *testing.T) {
	r, rc := intakeFixture(t)
	r.streams = r.streams[:1]
	addIntake(t, rc, r.streams[0], 1)
	blocked := false
	rc.AddHook(intakeCommandHook{before: func(_ context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "xreadgroup" {
			for _, arg := range cmd.Args() {
				if arg == "block" {
					blocked = true
				}
			}
		}
		return nil
	}})
	if _, err := r.read(context.Background(), 1); err != nil || !blocked {
		t.Fatalf("err=%v block=%v", err, blocked)
	}
}

func TestEventIntakeIdleTimeoutReusesEmptyTails(t *testing.T) {
	r, rc := intakeFixture(t)
	commands := 0
	pipelines := 0
	rc.AddHook(intakeCommandHook{before: func(_ context.Context, cmd goredis.Cmder) error {
		commands++
		if cmd.Name() == "xread" {
			return goredis.Nil
		}
		return nil
	}, pipeline: func(context.Context, []goredis.Cmder) error { pipelines++; return nil }})
	if _, err := r.read(context.Background(), 1); err != goredis.Nil {
		t.Fatal(err)
	}
	commands, pipelines = 0, 0
	for range 3 {
		if _, err := r.read(context.Background(), 1); err != goredis.Nil {
			t.Fatal(err)
		}
	}
	if commands != 3 || pipelines != 0 {
		t.Fatalf("idle timeout repeated polling: commands=%d pipelines=%d", commands, pipelines)
	}
}

func TestStreamTailCursorIgnoresNewMetadataAcrossProtocols(t *testing.T) {
	for _, value := range []any{
		[]interface{}{"length", int64(0), "idmp-duration", int64(100), "last-generated-id", "12-3", "new-field", []interface{}{}},
		map[interface{}]interface{}{"last-generated-id": "12-3", "new-field": true},
		map[string]interface{}{"last-generated-id": "12-3", "new-field": true},
	} {
		id, err := streamTailID(value)
		if err != nil || id != "12-3" {
			t.Fatalf("cursor=%q err=%v", id, err)
		}
	}
	for _, value := range []any{nil, []interface{}{"length", int64(0)}, []interface{}{"last-generated-id"}, map[string]interface{}{"last-generated-id": 123}} {
		if _, err := streamTailID(value); err == nil {
			t.Fatalf("invalid cursor accepted: %v", value)
		}
	}
}

// Exercise the public transport entry point: ready retry work must not be
// delayed by a blocking read of the empty main lane. No timing gate is needed.
func TestConsumeReadyRetryNeverBlocksOnEmptyMain(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	blocked := errors.New("blocking empty main lane while retry is ready")
	rc.AddHook(intakeCommandHook{before: func(_ context.Context, cmd goredis.Cmder) error {
		if cmd.Name() == "xreadgroup" {
			for _, arg := range cmd.Args() {
				if arg == "block" {
					return blocked
				}
			}
		}
		return nil
	}})
	backend := New(rc).(*redisTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()); _ = rc.Close() })
	topo := targetedEventTopology("consume-intake", "ready-retry")
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "router", Concurrency: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	addIntake(t, rc, backend.subscriberRetryStreamKey(topo, backend.groupName(sub)), 1)
	delivered := false
	err := backend.Consume(ctx, sub, func(transport.Delivery) { delivered = true; cancel() })
	if !errors.Is(err, context.Canceled) || !delivered {
		t.Fatalf("ready retry was blocked: delivered=%v err=%v", delivered, err)
	}
}
