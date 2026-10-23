package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func delayedBackend(t *testing.T) (*redisTransport, *goredis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	backend := New(rc).(*redisTransport)
	t.Cleanup(func() {
		_ = backend.Close(context.Background())
		_ = rc.Close()
	})
	return backend, rc
}

// DS-25/80/82: no early delivery, bounded batch, unchanged durable envelope,
// and no trimming of an existing unprocessed destination backlog.
func TestDelayedDispatchBoundedAndPreservesEnvelopes(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "delayed", Options: map[string]any{"redis.delay_batch_size": 2}}
	now := time.Unix(1700000000, 0)
	want := map[string]bool{"existing": true}
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.streamKey(topo), Values: map[string]any{"payload": "existing"}}).Err(); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		at := now
		if i == 3 {
			at = now.Add(time.Second)
		}
		payload, _, err := eventbus.NewCodec(nil).EncodeTopology(eventbus.TaskTopology, topo.JobType, i, 3,
			eventbus.WithOrderingKey(fmt.Sprintf("order-%d", i)), eventbus.WithDedupeKey(fmt.Sprintf("dedupe-%d", i)), eventbus.WithHeader("correlation-id", "synthetic"))
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.PublishAt(ctx, topo, transport.Message{Payload: payload}, at); err != nil {
			t.Fatal(err)
		}
		want[string(payload)] = true
	}
	for _, step := range []struct {
		at    time.Time
		moved int64
	}{{now.Add(-time.Millisecond), 0}, {now, 2}, {now, 1}, {now, 0}, {now.Add(time.Second), 1}} {
		moved, err := backend.dispatchDue(ctx, topo, step.at)
		if err != nil || moved != step.moved {
			t.Fatalf("moved = %d, %v; want %d", moved, err, step.moved)
		}
	}
	records, err := rc.XRange(ctx, backend.streamKey(topo), "-", "+").Result()
	if err != nil || len(records) != len(want) {
		t.Fatalf("stream count = %d, %v", len(records), err)
	}
	for _, record := range records {
		payload := record.Values["payload"].(string)
		if !want[payload] {
			t.Fatal("envelope modified or duplicated")
		}
		delete(want, payload)
	}
	if count, err := rc.ZCard(ctx, backend.delayedKey(topo)).Result(); err != nil || count != 0 {
		t.Fatal("moved sources retained")
	}
}

// DS-81: independent scheduler instances share no process-local lock; the
// server-side script serializes ownership of each due source entry.
func TestDelayedDispatchMultipleSweepers(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "multi", Options: map[string]any{"redis.delay_batch_size": 7}}
	now := time.Unix(1700000000, 0)
	for i := range 101 {
		if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte(fmt.Sprintf("job-%d", i))}, now); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			instance := New(rc).(*redisTransport)
			for {
				n, err := instance.dispatchDue(ctx, topo, now)
				if err != nil {
					errs <- err
					return
				}
				if n == 0 {
					return
				}
				if n > 7 {
					errs <- fmt.Errorf("batch exceeded limit")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	records, err := rc.XRange(ctx, backend.streamKey(topo), "-", "+").Result()
	if err != nil || len(records) != 101 {
		t.Fatalf("ready count = %d, %v", len(records), err)
	}
	seen := map[string]bool{}
	for _, record := range records {
		value := record.Values["payload"].(string)
		if seen[value] {
			t.Fatal("concurrent dispatch duplicated entry")
		}
		seen[value] = true
	}
}

func TestEventDelayedDispatchIsBoundedAndFairAcrossSubscriberLanes(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	now := time.Unix(1700000000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "fair", Kind: transport.EventTopology, Options: map[string]any{"redis.delay_batch_size": 1, redisEventLayoutOption: redisTargetedLayoutV1}}
	ensureTargetedEventTopology(t, backend, topo)
	for _, name := range []string{"alpha", "beta"} {
		sub := transport.Subscription{Topology: topo, Group: name}
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
		group := backend.groupName(sub)
		if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, group), goredis.Z{Score: float64(now.UnixMilli()), Member: name}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("shared")}, now); err != nil {
		t.Fatal(err)
	}
	for pass := range 3 {
		moved, err := backend.dispatchDue(ctx, topo, now)
		if err != nil || moved != 1 {
			t.Fatalf("pass %d moved = %d, %v", pass, moved, err)
		}
	}
	if moved, err := backend.dispatchDue(ctx, topo, now); err != nil || moved != 0 {
		t.Fatalf("fourth pass moved = %d, %v", moved, err)
	}
	if count, err := rc.XLen(ctx, backend.streamKey(topo)).Result(); err != nil || count != 1 {
		t.Fatalf("shared ready = %d, %v", count, err)
	}
	for _, name := range []string{"alpha", "beta"} {
		group := "subscriber:" + name
		if count, err := rc.XLen(ctx, backend.subscriberRetryStreamKey(topo, group)).Result(); err != nil || count != 1 {
			t.Fatalf("%s private ready = %d, %v", name, count, err)
		}
	}
}

func TestEventDelayedDispatchScansBoundedSubscriberPages(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "bounded-scan", Kind: transport.EventTopology, Options: map[string]any{"redis.delay_batch_size": 1000, redisEventLayoutOption: redisTargetedLayoutV1}}
	ensureTargetedEventTopology(t, backend, topo)
	for index := range 200 {
		sub := transport.Subscription{Topology: topo, Group: fmt.Sprintf("subscriber-%03d", index)}
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
		group := backend.groupName(sub)
		if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, group), goredis.Z{Score: float64(now.UnixMilli()), Member: group}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	hook := &schedulerCommandHook{}
	rc.AddHook(hook)
	moved, err := backend.dispatchDue(ctx, topo, now)
	if err != nil {
		t.Fatal(err)
	}
	if moved > schedulerLaneScanMax || hook.zrange.Load() > 2 || hook.scripts.Load() > 2*schedulerLaneScanMax {
		t.Fatalf("moved=%d range=%d scripts=%d", moved, hook.zrange.Load(), hook.scripts.Load())
	}
	remaining, err := rc.HLen(ctx, backend.groupManifestKey(topo)).Result()
	if err != nil || remaining != 200 {
		t.Fatalf("manifest changed during bounded scan: %d, %v", remaining, err)
	}
}

type schedulerCommandHook struct {
	zrange  atomic.Int64
	scripts atomic.Int64
}

func (h *schedulerCommandHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *schedulerCommandHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		switch strings.ToLower(cmd.Name()) {
		case "zrangebylex":
			h.zrange.Add(1)
		case "eval", "evalsha":
			h.scripts.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (h *schedulerCommandHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

// Redis scripts do not roll back runtime errors. Wrong-type keys must fail
// before writes, leaving every accepted delayed record available for retry.
func TestDelayedDispatchFailureRetainsSources(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{JobType: "failure"}
	now := time.Unix(1700000000, 0)
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("job")}, now); err != nil {
		t.Fatal(err)
	}
	if err := rc.Set(ctx, backend.streamKey(topo), "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, now); err == nil {
		t.Fatal("wrong-type destination succeeded")
	}
	if count, err := rc.ZCard(ctx, backend.delayedKey(topo)).Result(); err != nil || count != 1 {
		t.Fatal("source lost on destination failure")
	}
	if err := rc.Del(ctx, backend.streamKey(topo)).Err(); err != nil {
		t.Fatal(err)
	}
	if count, err := backend.dispatchDue(ctx, topo, now); err != nil || count != 1 {
		t.Fatal("source could not be retried")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := backend.dispatchDue(canceled, topo, now); err == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestRedisSchedulerHealthAndCloseJoin(t *testing.T) {
	backend, _ := delayedBackend(t)
	topo := transport.Topology{Namespace: "test", JobType: "health"}
	started := make(chan struct{})
	release := make(chan struct{})
	backend.sweepDelay = func(ctx context.Context, _ transport.Topology, _ time.Time) (int64, error) {
		close(started)
		<-release
		return 0, errors.New("synthetic scheduler failure")
	}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := backend.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v, want deadline", err)
	}
	close(release)
	if err := backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRedisSchedulerHealthRecordsFailureAndRecovery(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "health-status"}
	now := time.Unix(1_700_000_000, 0)
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("job")}, now); err != nil {
		t.Fatal(err)
	}
	if err := rc.Set(ctx, backend.streamKey(topo), "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, now); err == nil {
		t.Fatal("expected dispatch failure")
	}
	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || !status[0].Degraded || status[0].Retries == 0 || status[0].LastErrorOperation != "dispatch" {
		t.Fatalf("failure status = %#v", status)
	}
	if err := rc.Del(ctx, backend.streamKey(topo)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, now); err != nil {
		t.Fatal(err)
	}
	status = backend.SchedulerHealth(topo)
	if len(status) != 1 || status[0].Degraded || status[0].ConsecutiveErrors != 0 || status[0].LastSuccess.IsZero() {
		t.Fatalf("recovered status = %#v", status)
	}
}
