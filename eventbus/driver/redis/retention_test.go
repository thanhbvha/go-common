package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func stoppedRetentionBackend(t *testing.T) (*redisTransport, *goredis.Client) {
	t.Helper()
	backend, rc := delayedBackend(t)
	if err := backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return backend, rc
}

func TestEnsureSubscriptionDurablyRegistersGroupAndEpoch(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "provision")
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "billing"}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	state, err := rc.HGet(ctx, backend.groupManifestKey(topo), "subscriber:billing").Result()
	if err != nil || state != groupStateActive {
		t.Fatalf("manifest state = %q, %v", state, err)
	}
	epoch, err := rc.HGet(ctx, backend.metadataKey(topo), "retention_epoch").Int64()
	if err != nil || epoch != 1 {
		t.Fatalf("epoch = %d, %v", epoch, err)
	}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	epochAgain, _ := rc.HGet(ctx, backend.metadataKey(topo), "retention_epoch").Int64()
	if epochAgain != epoch {
		t.Fatalf("idempotent provision changed epoch: %d -> %d", epoch, epochAgain)
	}
}

func TestCompactionProtectsUnreadAndPendingAcrossGroups(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "watermark")
	ensureTargetedEventTopology(t, backend, topo)
	for _, name := range []string{"fast", "slow"} {
		if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: name}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 5 {
		if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatal(err)
		}
	}
	stream := backend.streamKey(topo)
	read := func(group string, count int64) []goredis.XMessage {
		t.Helper()
		result, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "subscriber:" + group, Consumer: "test", Streams: []string{stream, ">"}, Count: count}).Result()
		if err != nil || len(result) != 1 {
			t.Fatalf("read %s: streams=%d err=%v", group, len(result), err)
		}
		return result[0].Messages
	}
	fast := read("fast", 5)
	fastIDs := make([]string, len(fast))
	for i := range fast {
		fastIDs[i] = fast[i].ID
	}
	if err := rc.XAck(ctx, stream, "subscriber:fast", fastIDs...).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.compactReadyStream(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if n, _ := rc.XLen(ctx, stream).Result(); n != 5 {
		t.Fatalf("offline group lost history: len=%d", n)
	}

	slow := read("slow", 2)
	if _, err := backend.compactReadyStream(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if n, _ := rc.XLen(ctx, stream).Result(); n != 5 {
		t.Fatalf("pending work was trimmed: len=%d", n)
	}
	if err := rc.XAck(ctx, stream, "subscriber:slow", slow[0].ID, slow[1].ID).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.compactReadyStream(ctx, topo); err != nil {
		t.Fatal(err)
	}
	remaining, err := rc.XRange(ctx, stream, "-", "+").Result()
	if err != nil || len(remaining) == 0 || compareStreamIDs(remaining[0].ID, slow[1].ID) < 0 {
		t.Fatalf("unsafe watermark result: entries=%v err=%v", remaining, err)
	}
}

func TestManifestEpochRejectsStaleTrim(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "epoch")
	ensureTargetedEventTopology(t, backend, topo)
	if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	records, _ := rc.XRange(ctx, backend.streamKey(topo), "-", "+").Result()
	oldEpoch, _ := rc.HGet(ctx, backend.metadataKey(topo), "retention_epoch").Result()
	if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: "two"}); err != nil {
		t.Fatal(err)
	}
	trimmed, err := trimAtEpochScript.Run(ctx, rc, []string{
		backend.streamKey(topo), backend.metadataKey(topo), backend.groupManifestKey(topo),
	}, oldEpoch, records[0].ID, 100).Int64()
	if err != nil || trimmed != -1 {
		t.Fatalf("stale trim = %d, %v", trimmed, err)
	}
}

func TestTrimRejectsConcurrentUnmanagedGroup(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "unmanaged-race")
	ensureTargetedEventTopology(t, backend, topo)
	if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: "managed"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	epoch, _ := rc.HGet(ctx, backend.metadataKey(topo), "retention_epoch").Result()
	if err := rc.XGroupCreate(ctx, backend.streamKey(topo), "subscriber:external", "0").Err(); err != nil {
		t.Fatal(err)
	}
	trimmed, err := trimAtEpochScript.Run(ctx, rc, []string{
		backend.streamKey(topo), backend.metadataKey(topo), backend.groupManifestKey(topo),
	}, epoch, "+", 100).Int64()
	if err != nil || trimmed != -2 {
		t.Fatalf("unmanaged race result=%d err=%v", trimmed, err)
	}
	if length, _ := rc.XLen(ctx, backend.streamKey(topo)).Result(); length != 1 {
		t.Fatalf("unmanaged group race trimmed accepted entry: %d", length)
	}
}

func TestCompactionIsExactlyBoundedAndEventuallyReclaimsSmallBacklog(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "retention", JobType: "bounded", Options: map[string]any{
		"redis.retention_batch_size": int64(2),
	}}
	if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo}); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "workers", Consumer: "worker", Streams: []string{backend.streamKey(topo), ">"}, Count: 6}).Result()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(result[0].Messages))
	for i := range result[0].Messages {
		ids[i] = result[0].Messages[i].ID
	}
	if err := rc.XAck(ctx, backend.streamKey(topo), "workers", ids...).Err(); err != nil {
		t.Fatal(err)
	}

	for pass, wantMax := range []int64{4, 2, 1} {
		trimmed, err := backend.compactReadyStream(ctx, topo)
		if err != nil || trimmed > 2 {
			t.Fatalf("pass %d trimmed=%d err=%v", pass, trimmed, err)
		}
		if length, _ := rc.XLen(ctx, backend.streamKey(topo)).Result(); length > wantMax {
			t.Fatalf("pass %d length=%d want <=%d", pass, length, wantMax)
		}
	}
}

func TestReadyCapacityIsAtomicAndObservable(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "retention", JobType: "capacity", Options: map[string]any{"redis.max_ready_entries": int64(10)}}
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- backend.Publish(ctx, topo, transport.Message{Payload: []byte(strconv.Itoa(i))})
		}()
	}
	wg.Wait()
	close(errs)
	accepted, rejected := 0, 0
	for err := range errs {
		if err == nil {
			accepted++
			continue
		}
		if !errors.Is(err, transport.ErrCapacityExceeded) {
			t.Fatalf("unexpected publish error: %v", err)
		}
		rejected++
	}
	if accepted != 10 || rejected != 40 {
		t.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}
	var capacityErr *transport.CapacityError
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("overflow")}); !errors.As(err, &capacityErr) || capacityErr.Topology == "" {
		t.Fatalf("typed capacity error=%+v err=%v", capacityErr, err)
	}
	if n, _ := rc.XLen(ctx, backend.streamKey(topo)).Result(); n != 10 {
		t.Fatalf("ready len=%d", n)
	}
	report, err := backend.RetentionReport(ctx, topo)
	if err != nil || report.Ready || report.CapacityRejections["ready"] != 41 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestRetentionStatusPreservesProgressAndDegradesOnManifestConflict(t *testing.T) {
	backend, _ := stoppedRetentionBackend(t)
	topo := transport.Topology{Namespace: "retention", JobType: "status"}
	now := time.Now()
	backend.setRetentionStatus(topo, RetentionStatus{LastRun: now, LastSuccess: now, SafeWatermark: "1-0", Trimmed: 2})
	backend.setRetentionStatus(topo, RetentionStatus{LastRun: now.Add(time.Second), Blocked: "manifest-changed", EpochConflicts: 1})
	status := backend.RetentionStatus(topo)
	if !status.LastSuccess.Equal(now) || status.SafeWatermark != "1-0" || status.TotalTrimmed != 2 || status.EpochConflicts != 1 {
		t.Fatalf("status=%+v", status)
	}
	if !retentionBlockDegradesReadiness(status.Blocked) || retentionBlockDegradesReadiness("unread-group:offline") {
		t.Fatalf("unexpected readiness classification for %q", status.Blocked)
	}
}

func TestDelayedCapacityAndDispatchBackpressurePreserveSource(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "retention", JobType: "delayed-capacity", Options: map[string]any{
		"redis.max_delayed_entries": int64(1), "redis.max_ready_entries": int64(1),
	}}
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("first")}, now); err != nil {
		t.Fatal(err)
	}
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("second")}, now); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("delayed capacity error=%v", err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("ready")}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, now); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("dispatch error=%v", err)
	}
	if delayed, _ := rc.ZCard(ctx, backend.delayedKey(topo)).Result(); delayed != 1 {
		t.Fatalf("delayed source count=%d", delayed)
	}
	if ready, _ := rc.XLen(ctx, backend.streamKey(topo)).Result(); ready != 1 {
		t.Fatalf("ready count=%d", ready)
	}
}

func TestEventCapacityIsTopologyWideAcrossRetryLanes(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "retention", JobType: "aggregate-capacity", Kind: transport.EventTopology, Options: map[string]any{
		"redis.max_ready_entries": int64(2), "redis.max_delayed_entries": int64(1), redisEventLayoutOption: redisTargetedLayoutV1,
	}}
	ensureTargetedEventTopology(t, backend, topo)
	alpha := transport.Subscription{Topology: topo, Group: "alpha"}
	beta := transport.Subscription{Topology: topo, Group: "beta"}
	for _, sub := range []transport.Subscription{alpha, beta} {
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
	delivery := pendingEventDelivery(t, backend, rc, topo, "alpha")
	if err := delivery.Settle(ctx, transferDecision(transport.DecisionRetry)); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("shared")}); err != nil {
		t.Fatal(err)
	}
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("overflow-delayed")}, now); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("aggregate delayed capacity error = %v", err)
	}
	for index, group := range []string{"subscriber:alpha", "subscriber:beta"} {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.dlqKey(topo), ID: fmt.Sprintf("%d-0", index+1), Values: map[string]any{
			"payload": "requeue", "reason": "test", "error": "test", "attempts": 1, "failed_at": 1,
			"source_stream": backend.streamKey(topo), "source_group": group, "source_id": "source",
		}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Requeue(ctx, topo, []string{"1-0", "2-0"}); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("aggregate ready capacity error = %v", err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 2 {
		t.Fatalf("capacity rejection changed DLQ: %d, %v", count, err)
	}
}

func TestSafeAndForcedSubscriberRetirement(t *testing.T) {
	t.Run("safe", func(t *testing.T) {
		backend, rc := stoppedRetentionBackend(t)
		ctx := context.Background()
		topo := targetedEventTopology("retention", "retire-safe")
		ensureTargetedEventTopology(t, backend, topo)
		sub := transport.Subscription{Topology: topo, Group: "old"}
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
		attempt, err := backend.RetireSubscription(ctx, sub, false)
		if !errors.Is(err, ErrSubscriptionNotDrained) || attempt.Unread != 3 || attempt.Retired {
			t.Fatalf("attempt=%+v err=%v", attempt, err)
		}
		if err := backend.EnsureSubscription(ctx, sub); !errors.Is(err, ErrSubscriptionRetiring) {
			t.Fatalf("restart retirement error=%v", err)
		}
		result, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "subscriber:old", Consumer: "drain", Streams: []string{backend.streamKey(topo), ">"}, Count: 10}).Result()
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, len(result[0].Messages))
		for i := range result[0].Messages {
			ids[i] = result[0].Messages[i].ID
		}
		if err := rc.XAck(ctx, backend.streamKey(topo), "subscriber:old", ids...).Err(); err != nil {
			t.Fatal(err)
		}
		completed, err := backend.RetireSubscription(ctx, sub, false)
		if err != nil || !completed.Retired {
			t.Fatalf("completed=%+v err=%v", completed, err)
		}
	})

	t.Run("forced reports abandoned work", func(t *testing.T) {
		backend, rc := stoppedRetentionBackend(t)
		ctx := context.Background()
		topo := targetedEventTopology("retention", "retire-force")
		ensureTargetedEventTopology(t, backend, topo)
		sub := transport.Subscription{Topology: topo, Group: "old"}
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("x")}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{Group: "subscriber:old", Consumer: "reader", Streams: []string{backend.streamKey(topo), ">"}, Count: 1}).Result(); err != nil {
			t.Fatal(err)
		}
		completed, err := backend.RetireSubscription(ctx, sub, true)
		if err != nil || !completed.Retired || completed.Pending != 1 || completed.Unread != 2 {
			t.Fatalf("forced=%+v err=%v", completed, err)
		}
	})
}

func TestSubscriberRetirementIncludesPrivateRetryLanes(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "private-retire")
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "old"}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	group := backend.groupName(sub)
	if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, group), goredis.Z{Score: 1, Member: "delayed"}).Err(); err != nil {
		t.Fatal(err)
	}
	attempt, err := backend.RetireSubscription(ctx, sub, false)
	if !errors.Is(err, ErrSubscriptionNotDrained) || attempt.Unread != 1 {
		t.Fatalf("safe retirement = %+v, %v", attempt, err)
	}
	completed, err := backend.RetireSubscription(ctx, sub, true)
	if err != nil || !completed.Retired || completed.Unread != 1 {
		t.Fatalf("forced retirement = %+v, %v", completed, err)
	}
	if count, err := rc.Exists(ctx, backend.subscriberRetryStreamKey(topo, group), backend.subscriberRetryDelayedKey(topo, group)).Result(); err != nil || count != 0 {
		t.Fatalf("private lanes retained after retirement: %d, %v", count, err)
	}
}

func TestRetiredSubscriberDLQRequeueCannotRecreateRetryLane(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "retire-requeue-fence")
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "old"}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	group := backend.groupName(sub)
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.dlqKey(topo), ID: "1-0", Values: map[string]any{
		"payload": "requeue", "reason": "test", "error": "test", "attempts": 1, "failed_at": 1,
		"source_stream": backend.streamKey(topo), "source_group": group, "source_id": "source",
	}}).Err(); err != nil {
		t.Fatal(err)
	}
	retired, err := backend.RetireSubscription(ctx, sub, true)
	if err != nil || !retired.Retired {
		t.Fatalf("retirement = %+v, %v", retired, err)
	}
	if err := backend.Requeue(ctx, topo, []string{"1-0"}); !errors.Is(err, ErrSubscriptionRetiring) {
		t.Fatalf("requeue after retirement = %v", err)
	}
	if exists, err := rc.Exists(ctx, backend.subscriberRetryStreamKey(topo, group)).Result(); err != nil || exists != 0 {
		t.Fatalf("orphan retry lane recreated: %d, %v", exists, err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 1 {
		t.Fatalf("fenced DLQ source changed: %d, %v", count, err)
	}
}

func TestRetentionReportIncludesPrivateRetryLanes(t *testing.T) {
	backend, rc := stoppedRetentionBackend(t)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "private-report")
	ensureTargetedEventTopology(t, backend, topo)
	sub := transport.Subscription{Topology: topo, Group: "alpha"}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	group := backend.groupName(sub)
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.subscriberRetryStreamKey(topo, group), Values: map[string]any{"payload": "ready"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, group), goredis.Z{Score: 1, Member: "delayed"}).Err(); err != nil {
		t.Fatal(err)
	}
	report, err := backend.RetentionReport(ctx, topo)
	if err != nil || report.ReadyEntries != 1 || report.DelayedEntries != 1 {
		t.Fatalf("report = %+v, %v", report, err)
	}
}

func TestImmediatePublishDoesNotTrimAcceptedBacklog(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()
	backend := New(rc).(*redisTransport)
	ctx := context.Background()
	topo := targetedEventTopology("retention", "events")
	ensureTargetedEventTopology(t, backend, topo)
	stream := backend.streamKey(topo)
	for _, group := range []string{"subscriber:fast", "subscriber:slow"} {
		if err := rc.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
			t.Fatal(err)
		}
	}

	const oldBoundary = 100_000
	for i := 0; i < oldBoundary; i++ {
		if _, err := mr.XAdd(stream, "*", []string{"payload", strconv.Itoa(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("last")}); err != nil {
		t.Fatal(err)
	}
	if length, err := rc.XLen(ctx, stream).Result(); err != nil || length != oldBoundary+1 {
		t.Fatalf("ready stream length = %d, %v; accepted backlog was trimmed", length, err)
	}

	for _, group := range []string{"subscriber:fast", "subscriber:slow"} {
		streams, err := rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group: group, Consumer: "reader", Streams: []string{stream, ">"}, Count: oldBoundary + 1,
		}).Result()
		if err != nil || len(streams) != 1 || len(streams[0].Messages) != oldBoundary+1 {
			t.Fatalf("group %q observed %d records, streams=%d, err=%v", group, messageCount(streams), len(streams), err)
		}
	}
}

func messageCount(streams []goredis.XStream) int {
	if len(streams) == 0 {
		return 0
	}
	return len(streams[0].Messages)
}
