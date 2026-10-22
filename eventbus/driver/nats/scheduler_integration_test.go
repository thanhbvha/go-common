//go:build integration

package nats

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestDelaySchedulerBoundsBurstAndUsesCollisionFreeKeys(t *testing.T) {
	js, backend, topo, kv, now := schedulerFixture(t, map[string]any{"nats.delay_batch_size": 7})
	ctx := context.Background()
	runAt := now.Add(5 * time.Second)
	for i := range 101 {
		msg := transport.Message{ID: "shared-logical-id", Payload: []byte(fmt.Sprintf("job-%d", i))}
		if err := backend.PublishAt(ctx, topo, msg, runAt); err != nil {
			t.Fatal(err)
		}
	}
	keys := scheduledKeys(t, kv)
	if len(keys) != 101 {
		t.Fatalf("scheduled records = %d, want 101", len(keys))
	}
	unique := make(map[string]bool, len(keys))
	for _, key := range keys {
		if unique[key] {
			t.Fatalf("duplicate schedule key %q", key)
		}
		unique[key] = true
	}

	dispatchAt := runAt.Add(time.Second)
	for attempts := 0; attempts < 30; attempts++ {
		published, err := backend.dispatchDue(ctx, topo, kv, dispatchAt)
		if err != nil {
			t.Fatal(err)
		}
		if published > 7 {
			t.Fatalf("published batch = %d, want <= 7", published)
		}
		info, err := js.StreamInfo(backend.streamName(topo))
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs == 101 {
			if len(scheduledKeys(t, kv)) != 0 {
				t.Fatal("published schedule records were not deleted")
			}
			return
		}
	}
	t.Fatal("bounded scheduler did not drain burst")
}

func TestDelaySchedulerBacklogDoesNotOccupyMainConsumerCapacity(t *testing.T) {
	js, backend, topo, _, now := schedulerFixture(t, nil)
	ctx := context.Background()
	for i := range 101 {
		if err := backend.PublishAt(ctx, topo, transport.Message{ID: fmt.Sprintf("delayed-%d", i), Payload: []byte("delayed")}, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	info, err := js.StreamInfo(backend.streamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("future backlog inserted %d records into main stream", info.State.Msgs)
	}
	if err := backend.Publish(ctx, topo, transport.Message{ID: "immediate", Payload: []byte("ready")}); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}
	if err := backend.reconcileConsumer(ctx, backend.streamName(topo), sub); err != nil {
		t.Fatal(err)
	}
	consumer, err := js.ConsumerInfo(backend.streamName(topo), backend.consumerName(sub))
	if err != nil {
		t.Fatal(err)
	}
	if consumer.Config.MaxAckPending != 2 || consumer.NumPending != 1 {
		t.Fatalf("main consumer capacity/pending = %d/%d, want 2/1", consumer.Config.MaxAckPending, consumer.NumPending)
	}
}

func TestDelaySchedulerCASAcrossInstances(t *testing.T) {
	js, first, topo, kv, now := schedulerFixture(t, map[string]any{"nats.delay_batch_size": 1000})
	ctx := context.Background()
	second := New(js).(*natsTransport)
	second.now = func() time.Time { return now }
	if err := second.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	runAt := now.Add(2 * time.Second)
	for i := range 101 {
		if err := first.PublishAt(ctx, topo, transport.Message{ID: fmt.Sprintf("job-%d", i), Payload: []byte("payload")}, runAt); err != nil {
			t.Fatal(err)
		}
	}
	dispatchAt := runAt.Add(time.Second)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, instance := range []*natsTransport{first, second} {
		wg.Add(1)
		go func(instance *natsTransport) {
			defer wg.Done()
			_, err := instance.dispatchDue(ctx, topo, kv, dispatchAt)
			errs <- err
		}(instance)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	info, err := js.StreamInfo(first.streamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 101 {
		t.Fatalf("multi-instance dispatch stored %d messages, want 101", info.State.Msgs)
	}
	if len(scheduledKeys(t, kv)) != 0 {
		t.Fatal("multi-instance dispatch left scheduled records")
	}
}

func TestDelaySchedulerRecoversExpiredLeaseAndPostPublishCrash(t *testing.T) {
	js, backend, topo, kv, now := schedulerFixture(t, map[string]any{"nats.delay_lease": 5 * time.Second})
	ctx := context.Background()
	runAt := now.Add(time.Second)
	if err := backend.PublishAt(ctx, topo, transport.Message{ID: "leased", Payload: []byte("payload")}, runAt); err != nil {
		t.Fatal(err)
	}
	key := scheduledKeys(t, kv)[0]
	entry, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	var record delayRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		t.Fatal(err)
	}
	record.Owner = "crashed-instance"
	record.LeaseUntil = runAt.Add(5 * time.Second).UnixMilli()
	claimed, _ := json.Marshal(record)
	if _, err := kv.Update(key, claimed, entry.Revision()); err != nil {
		t.Fatal(err)
	}
	if count, err := backend.dispatchDue(ctx, topo, kv, runAt.Add(time.Second)); err != nil || count != 0 {
		t.Fatalf("live lease dispatch = %d, %v", count, err)
	}
	if count, err := backend.dispatchDue(ctx, topo, kv, runAt.Add(6*time.Second)); err != nil || count != 1 {
		t.Fatalf("expired lease dispatch = %d, %v", count, err)
	}

	secondRunAt := runAt.Add(10 * time.Second)
	if err := backend.PublishAt(ctx, topo, transport.Message{ID: "post-publish", Payload: []byte("payload-2")}, secondRunAt); err != nil {
		t.Fatal(err)
	}
	key = scheduledKeys(t, kv)[0]
	entry, err = kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.publish(ctx, backend.streamName(topo), record.MessageID, "delay:"+record.ScheduleID, record.Payload); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, kv, secondRunAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	info, err := js.StreamInfo(backend.streamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("post-publish recovery stored %d messages, want 2", info.State.Msgs)
	}
}

func TestDelaySchedulerMigratesLegacyAndRecoversCursorAfterDowntime(t *testing.T) {
	js, backend, topo, kv, now := schedulerFixture(t, nil)
	ctx := context.Background()
	runAt := now.Add(2 * time.Second)
	legacyKey := fmt.Sprintf("%020d_%s", runAt.UnixMilli(), "legacy-id")
	if _, err := kv.Put(legacyKey, []byte("legacy-payload")); err != nil {
		t.Fatal(err)
	}
	// Model a long scheduler outage. Recovery streams live key metadata and
	// jumps over empty seconds instead of probing the entire gap.
	if _, err := kv.Put(delayCursorKey, []byte(fmt.Sprint(delaySlot(now.Add(-24*time.Hour))))); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, kv, runAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get(legacyKey); !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		t.Fatalf("legacy record was not removed: %v", err)
	}
	stored, err := js.GetMsg(backend.streamName(topo), 1)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Data) != "legacy-payload" || stored.Header.Get(messageIDHeader) != "legacy-id" {
		t.Fatal("legacy migration lost payload or message identity")
	}
}

func TestDelaySchedulerQuarantinesLegacyEventKeyWithUnknownProvenance(t *testing.T) {
	js, backend, topo, kv, now := eventSchedulerFixture(t, "legacy_unknown_provenance")
	runAt := now.Add(time.Second)
	legacyKey := fmt.Sprintf("%020d_%s", runAt.UnixMilli(), "legacy-event")
	if _, err := kv.Put(legacyKey, []byte("legacy-event-payload")); err != nil {
		t.Fatal(err)
	}
	if moved, err := backend.dispatchDue(context.Background(), topo, kv, runAt.Add(time.Second)); err != nil || moved != 0 {
		t.Fatalf("legacy event dispatch = %d, %v", moved, err)
	}
	if _, err := kv.Get(legacyKey); !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		t.Fatalf("legacy event source retained: %v", err)
	}
	hash := sha256.Sum256([]byte(legacyKey))
	if _, err := kv.Get(delayQuarantinePrefix + fmt.Sprintf("legacy-%x", hash[:])); err != nil {
		t.Fatal(err)
	}
	info, err := js.StreamInfo(backend.streamName(topo))
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("legacy event reached broadcast stream: %+v, %v", info, err)
	}
}

func TestDelaySchedulerV2CompatibilityAndLegacyEventRetryFailClosed(t *testing.T) {
	t.Run("ordinary event delay broadcasts", func(t *testing.T) {
		_, backend, topo, kv, now := eventSchedulerFixture(t, "ordinary")
		record := delayRecord{
			Version: legacyDelayRecordVersion, ScheduleID: "legacy-delay", MessageID: "ordinary",
			RunAt: now.UnixMilli(), Payload: []byte("ordinary"),
		}
		putDelayRecord(t, kv, now, record)
		if moved, err := backend.dispatchDue(context.Background(), topo, kv, now.Add(time.Second)); err != nil || moved != 1 {
			t.Fatalf("legacy ordinary dispatch = %d, %v", moved, err)
		}
	})

	t.Run("event retry without target", func(t *testing.T) {
		_, backend, topo, kv, now := eventSchedulerFixture(t, "retry")
		record := delayRecord{
			Version: legacyDelayRecordVersion, ScheduleID: "legacy-retry", MessageID: "retry",
			RunAt: now.UnixMilli(), Payload: []byte("retry"), TransferID: "legacy-transfer",
		}
		key := putDelayRecord(t, kv, now, record)
		if moved, err := backend.dispatchDue(context.Background(), topo, kv, now.Add(time.Second)); err != nil || moved != 0 {
			t.Fatalf("legacy retry dispatch = %d, %v", moved, err)
		}
		if _, err := kv.Get(key); !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
			t.Fatalf("legacy retry source retained: %v", err)
		}
		if _, err := kv.Get(delayQuarantinePrefix + record.ScheduleID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTargetedDelayQuarantinesStaleLaneAndDoesNotBlockSlot(t *testing.T) {
	js, backend, topo, kv, now := eventSchedulerFixture(t, "filter_changed")
	ctx := context.Background()
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := backend.consumerName(sub)
	record := delayRecord{
		Version: delayRecordVersion, ScheduleID: "targeted", MessageID: "targeted", RunAt: now.UnixMilli(),
		Payload: []byte("targeted"), TransferID: "targeted-transfer", Target: durable,
	}
	key := putDelayRecord(t, kv, now, record)
	ordinary := delayRecord{Version: delayRecordVersion, ScheduleID: "ordinary", MessageID: "ordinary", RunAt: now.UnixMilli(), Payload: []byte("ordinary")}
	putDelayRecord(t, kv, now, ordinary)
	if err := js.DeleteStream(backend.retryStreamName(topo, durable)); err != nil {
		t.Fatal(err)
	}
	if moved, err := backend.dispatchDue(ctx, topo, kv, now.Add(time.Second)); err != nil || moved != 1 {
		t.Fatalf("changed-filter dispatch = %d, %v", moved, err)
	}
	if _, err := kv.Get(key); !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		t.Fatalf("stale target source retained: %v", err)
	}
	if _, err := kv.Get(delayQuarantinePrefix + record.ScheduleID); err != nil {
		t.Fatal(err)
	}
	info, err := js.StreamInfo(backend.streamName(topo))
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("unrelated slot work = %+v, %v", info, err)
	}
}

func TestSubscriberStatsIncludeTargetedDelayedAndReadyWork(t *testing.T) {
	js, backend, topo, kv, now := eventSchedulerFixture(t, "stats")
	ctx := context.Background()
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := backend.consumerName(sub)
	record := delayRecord{
		Version: delayRecordVersion, ScheduleID: "stats-targeted", MessageID: "stats-targeted",
		RunAt: now.Add(time.Hour).UnixMilli(), Payload: []byte("targeted"), TransferID: "stats-transfer", Target: durable,
	}
	putDelayRecord(t, kv, now.Add(time.Hour), record)
	stats, err := backend.SubscriptionStats(ctx, sub)
	if err != nil || stats.Delayed != 1 || stats.Lag != 0 {
		t.Fatalf("subscriber delayed stats = %#v, %v", stats, err)
	}
	if _, err := backend.publishToStream(ctx, backend.retryStreamName(topo, durable), backend.retrySubject(topo, durable), "ready", "ready-stats", []byte("ready")); err != nil {
		t.Fatal(err)
	}
	stats, err = backend.SubscriptionStats(ctx, sub)
	if err != nil || stats.Delayed != 1 || stats.Pending != 1 || stats.Lag != 1 {
		t.Fatalf("subscriber ready stats = %#v, %v", stats, err)
	}
	info, err := js.StreamInfo(backend.streamName(topo))
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("targeted work leaked to shared stream = %+v, %v", info, err)
	}
}

func TestTopologyStatsAggregatePrivateRetryConsumersExactlyOnce(t *testing.T) {
	js, backend, topo, _, _ := eventSchedulerFixture(t, "topology_stats")
	ctx := context.Background()
	alpha := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	beta := transport.Subscription{Topology: topo, Group: "beta", Concurrency: 1, Prefetch: 1}
	for _, sub := range []transport.Subscription{alpha, beta} {
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
		durable := backend.consumerName(sub)
		if _, err := backend.publishToStream(ctx, backend.retryStreamName(topo, durable), backend.retrySubject(topo, durable), sub.Group, sub.Group, []byte(sub.Group)); err != nil {
			t.Fatal(err)
		}
	}
	durable := backend.consumerName(alpha)
	pull, err := js.PullSubscribe(backend.retrySubject(topo, durable), backend.retryConsumerName(topo, durable), gonats.ManualAck(), gonats.Bind(backend.retryStreamName(topo, durable), backend.retryConsumerName(topo, durable)))
	if err != nil {
		t.Fatal(err)
	}
	defer pull.Unsubscribe()
	msgs, err := pull.Fetch(1, gonats.MaxWait(time.Second))
	if err != nil || len(msgs) != 1 {
		t.Fatalf("fetch private retry = %d, %v", len(msgs), err)
	}
	stats, err := backend.Stats(ctx, topo)
	if err != nil || stats.Pending != 1 || stats.InFlight != 1 || stats.Lag != 2 {
		t.Fatalf("topology private retry stats = %#v, %v", stats, err)
	}
}

func TestTopologyStatsRepeatedLargeDelayedSnapshotsDoNotLeakGoroutines(t *testing.T) {
	_, backend, topo, kv, now := schedulerFixture(t, nil)
	ctx := context.Background()
	for i := range 300 {
		putDelayRecord(t, kv, now.Add(time.Hour), delayRecord{
			Version: delayRecordVersion, ScheduleID: fmt.Sprintf("stats-%d", i),
			MessageID: fmt.Sprintf("stats-%d", i), RunAt: now.Add(time.Hour).UnixMilli(),
			Payload: []byte("payload"), TransferID: fmt.Sprintf("transfer-%d", i),
		})
	}
	transient := delayRecord{
		Version: delayRecordVersion, ScheduleID: "stats-transient", MessageID: "stats-transient",
		RunAt: now.Add(time.Hour).UnixMilli(), Payload: []byte("payload"), TransferID: "transfer-transient",
	}
	transientKey := delayKey(delaySlot(now.Add(time.Hour)), transient.RunAt, transient.ScheduleID)
	transientData, err := json.Marshal(transient)
	if err != nil {
		t.Fatal(err)
	}
	writeCtx, stopWrites := context.WithCancel(ctx)
	writerDone := make(chan error, 1)
	go func() {
		for {
			select {
			case <-writeCtx.Done():
				writerDone <- nil
				return
			default:
			}
			if _, err := kv.Put(transientKey, transientData); err != nil {
				writerDone <- err
				return
			}
			if err := kv.Delete(transientKey); err != nil {
				writerDone <- err
				return
			}
		}
	}()
	writerRunning := true
	defer func() {
		if writerRunning {
			stopWrites()
			<-writerDone
		}
	}()
	runtime.GC()
	before := runtime.NumGoroutine()
	for range 25 {
		stats, err := backend.Stats(ctx, topo)
		if err != nil || stats.Delayed < 299 || stats.Delayed > 301 {
			t.Fatalf("stats = %#v, %v", stats, err)
		}
	}
	stopWrites()
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	writerRunning = false
	if stats, err := backend.Stats(ctx, topo); err != nil || stats.Delayed != 300 {
		t.Fatalf("settled stats = %#v, %v", stats, err)
	}
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	if growth := runtime.NumGoroutine() - before; growth > 3 {
		t.Fatalf("goroutine growth after repeated stats = %d, want <= 3", growth)
	}
}

func eventSchedulerFixture(t *testing.T, suffix string) (gonats.JetStreamContext, *natsTransport, transport.Topology, gonats.KeyValue, time.Time) {
	t.Helper()
	js, backend, topo, _, now := schedulerFixture(t, nil)
	topo.Namespace += "_" + suffix
	topo.Kind = transport.EventTopology
	if topo.Options == nil {
		topo.Options = make(map[string]any)
	}
	topo.Options[natsEventLayoutOption] = natsTargetedLayoutV1
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	newKV, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	return js, backend, topo, newKV, now
}

func putDelayRecord(t *testing.T, kv gonats.KeyValue, runAt time.Time, record delayRecord) string {
	t.Helper()
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	key := delayKey(delaySlot(runAt), record.RunAt, record.ScheduleID)
	if _, err := kv.Put(key, data); err != nil {
		t.Fatal(err)
	}
	return key
}

func TestNATSSchedulerHealthRecordsFailureAndRecovery(t *testing.T) {
	_, backend, topo, kv, now := schedulerFixture(t, nil)
	ctx := context.Background()
	if _, err := kv.Put(delayCursorKey, []byte("invalid")); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, kv, now); err == nil {
		t.Fatal("expected cursor failure")
	}
	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || !status[0].Degraded || status[0].Retries == 0 || status[0].LastErrorOperation != "poll" {
		t.Fatalf("failure status = %#v", status)
	}
	if _, err := kv.Put(delayCursorKey, []byte(fmt.Sprint(delaySlot(now)))); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.dispatchDue(ctx, topo, kv, now); err != nil {
		t.Fatal(err)
	}
	status = backend.SchedulerHealth(topo)
	if len(status) != 1 || status[0].Degraded || status[0].ConsecutiveErrors != 0 || status[0].LastSuccess.IsZero() {
		t.Fatalf("recovered status = %#v", status)
	}
}

func schedulerFixture(t *testing.T, options map[string]any) (gonats.JetStreamContext, *natsTransport, transport.Topology, gonats.KeyValue, time.Time) {
	t.Helper()
	s, js := provisioningJetStream(t)
	t.Cleanup(s.Shutdown)
	backend := New(js).(*natsTransport)
	now := time.Unix(1_900_000_000, 0).UTC()
	backend.now = func() time.Time { return now }
	topo := transport.Topology{Namespace: fmt.Sprintf("scheduler_%d", time.Now().UnixNano()), JobType: "job", Options: options}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	kv, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	return js, backend, topo, kv, now
}

func scheduledKeys(t *testing.T, kv gonats.KeyValue) []string {
	t.Helper()
	keys, err := kv.Keys()
	if err != nil && !errors.Is(err, gonats.ErrNoKeysFound) {
		t.Fatal(err)
	}
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, "v2.") {
			result = append(result, key)
		}
	}
	return result
}
