//go:build integration

package nats

import (
	"context"
	"errors"
	"fmt"
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
