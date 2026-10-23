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
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestDelayDisabledTopologyDoesNotStartSweeper(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	backend := New(rc).(*redisTransport)
	t.Cleanup(func() {
		_ = backend.Close(context.Background())
		_ = rc.Close()
	})
	topo := transport.Topology{Namespace: "immediate", JobType: "task", Kind: transport.TaskTopology, Options: map[string]any{"eventbus.delay_disabled": true}}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	_, exists := backend.sweepers[topologyPrefix(topo)]
	backend.mu.Unlock()
	if exists {
		t.Fatal("delay-disabled topology started a sweeper")
	}
	if err := backend.PublishAt(context.Background(), topo, transport.Message{Payload: []byte("future")}, time.Now().Add(time.Hour)); !errors.Is(err, transport.ErrNotSupported) {
		t.Fatalf("PublishAt = %v", err)
	}
	if mr.Exists(backend.delayedKey(topo)) {
		t.Fatal("delay-disabled topology created delayed storage")
	}
}

func TestEnsureTopologyReconcilesMissingAggregateCapacityCounters(t *testing.T) {
	mr := miniredis.RunT(t)
	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rc.Close() })
	backend := New(rc).(*redisTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "upgrade", JobType: "aggregate-counters", Kind: transport.EventTopology, Options: map[string]any{
		"redis.max_ready_entries": int64(5), "redis.max_delayed_entries": int64(4), redisEventLayoutOption: redisTargetedLayoutV1,
	}}
	groups := []string{"subscriber:alpha", "subscriber:beta"}
	if err := rc.HSet(ctx, backend.metadataKey(topo), "retention_epoch", 2).Err(); err != nil {
		t.Fatal(err)
	}
	for _, group := range groups {
		if err := rc.HSet(ctx, backend.groupManifestKey(topo), group, groupStateActive).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for index := range 2 {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.streamKey(topo), Values: map[string]any{"payload": fmt.Sprintf("shared-%d", index)}}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	for index, group := range groups {
		if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.subscriberRetryStreamKey(topo, group), Values: map[string]any{"payload": group}}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, group), goredis.Z{Score: float64(index), Member: group}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := rc.ZAdd(ctx, backend.delayedKey(topo), goredis.Z{Score: 0, Member: "shared-delayed"}).Err(); err != nil {
		t.Fatal(err)
	}

	hook := newCapacityInitPauseHook()
	rc.AddHook(hook)
	ensureDone := make(chan error, 1)
	go func() { ensureDone <- backend.EnsureTopology(ctx, topo) }()
	select {
	case <-hook.entered:
	case <-time.After(time.Second):
		t.Fatal("capacity initialization did not reach manifest scan")
	}
	newGroup := "subscriber:gamma"
	if err := rc.HSet(ctx, backend.groupManifestKey(topo), newGroup, groupStateActive).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.HIncrBy(ctx, backend.metadataKey(topo), "retention_epoch", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.subscriberRetryStreamKey(topo, newGroup), Values: map[string]any{"payload": newGroup}}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rc.ZAdd(ctx, backend.subscriberRetryDelayedKey(topo, newGroup), goredis.Z{Score: 3, Member: newGroup}).Err(); err != nil {
		t.Fatal(err)
	}

	admissionCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	err := backend.Publish(admissionCtx, topo, transport.Message{Payload: []byte("must-wait")})
	cancel()
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "capacity counters are initializing") {
		t.Fatalf("concurrent admission error = %v", err)
	}
	close(hook.release)
	if err := <-ensureDone; err != nil {
		t.Fatal(err)
	}

	ready, err := rc.HGet(ctx, backend.metadataKey(topo), "ready_entries").Int64()
	if err != nil || ready != 5 {
		t.Fatalf("ready aggregate = %d, %v", ready, err)
	}
	delayed, err := rc.HGet(ctx, backend.metadataKey(topo), "delayed_entries").Int64()
	if err != nil || delayed != 4 {
		t.Fatalf("delayed aggregate = %d, %v", delayed, err)
	}
	if scans := hook.scans.Load(); scans < 2 {
		t.Fatalf("manifest change did not force reconciliation retry: scans=%d", scans)
	}
	var capacityErr *transport.CapacityError
	if err := backend.Publish(ctx, topo, transport.Message{Payload: []byte("over-ready-limit")}); !errors.As(err, &capacityErr) {
		t.Fatalf("ready admission error = %v", err)
	}
	capacityErr = nil
	if err := backend.PublishAt(ctx, topo, transport.Message{Payload: []byte("over-delayed-limit")}, time.Now()); !errors.As(err, &capacityErr) {
		t.Fatalf("delayed admission error = %v", err)
	}
}

type capacityInitPauseHook struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	scans   atomic.Int64
}

func newCapacityInitPauseHook() *capacityInitPauseHook {
	return &capacityInitPauseHook{entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *capacityInitPauseHook) DialHook(next goredis.DialHook) goredis.DialHook { return next }

func (h *capacityInitPauseHook) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook {
	return func(ctx context.Context, cmd goredis.Cmder) error {
		if strings.EqualFold(cmd.Name(), "hscan") {
			h.scans.Add(1)
			h.once.Do(func() {
				close(h.entered)
				select {
				case <-h.release:
				case <-ctx.Done():
				}
			})
		}
		return next(ctx, cmd)
	}
}

func (h *capacityInitPauseHook) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return next
}

func TestRedisTransport_PublishAndConsume(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()

	trans := New(rc)

	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: "testjob"}

	err = trans.EnsureTopology(ctx, topo)
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	// Publish
	err = trans.Publish(ctx, topo, transport.Message{
		ID:      "msg-1",
		Type:    "testjob",
		Payload: []byte(`{"hello":"world"}`),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Consume
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan transport.Delivery)
	consumeDone := make(chan error, 1)

	go func() {
		consumeDone <- trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
	}()

	select {
	case d := <-received:
		msg := d.Message()
		if string(msg.Payload) != `{"hello":"world"}` {
			t.Fatalf("Expected payload, got %s", msg.Payload)
		}
		// Ack
		err = d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timeout waiting for message")
	}

	cancel()
	select {
	case err := <-consumeDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Consume error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not stop after cancellation")
	}
	// Close transport to stop sweepers
	if err := trans.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRedisTransport_Delay(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	defer mr.Close()

	rc := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	defer rc.Close()

	trans := New(rc)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: "delayjob"}

	trans.EnsureTopology(ctx, topo)

	// Publish with delay
	runAt := time.Now().Add(500 * time.Millisecond)
	err = trans.PublishAt(ctx, topo, transport.Message{
		Payload: []byte("delayed-payload"),
	}, runAt)

	if err != nil {
		t.Fatalf("PublishAt: %v", err)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan transport.Delivery)

	go func() {
		_ = trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
	}()

	start := time.Now()
	select {
	case d := <-received:
		dur := time.Since(start)
		if dur < 400*time.Millisecond {
			t.Fatalf("Message received too early! dur=%v", dur)
		}
		if string(d.Message().Payload) != "delayed-payload" {
			t.Fatalf("Wrong payload")
		}
		d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
	case <-time.After(3 * time.Second):
		t.Fatalf("Timeout waiting for delayed message")
	}

	trans.Close(ctx)
}
