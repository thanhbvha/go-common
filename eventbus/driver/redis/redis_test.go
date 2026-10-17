package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

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

	go func() {
		err := trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Consume error: %v", err)
		}
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

	// Close transport to stop sweepers
	trans.Close(ctx)
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
