//go:build integration

package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func RunServerOnPort(t *testing.T, port int) *server.Server {
	t.Helper()
	opts := &server.Options{
		Port:      port,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := server.NewServer(opts)
	if err != nil {
		panic(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		panic("nats server failed to start")
	}
	return s
}

func TestNatsTransport_PublishAndConsume(t *testing.T) {
	s := RunServerOnPort(t, server.RANDOM_PORT)
	defer s.Shutdown()

	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	trans := New(js)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: "testjob_nats"}

	err = trans.EnsureTopology(ctx, topo)
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	// Publish
	err = trans.Publish(ctx, topo, transport.Message{
		ID:      "msg-1",
		Payload: []byte("hello nats"),
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
		if string(msg.Payload) != "hello nats" {
			t.Fatalf("Expected payload, got %s", msg.Payload)
		}
		// Ack
		err = d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for message")
	}

	trans.Close(ctx)
}

func TestNatsTransport_Delay(t *testing.T) {
	s := RunServerOnPort(t, server.RANDOM_PORT)
	defer s.Shutdown()

	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	trans := New(js)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_nats_delay_%d", time.Now().UnixNano())}

	trans.EnsureTopology(ctx, topo)

	payload := `{"hello":"delayed"}`

	err = trans.PublishAt(ctx, topo, transport.Message{
		ID:      "delay-msg-1",
		Payload: []byte(payload),
	}, time.Now().Add(3*time.Second))

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
		if dur < 2*time.Second {
			t.Fatalf("Message received too early! dur=%v", dur)
		}
		// Ack
		d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
	case <-time.After(8 * time.Second):
		t.Fatalf("Timeout waiting for delayed message")
	}

	trans.Close(ctx)
}
