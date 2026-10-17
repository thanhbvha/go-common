//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewWithClientSnapshotAndCommitPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probe, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := probe.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	for _, interval := range []time.Duration{0, time.Second} {
		cfg := commonkafka.DefaultConfig()
		cfg.ConsumerCommitInterval = interval
		wrapper := commonkafka.New(cfg)
		if err := wrapper.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		defer wrapper.Close()
		cfg.Brokers[0] = "mutated.invalid:9092"
		backend, err := NewWithClient(wrapper)
		if interval == 0 {
			if backend != nil || !errors.Is(err, transport.ErrNotSupported) {
				t.Fatalf("manual policy error = %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		consumer, err := backend.(*kafkaTransport).newConsumer(kgo.ConsumerGroup("snapshot-review"), kgo.AutoCommitMarks())
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		for _, client := range []*kgo.Client{wrapper.Native(), consumer} {
			brokers := client.OptValue(kgo.SeedBrokers).([]string)
			if len(brokers) != 1 || brokers[0] != "localhost:9092" {
				t.Fatal("client snapshots diverged")
			}
		}
	}
}

func TestKafkaTransport_PublishAndConsume(t *testing.T) {
	// Create base kgo.Client
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	// Need a quick ping to see if kafka is available
	if err := cl.Ping(context.Background()); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	trans := New(cl, []string{"localhost:9092"})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_kafka_%d", time.Now().UnixNano())}

	err = trans.EnsureTopology(ctx, topo)
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	// Wait a moment for topic creation to propagate
	time.Sleep(1 * time.Second)

	// Publish
	err = trans.Publish(ctx, topo, transport.Message{
		Payload: []byte("hello kafka"),
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
		if string(msg.Payload) != "hello kafka" {
			t.Fatalf("Expected payload, got %s", msg.Payload)
		}
		// Ack
		err = d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("Timeout waiting for message")
	}

	trans.Close(ctx)
}

func TestKafkaTransport_Delay(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	if err := cl.Ping(context.Background()); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	trans := New(cl, []string{"localhost:9092"})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_delay_%d", time.Now().UnixNano())}

	trans.EnsureTopology(ctx, topo)
	time.Sleep(1 * time.Second)

	// Since Envelope is parsed in the delay consumer to find RunAt, we must serialize a proper Envelope
	// But kafkaTransport doesn't use the JSON's RunAt directly for Pause!
	// Wait, in `kafka.go`, delayConsumer does: `if err := json.Unmarshal(record.Value, &env); err != nil { ... cl.MarkCommitRecords }`
	// It relies on `eventbus.Envelope`! So we MUST publish an `eventbus.Envelope` JSON.

	payload := `{"id":"123","type":"testjob_kafka_delay","payload":"\"hello\"","run_at":` + fmt.Sprintf("%d", time.Now().Add(3*time.Second).UnixMilli()) + `}`

	err = trans.PublishAt(ctx, topo, transport.Message{
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

func TestKafkaTransport_PartitionProcessing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	backend := New(cl, nil).(*kafkaTransport)
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("partition_%d", time.Now().UnixNano())}
	responses, err := backend.adm.CreateTopics(ctx, 2, 1, nil, topo.Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range responses {
		if response.Err != nil {
			t.Fatal(response.Err)
		}
	}
	defer backend.adm.DeleteTopics(context.Background(), topo.Name())
	for partition := int32(0); partition < 2; partition++ {
		for i := 0; i < 2; i++ {
			if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topo.Name(), Partition: partition, Value: []byte(fmt.Sprintf("p%d-%d", partition, i))}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	consumeCtx, stop := context.WithCancel(ctx)
	received := make(chan *kafkaDelivery, 4)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(consumeCtx, transport.Subscription{Topology: topo, Concurrency: 2, Prefetch: 2}, func(d transport.Delivery) { received <- d.(*kafkaDelivery) })
	}()
	defer func() { stop(); <-done }()
	receive := func() *kafkaDelivery {
		t.Helper()
		select {
		case delivery := <-received:
			return delivery
		case <-ctx.Done():
			t.Fatal("timed out waiting for partition delivery")
			return nil
		}
	}
	counts := make(map[int32]int)
	for range 4 {
		delivery := receive()
		partition := delivery.record.Partition
		want := fmt.Sprintf("p%d-%d", partition, counts[partition])
		if string(delivery.Message().Payload) != want {
			t.Fatal("partition processing order violated")
		}
		counts[partition]++
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
	}
	if counts[0] != 2 || counts[1] != 2 {
		t.Fatal("not all partitions processed")
	}
}
