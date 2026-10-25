//go:build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaDelaySchedulerRejectsRelatedPartitionMismatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	backend := New(producer, nil).(*kafkaTransport)
	topo := transport.Topology{Namespace: "p56", JobType: fmt.Sprintf("partition-mismatch-%d", time.Now().UnixNano())}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	topics = append(topics, backend.dlqAdminLockTopic(topo))
	created, err := backend.adm.CreateTopics(ctx, 2, 1, nil, backend.mainTopic(topo))
	if err != nil {
		t.Fatal(err)
	}
	if result := created[backend.mainTopic(topo)]; result.Err != nil {
		t.Fatal(result.Err)
	}
	defer backend.adm.DeleteTopics(context.Background(), topics...)
	if err := backend.EnsureTopology(ctx, topo); err == nil {
		t.Fatal("EnsureTopology accepted inconsistent related partition counts")
	}
	backend.mu.Lock()
	_, started := backend.sweepers[topo.Name()]
	backend.mu.Unlock()
	if started {
		t.Fatal("scheduler started for an invalid topology")
	}
}

func TestKafkaDelaySchedulerBoundedBurstAndOrderingKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	backend := New(producer, nil).(*kafkaTransport)
	topo := transport.Topology{
		Namespace: "p56",
		JobType:   fmt.Sprintf("bounded-delay-%d", time.Now().UnixNano()),
		Options: map[string]any{
			"kafka.partitions":       int32(2),
			"kafka.delay_batch_size": 2,
		},
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = backend.Close(context.Background())
		topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
		topics = append(topics, backend.dlqAdminLockTopic(topo))
		_, _ = backend.adm.DeleteTopics(context.Background(), topics...)
	})
	runAt := time.Now().Add(1500 * time.Millisecond)
	for i := range 5 {
		payload := []byte(fmt.Sprintf(`{"v":3,"id":"delayed-%d","type":"task","attempt":1,"max_attempts":1,"run_at":%d,"ordering_key":"account-7","headers":{"traceparent":"trace-%d"}}`, i, runAt.UnixMilli(), i))
		if err := backend.PublishAt(ctx, topo, transport.Message{
			Payload: payload, OrderingKey: "account-7", Headers: map[string]string{"traceparent": fmt.Sprintf("trace-%d", i)},
		}, runAt); err != nil {
			t.Fatal(err)
		}
	}

	consumer, err := backend.newManualConsumer(
		kgo.ConsumerGroup(topo.Name()+"-verification"),
		kgo.ConsumeTopics(backend.mainTopic(topo)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.CloseAllowingRebalance()
	seen := make(map[string]struct{}, 5)
	partition := int32(-1)
	for len(seen) < 5 {
		fetches := consumer.PollRecords(ctx, 5-len(seen))
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetches.Records() {
			if string(record.Key) != "account-7" {
				t.Fatalf("ordering key = %q", record.Key)
			}
			if partition == -1 {
				partition = record.Partition
			} else if record.Partition != partition {
				t.Fatalf("same ordering key reached partitions %d and %d", partition, record.Partition)
			}
			seen[string(record.Value)] = struct{}{}
		}
	}
	if elapsed := time.Since(runAt); elapsed > 8*time.Second {
		// A full bounded batch triggers a fast follow-up pass. The records remain
		// durable in Kafka, but a backlog no longer waits five seconds per batch.
		t.Fatalf("bounded burst drained too slowly: %v after deadline", elapsed)
	}
}
