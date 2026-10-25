//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaSettlementBrokerFaultWindows(t *testing.T) {
	probeCtx, probeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer probeCancel()
	producer, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Ping(probeCtx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		for _, failure := range []string{"produce", "produce response", "commit", "commit response"} {
			t.Run(fmt.Sprint(kind)+"/"+failure, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				backend := New(producer, nil).(*kafkaTransport)
				topo := transport.Topology{Namespace: "p55", JobType: fmt.Sprintf("settlement-%d", time.Now().UnixNano())}
				topic := backend.mainTopic(topo)
				destination := topic
				if kind == transport.DecisionDeadLetter {
					destination = backend.dlqTopic(topo)
				}
				topics := []string{topic}
				if destination != topic {
					topics = append(topics, destination)
				}
				created, err := backend.adm.CreateTopics(ctx, 1, 1, nil, topics...)
				if err != nil {
					t.Fatal(err)
				}
				for _, result := range created {
					if result.Err != nil {
						t.Fatal(result.Err)
					}
				}
				defer backend.adm.DeleteTopics(context.Background(), topics...)
				if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("source")}).FirstErr(); err != nil {
					t.Fatal(err)
				}
				group := topic + "-workers"
				consumer, err := backend.newManualConsumer(mainConsumerOptions([]string{topic}, group, topo)...)
				if err != nil {
					t.Fatal(err)
				}
				defer consumer.CloseAllowingRebalance()
				var records []*kgo.Record
				for len(records) == 0 {
					fetched := consumer.PollRecords(ctx, 1)
					if err := fetched.Err(); err != nil {
						t.Fatal(err)
					}
					records = fetched.Records()
				}
				injected := errors.New("injected boundary failure")
				fail := true
				backend.producer = settlementProducer(func(ctx context.Context, record *kgo.Record) error {
					if fail && failure == "produce" {
						return injected
					}
					if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
						return err
					}
					if fail && failure == "produce response" {
						return injected
					}
					return nil
				})
				coordinator := newOffsetCoordinator(settlementCommitter(func(ctx context.Context, records ...*kgo.Record) error {
					if fail && failure == "commit" {
						return injected
					}
					if err := consumer.CommitRecords(ctx, records...); err != nil {
						return err
					}
					if fail && failure == "commit response" {
						return injected
					}
					return nil
				}), 1)
				defer coordinator.close()
				if err := coordinator.register(ctx, records); err != nil {
					t.Fatal(err)
				}
				delivery := &kafkaDelivery{transport: backend, topo: topo, coordinator: coordinator, record: records[0]}
				decision := settlementDecision(kind)
				if err := delivery.Settle(ctx, decision); !errors.Is(err, injected) {
					t.Fatalf("settlement = %v", err)
				}
				checkOffset := func(want int64) {
					t.Helper()
					offsets, err := backend.adm.FetchOffsetsForTopics(ctx, group, topic)
					if err != nil {
						t.Fatal(err)
					}
					offset, ok := offsets.Lookup(topic, records[0].Partition)
					if !ok || offset.Err != nil || offset.At != want {
						t.Fatalf("group offset = %+v, found=%v, want %d", offset, ok, want)
					}
				}
				wantOffset := int64(-1)
				if failure == "commit response" {
					wantOffset = records[0].Offset + 1
				}
				checkOffset(wantOffset)
				fail = false
				if err := delivery.Settle(ctx, decision); err != nil {
					t.Fatal(err)
				}
				checkOffset(records[0].Offset + 1)
				// Inspect the actual append-only destination log. Ambiguous writes
				// and failed/ambiguous commits can duplicate the destination.
				ends, err := backend.adm.ListEndOffsets(ctx, destination)
				if err != nil {
					t.Fatal(err)
				}
				end, ok := ends.Lookup(destination, 0)
				wantEnd := int64(2)
				if failure == "produce" {
					wantEnd = 1
				}
				if destination == topic {
					wantEnd++
				} // original source
				if !ok || end.Err != nil || end.Offset != wantEnd {
					t.Fatalf("destination end = %+v, found=%v, want %d", end, ok, wantEnd)
				}
			})
		}
	}
}
