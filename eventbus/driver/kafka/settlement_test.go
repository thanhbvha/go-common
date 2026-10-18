package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type settlementProducer func(context.Context, *kgo.Record) error

func (p settlementProducer) ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: p(ctx, record)}
	}
	return results
}

type settlementCommitter func(context.Context, ...*kgo.Record) error

func (c settlementCommitter) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c(ctx, records...)
}

func settlementDecision(kind transport.DecisionKind) transport.Decision {
	source := transport.Message{ID: "source-id", Payload: []byte(`{"v":3,"attempt":1}`), OrderingKey: "customer", Headers: map[string]string{"correlation-id": "synthetic-correlation"}}
	next := source
	next.Payload = []byte(`{"v":3,"attempt":2}`)
	return transport.Decision{Kind: kind, Next: next, Source: &source}
}

func TestKafkaSettlementPublishBeforeCommit(t *testing.T) {
	// DS-40..44: no source commit before confirmed destination production;
	// errors reopen the guard, and only confirmed commits complete settlement.
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		for _, failure := range []string{"none", "produce", "commit", "ambiguous produce"} {
			t.Run(fmt.Sprint(kind)+"/"+failure, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					injected := errors.New("injected transport failure")
					var events []string
					writes, commits := 0, 0
					fail := true
					backend := &kafkaTransport{producer: settlementProducer(func(_ context.Context, record *kgo.Record) error {
						events = append(events, "produce")
						if fail && failure == "produce" {
							return injected
						}
						writes++
						if fail && failure == "ambiguous produce" {
							return injected // broker wrote, response was lost
						}
						return nil
					})}
					coordinator := newOffsetCoordinator(settlementCommitter(func(_ context.Context, records ...*kgo.Record) error {
						events = append(events, "commit")
						commits++
						if fail && failure == "commit" {
							return injected
						}
						return nil
					}), 1)
					defer coordinator.close()
					record := processingRecord(0, 7)
					if err := coordinator.register(ctx, []*kgo.Record{record}); err != nil {
						t.Fatal(err)
					}
					delivery := &kafkaDelivery{transport: backend, coordinator: coordinator, record: record}
					decision := settlementDecision(kind)
					err := delivery.Settle(ctx, decision)
					if failure == "none" {
						if err != nil {
							t.Fatal(err)
						}
					} else {
						if !errors.Is(err, injected) {
							t.Fatalf("error = %v", err)
						}
						if failure != "commit" && commits != 0 {
							t.Fatal("source committed after failed publish")
						}
						fail = false
						if err := delivery.Settle(ctx, decision); err != nil {
							t.Fatal(err)
						}
					}
					wantWrites := 1
					if failure == "commit" || failure == "ambiguous produce" {
						wantWrites = 2
					}
					if writes != wantWrites {
						t.Fatalf("destination writes = %d, want %d", writes, wantWrites)
					}
					if events[len(events)-1] != "commit" {
						t.Fatalf("settlement sequence = %v", events)
					}
					before := len(events)
					if err := delivery.Settle(ctx, decision); !errors.Is(err, transport.ErrDeliverySettled) {
						t.Fatalf("double settlement = %v", err)
					}
					if len(events) != before {
						t.Fatal("double settlement performed broker work")
					}
				})
			})
		}
	}
}

func TestKafkaSettlementWaitsForBothConfirmations(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				published, committed := make(chan struct{}), make(chan struct{})
				client := &processingConsumer{}
				coordinator := newOffsetCoordinator(settlementCommitter(func(ctx context.Context, records ...*kgo.Record) error {
					<-committed
					return client.CommitRecords(ctx, records...)
				}), 1)
				defer coordinator.close()
				record := processingRecord(0, 7)
				if err := coordinator.register(context.Background(), []*kgo.Record{record}); err != nil {
					t.Fatal(err)
				}
				backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { <-published; return nil })}
				delivery := &kafkaDelivery{transport: backend, coordinator: coordinator, record: record}
				done := make(chan error, 1)
				go func() { done <- delivery.Settle(context.Background(), settlementDecision(kind)) }()
				synctest.Wait()
				if client.committedCount() != 0 {
					t.Fatal("committed before produce confirmation")
				}
				close(published)
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("completed before commit confirmation")
				default:
				}
				close(committed)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestKafkaSettlementRoutingAndMetadata(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		topo := transport.Topology{Namespace: "settlement", JobType: "task"}
		for _, scenario := range []string{"retry immediate", "retry delayed", "dlq normalized", "dlq raw"} {
			decision := settlementDecision(transport.DecisionRetry)
			backend := &kafkaTransport{}
			wantTopic, wantMsg := backend.mainTopic(topo), decision.Next
			switch scenario {
			case "retry delayed":
				decision.RetryAt = time.Now().Add(time.Minute)
				wantTopic = backend.delayTopic(topo, time.Minute)
			case "dlq normalized", "dlq raw":
				decision.Kind = transport.DecisionDeadLetter
				wantTopic, wantMsg = backend.dlqTopic(topo), *decision.Source
				if scenario == "dlq raw" {
					decision.Source = nil
				}
			}
			backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
				want := &kgo.Record{Topic: wantTopic, Key: []byte(wantMsg.OrderingKey), Value: wantMsg.Payload, Headers: []kgo.RecordHeader{{Key: "correlation-id", Value: []byte("synthetic-correlation")}}}
				if !reflect.DeepEqual(record, want) {
					t.Fatalf("%s: record = %+v", scenario, record)
				}
				return nil
			})
			coordinator := newOffsetCoordinator(&processingConsumer{}, 1)
			record := processingRecord(0, 1)
			if err := coordinator.register(context.Background(), []*kgo.Record{record}); err != nil {
				t.Fatal(err)
			}
			delivery := &kafkaDelivery{transport: backend, topo: topo, coordinator: coordinator, record: record, msg: wantMsg}
			if err := delivery.Settle(context.Background(), decision); err != nil {
				t.Fatal(err)
			}
			coordinator.close()
		}
	})
}

func TestKafkaSettlementRejectsUnsafeProducer(t *testing.T) {
	for _, scenario := range []string{"no ack", "transactional", "leader ack", "all ack"} {
		t.Run(scenario, func(t *testing.T) {
			opts := []kgo.Opt{}
			switch scenario {
			case "no ack":
				opts = append(opts, kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.NoAck()))
			case "transactional":
				opts = append(opts, kgo.TransactionalID("synthetic-transaction"))
			case "leader ack":
				opts = append(opts, kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.LeaderAck()))
			}
			client, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			backend := New(client, nil)
			err = backend.(transport.TopologyValidator).ValidateTopology(transport.Topology{JobType: "task"})
			unsafe := scenario == "no ack" || scenario == "transactional"
			if unsafe != errors.Is(err, transport.ErrNotSupported) {
				t.Fatalf("validation = %v", err)
			}
			if unsafe {
				if err := backend.Publish(context.Background(), transport.Topology{}, transport.Message{}); !errors.Is(err, transport.ErrNotSupported) {
					t.Fatalf("publish = %v", err)
				}
			}
		})
	}
	cfg := commonkafka.DefaultConfig()
	cfg.ProducerIdempotent, cfg.ProducerAcks = false, 0
	if _, err := NewWithConfig(cfg); !errors.Is(err, transport.ErrNotSupported) {
		t.Fatalf("config = %v", err)
	}
}

func TestKafkaFailedTransferBlocksHigherCommit(t *testing.T) {
	// DS-41/51: even a successful later handler cannot skip a failed transfer.
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				client := &processingConsumer{}
				coordinator := newOffsetCoordinator(client, 2)
				defer coordinator.close()
				low, high := processingRecord(0, 10), processingRecord(0, 12)
				if err := coordinator.register(ctx, []*kgo.Record{low, high}); err != nil {
					t.Fatal(err)
				}
				backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { return errors.New("destination unavailable") })}
				delivery := &kafkaDelivery{transport: backend, coordinator: coordinator, record: low}
				if err := delivery.Settle(ctx, settlementDecision(kind)); err == nil {
					t.Fatal("failed produce succeeded")
				}
				done := make(chan error, 1)
				go func() { done <- coordinator.settle(ctx, high) }()
				synctest.Wait()
				if client.committedCount() != 0 {
					t.Fatal("higher commit skipped the failed transfer")
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("higher settlement = %v", err)
				}
			})
		})
	}
}

func TestKafkaSettlementCancellationAfterPublish(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				client := &processingConsumer{}
				coordinator := newOffsetCoordinator(client, 1)
				defer coordinator.close()
				record := processingRecord(0, 1)
				if err := coordinator.register(ctx, []*kgo.Record{record}); err != nil {
					t.Fatal(err)
				}
				backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { cancel(); return nil })}
				delivery := &kafkaDelivery{transport: backend, coordinator: coordinator, record: record}
				if err := delivery.Settle(ctx, settlementDecision(kind)); !errors.Is(err, context.Canceled) {
					t.Fatalf("settlement = %v", err)
				}
				// Barrier: any queued cancellation must be observed before inspection.
				if err := coordinator.register(context.Background(), nil); err != nil {
					t.Fatal(err)
				}
				if client.committedCount() != 0 {
					t.Fatal("canceled settlement committed the source")
				}
			})
		})
	}
}
