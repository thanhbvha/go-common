package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

type processingConsumer struct {
	mu      sync.Mutex
	marked  []*kgo.Record
	polls   []int
	records []*kgo.Record
}

func (c *processingConsumer) MarkCommitRecords(records ...*kgo.Record) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marked = append(c.marked, records...)
}

func (c *processingConsumer) PollRecords(ctx context.Context, limit int) kgo.Fetches {
	c.polls = append(c.polls, limit)
	if len(c.records) == 0 {
		<-ctx.Done()
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Err: ctx.Err()}}}}}}
	}
	n := min(limit, len(c.records))
	records := c.records[:n]
	c.records = c.records[n:]
	partitions := make(map[int32][]*kgo.Record)
	for _, record := range records {
		partitions[record.Partition] = append(partitions[record.Partition], record)
	}
	topic := kgo.FetchTopic{Topic: "jobs"}
	for partition, records := range partitions {
		topic.Partitions = append(topic.Partitions, kgo.FetchPartition{Partition: partition, Records: records})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{topic}}}
}

func processingRecord(partition int32, offset int64) *kgo.Record {
	return &kgo.Record{Topic: "jobs", Partition: partition, Offset: offset, Value: []byte(`{"v":3,"attempt":1}`)}
}

func TestPartitionProcessingOrderAndConcurrency(t *testing.T) {
	// DS-51/53: a slow lower offset blocks only its own partition.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &processingConsumer{}
		received := make(chan *kafkaDelivery, 4)
		done := make(chan error, 1)
		go func() {
			done <- (&kafkaTransport{}).dispatchBatch(ctx, transport.Topology{}, client,
				[]*kgo.Record{processingRecord(0, 10), processingRecord(0, 12), processingRecord(1, 30), processingRecord(1, 31)},
				func(d transport.Delivery) { received <- d.(*kafkaDelivery) })
		}()
		first, other := <-received, <-received
		if first.record.Partition != 0 || first.record.Offset != 10 || other.record.Partition != 1 {
			t.Fatal("incorrect initial partition heads")
		}
		synctest.Wait()
		if len(received) != 0 {
			t.Fatal("callback acceptance unlocked an unsettled partition")
		}
		if err := other.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		next := <-received
		if next.record.Partition != 1 || next.record.Offset != 31 {
			t.Fatal("other partition did not progress independently")
		}
		if err := next.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(received) != 0 {
			t.Fatal("slow partition advanced before settlement")
		}
		if err := first.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		next = <-received
		if next.record.Offset != 12 {
			t.Fatal("partition order changed across offset gap")
		}
		if err := next.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestPartitionProcessingBoundsPollingAndCancellation(t *testing.T) {
	// DS-71/73: no new batch while records are unsettled; cancellation stops intake.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		client := &processingConsumer{records: []*kgo.Record{processingRecord(0, 0), processingRecord(0, 1), processingRecord(1, 0), processingRecord(1, 1), processingRecord(2, 0)}}
		received := make(chan transport.Delivery, 5)
		done := make(chan error, 1)
		go func() {
			done <- (&kafkaTransport{}).consumeRecords(ctx, transport.Topology{}, client, 3, func(d transport.Delivery) { received <- d })
		}()
		synctest.Wait()
		if len(client.polls) != 1 || client.polls[0] != 3 || len(client.records) != 2 || len(received) != 2 {
			t.Fatal("unsettled batch was not bounded")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation = %v", err)
		}
		// Accepted deliveries can finish during drain, after intake has stopped.
		for len(received) > 0 {
			if err := (<-received).Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
				t.Fatal(err)
			}
		}
		if len(client.marked) != 2 {
			t.Fatal("late settlement did not retain its client")
		}
	})
}

func TestPartitionProcessingSettlementFailureStopsLaterOffsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &processingConsumer{}
		received := make(chan transport.Delivery, 2)
		done := make(chan error, 1)
		go func() {
			done <- (&kafkaTransport{}).dispatchBatch(context.Background(), transport.Topology{}, client,
				[]*kgo.Record{processingRecord(0, 0), processingRecord(0, 1)}, func(d transport.Delivery) { received <- d })
		}()
		delivery := <-received
		failed, cancel := context.WithCancel(context.Background())
		cancel()
		if err := delivery.Settle(failed, transport.Decision{Kind: transport.DecisionAck}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if len(client.marked) != 0 || len(received) != 0 {
			t.Fatal("failed settlement advanced the partition")
		}
	})
}

func TestSubscriptionCapacity(t *testing.T) {
	for _, tc := range []struct {
		sub  transport.Subscription
		want int
	}{
		{transport.Subscription{}, 30},
		{transport.Subscription{Concurrency: 2, Prefetch: 3}, 5},
	} {
		got, err := subscriptionCapacity(tc.sub)
		if err != nil || got != tc.want {
			t.Fatalf("capacity = %d, %v; want %d", got, err, tc.want)
		}
	}
	for _, sub := range []transport.Subscription{{Concurrency: -1}, {Prefetch: -1}, {Concurrency: int(^uint(0) >> 1), Prefetch: 1}} {
		if _, err := subscriptionCapacity(sub); err == nil {
			t.Fatal("invalid capacity accepted")
		}
	}
}

type engineProcessingTransport struct {
	*kafkaTransport
	client *processingConsumer
}

func (t *engineProcessingTransport) EnsureTopology(context.Context, transport.Topology) error {
	return nil
}
func (t *engineProcessingTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	capacity, err := subscriptionCapacity(sub)
	if err != nil {
		return err
	}
	err = t.consumeRecords(ctx, sub.Topology, t.client, capacity, deliver)
	if sub.WaitForHandlers != nil {
		sub.WaitForHandlers()
	}
	return err
}

func TestPartitionProcessingThroughEngineWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		codec := eventbus.NewCodec(nil)
		client := &processingConsumer{}
		positions := make(map[string]int)
		for i, record := range []*kgo.Record{processingRecord(0, 0), processingRecord(0, 1), processingRecord(1, 0), processingRecord(1, 1)} {
			data, env, err := codec.EncodeTopology(transport.TaskTopology, "jobs", i, 1)
			if err != nil {
				t.Fatal(err)
			}
			record.Value = data
			positions[env.ID] = i
			client.records = append(client.records, record)
		}
		backend := &engineProcessingTransport{kafkaTransport: &kafkaTransport{}, client: client}
		bus, err := eventbus.Open(eventbus.Config{Concurrency: 2, Prefetch: 2}, eventbus.WithTransport(backend))
		if err != nil {
			t.Fatal(err)
		}
		started := make(chan int, 4)
		gates := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
		if err := bus.Register("jobs", func(ctx context.Context, job eventbus.Job) error {
			i := positions[job.ID()]
			started <- i
			select {
			case <-gates[i]:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := bus.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		a, b := <-started, <-started
		if !((a == 0 && b == 2) || (a == 2 && b == 0)) {
			t.Fatal("engine did not run separate partition heads concurrently")
		}
		close(gates[2])
		if next := <-started; next != 3 {
			t.Fatalf("next handler = %d, want other partition's successor", next)
		}
		close(gates[3])
		synctest.Wait()
		if len(started) != 0 {
			t.Fatal("engine ran a later offset before slow handler finished")
		}
		close(gates[0])
		if next := <-started; next != 1 {
			t.Fatal("slow partition did not resume in order")
		}
		close(gates[1])
		synctest.Wait()
		if err := bus.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(client.marked) != 4 {
			t.Fatal("engine did not settle all records")
		}
	})
}
