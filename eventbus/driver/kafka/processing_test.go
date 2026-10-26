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
	commits [][]*kgo.Record
	polls   []int
	records []*kgo.Record
	allowed int
	rewinds []map[string]map[int32]kgo.EpochOffset
}

func (c *processingConsumer) AllowRebalance() {
	c.mu.Lock()
	c.allowed++
	c.mu.Unlock()
}

func (c *processingConsumer) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	c.mu.Lock()
	c.rewinds = append(c.rewinds, offsets)
	c.mu.Unlock()
}

func (c *processingConsumer) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits = append(c.commits, append([]*kgo.Record(nil), records...))
	return nil
}

func (c *processingConsumer) committedCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, records := range c.commits {
		count += len(records)
	}
	return count
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

func TestOffsetCoordinatorCommitsOnlyContiguousSettlements(t *testing.T) {
	// DS-41/51: a higher completed offset cannot advance the group past a
	// lower unsettled offset, while an independent partition can commit.
	synctest.Test(t, func(t *testing.T) {
		client := &processingConsumer{}
		coordinator := newOffsetCoordinator(client, 3)
		defer coordinator.close()
		low := processingRecord(0, 10)
		high := processingRecord(0, 12) // compacted logs may contain gaps.
		other := processingRecord(1, 30)
		if err := coordinator.register(context.Background(), []*kgo.Record{low, high, other}); err != nil {
			t.Fatal(err)
		}

		highDone := make(chan error, 1)
		go func() { highDone <- coordinator.settle(context.Background(), high) }()
		synctest.Wait()
		if client.committedCount() != 0 {
			t.Fatal("higher offset committed before the lower offset settled")
		}
		if err := coordinator.settle(context.Background(), other); err != nil {
			t.Fatal(err)
		}
		if client.committedCount() != 1 {
			t.Fatal("independent partition did not commit")
		}
		if err := coordinator.settle(context.Background(), low); err != nil {
			t.Fatal(err)
		}
		if err := <-highDone; err != nil {
			t.Fatal(err)
		}

		client.mu.Lock()
		defer client.mu.Unlock()
		if len(client.commits) != 2 || len(client.commits[1]) != 1 || client.commits[1][0] != high {
			t.Fatal("coordinator did not commit the highest contiguous record")
		}
	})
}

type blockingCommitter struct {
	started chan *kgo.Record
	release chan error
}

type panicCommitter struct {
	mu    sync.Mutex
	calls int
}

func (c *panicCommitter) CommitRecords(context.Context, ...*kgo.Record) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	panic("must not escape")
}

func TestOffsetCoordinatorPanicStopsCommitsAndSurfacesFailure(t *testing.T) {
	committer := &panicCommitter{}
	coordinator := newOffsetCoordinator(committer, 1)
	record := processingRecord(0, 1)
	if err := coordinator.register(context.Background(), []*kgo.Record{record}); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.settle(context.Background(), record); !errors.Is(err, transport.ErrRecoveredPanic) {
		t.Fatalf("settlement error = %v, want recovered panic", err)
	}
	if err := coordinator.register(context.Background(), []*kgo.Record{processingRecord(0, 2)}); !errors.Is(err, transport.ErrRecoveredPanic) {
		t.Fatalf("registration error = %v, want recovered panic", err)
	}
	coordinator.close()
	committer.mu.Lock()
	defer committer.mu.Unlock()
	if committer.calls != 1 {
		t.Fatalf("commit calls = %d, want 1", committer.calls)
	}
}

func (c *blockingCommitter) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	c.started <- records[len(records)-1]
	return <-c.release
}

func TestOffsetCoordinatorWaitsForBrokerConfirmation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		committer := &blockingCommitter{started: make(chan *kgo.Record, 1), release: make(chan error, 1)}
		coordinator := newOffsetCoordinator(committer, 1)
		defer coordinator.close()
		record := processingRecord(0, 1)
		if err := coordinator.register(context.Background(), []*kgo.Record{record}); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- coordinator.settle(context.Background(), record) }()
		if committed := <-committer.started; committed != record {
			t.Fatal("coordinator committed the wrong record")
		}
		synctest.Wait()
		if len(done) != 0 {
			t.Fatal("settlement completed before Kafka confirmed the commit")
		}
		committer.release <- nil
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestOffsetCoordinatorCommitFailureDoesNotAdvance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		committer := &blockingCommitter{started: make(chan *kgo.Record, 1), release: make(chan error, 1)}
		coordinator := newOffsetCoordinator(committer, 1)
		defer coordinator.close()
		record := processingRecord(0, 1)
		if err := coordinator.register(context.Background(), []*kgo.Record{record}); err != nil {
			t.Fatal(err)
		}
		commitErr := errors.New("commit failed")
		first := make(chan error, 1)
		go func() { first <- coordinator.settle(context.Background(), record) }()
		<-committer.started
		committer.release <- commitErr
		if err := <-first; !errors.Is(err, commitErr) {
			t.Fatalf("first settlement = %v, want commit failure", err)
		}

		second := make(chan error, 1)
		go func() { second <- coordinator.settle(context.Background(), record) }()
		if retried := <-committer.started; retried != record {
			t.Fatal("failed commit advanced past its source record")
		}
		committer.release <- nil
		if err := <-second; err != nil {
			t.Fatal(err)
		}
	})
}

func TestOffsetCoordinatorFencesRevokedAndLostPartitions(t *testing.T) {
	client := &processingConsumer{}
	coordinator := newOffsetCoordinator(client, 2)
	defer coordinator.close()
	coordinator.trackOwnership()
	assignment := map[string][]int32{"jobs": {0}}
	if err := coordinator.assign(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}

	revoked := processingRecord(0, 1)
	if err := coordinator.register(context.Background(), []*kgo.Record{revoked}); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.revoke(context.Background(), assignment, false); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.settle(context.Background(), revoked); !errors.Is(err, errPartitionRevoked) {
		t.Fatalf("revoked settlement = %v, want partition-revoked fence", err)
	}
	if client.committedCount() != 0 {
		t.Fatal("revoked partition committed a stale delivery")
	}
	if err := coordinator.register(context.Background(), []*kgo.Record{processingRecord(0, 2)}); !errors.Is(err, errPartitionRevoked) {
		t.Fatalf("registration after revoke = %v", err)
	}

	if err := coordinator.assign(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	lost := processingRecord(0, 3)
	if err := coordinator.register(context.Background(), []*kgo.Record{lost}); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.revoke(context.Background(), assignment, true); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.settle(context.Background(), lost); !errors.Is(err, errPartitionLost) {
		t.Fatalf("lost settlement = %v, want partition-lost fence", err)
	}
}

func TestRebalanceStopsNewPartitionIntakeAndDrainsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &processingConsumer{}
		coordinator := newOffsetCoordinator(client, 2)
		defer coordinator.close()
		rebalances := newRebalanceController()
		rebalances.bind(coordinator)
		assignment := map[string][]int32{"jobs": {0}}
		if err := rebalances.assigned(context.Background(), assignment); err != nil {
			t.Fatal(err)
		}

		received := make(chan transport.Delivery, 2)
		type dispatchResult struct {
			rewind map[string]map[int32]kgo.EpochOffset
			err    error
		}
		done := make(chan dispatchResult, 1)
		go func() {
			rewind, err := (&kafkaTransport{}).dispatchBatch(context.Background(), transport.Topology{}, "", coordinator, rebalances,
				[]*kgo.Record{processingRecord(0, 0), processingRecord(0, 1)}, func(d transport.Delivery) { received <- d })
			done <- dispatchResult{rewind: rewind, err: err}
		}()
		first := <-received
		rebalances.block()
		synctest.Wait()
		if len(received) != 0 {
			t.Fatal("rebalance signal admitted another record")
		}
		if err := first.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		result := <-done
		if !errors.Is(result.err, errRebalanceRequested) {
			t.Fatalf("dispatch result = %v, want rebalance drain", result.err)
		}
		if got := result.rewind["jobs"][0].Offset; got != 1 {
			t.Fatalf("rewind offset = %d, want first undispatched offset 1", got)
		}
		if len(received) != 0 || client.committedCount() != 1 {
			t.Fatal("rebalance did not drain exactly the in-flight record")
		}
		if err := rebalances.revoked(context.Background(), assignment, false); err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsumeAllowsRebalanceOnlyAfterSettlement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &processingConsumer{records: []*kgo.Record{processingRecord(0, 0)}}
		coordinator := newOffsetCoordinator(client, 1)
		defer coordinator.close()
		received := make(chan transport.Delivery, 1)
		done := make(chan error, 1)
		go func() {
			done <- (&kafkaTransport{}).consumeRecords(ctx, transport.Subscription{}, client, coordinator, nil, 1, func(d transport.Delivery) { received <- d })
		}()
		delivery := <-received
		synctest.Wait()
		client.mu.Lock()
		allowedBefore := client.allowed
		client.mu.Unlock()
		if allowedBefore != 0 {
			t.Fatal("rebalance allowed before in-flight settlement")
		}
		if err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		client.mu.Lock()
		allowedAfter := client.allowed
		client.mu.Unlock()
		if allowedAfter != 1 {
			t.Fatalf("AllowRebalance calls = %d, want 1 after settlement", allowedAfter)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestPartitionProcessingOrderAndConcurrency(t *testing.T) {
	// DS-51/53: a slow lower offset blocks only its own partition.
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := &processingConsumer{}
		coordinator := newOffsetCoordinator(client, 4)
		defer coordinator.close()
		received := make(chan *kafkaDelivery, 4)
		done := make(chan error, 1)
		go func() {
			_, err := (&kafkaTransport{}).dispatchBatch(ctx, transport.Topology{}, "", coordinator, nil,
				[]*kgo.Record{processingRecord(0, 10), processingRecord(0, 12), processingRecord(1, 30), processingRecord(1, 31)},
				func(d transport.Delivery) { received <- d.(*kafkaDelivery) })
			done <- err
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
		coordinator := newOffsetCoordinator(client, 3)
		defer coordinator.close()
		received := make(chan transport.Delivery, 5)
		done := make(chan error, 1)
		go func() {
			done <- (&kafkaTransport{}).consumeRecords(ctx, transport.Subscription{}, client, coordinator, nil, 3, func(d transport.Delivery) { received <- d })
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
		if client.committedCount() != 2 {
			t.Fatal("late settlement did not retain its client")
		}
	})
}

func TestPartitionProcessingSettlementFailureStopsLaterOffsets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &processingConsumer{}
		coordinator := newOffsetCoordinator(client, 2)
		defer coordinator.close()
		received := make(chan transport.Delivery, 2)
		done := make(chan error, 1)
		go func() {
			_, err := (&kafkaTransport{}).dispatchBatch(context.Background(), transport.Topology{}, "", coordinator, nil,
				[]*kgo.Record{processingRecord(0, 0), processingRecord(0, 1)}, func(d transport.Delivery) { received <- d })
			done <- err
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
		if client.committedCount() != 0 || len(received) != 0 {
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

func TestKafkaAdvertisesPartitionOrderingAfterSafeRebalance(t *testing.T) {
	if !(&kafkaTransport{}).Capabilities().Ordering {
		t.Fatal("Kafka partition ordering was not advertised after contiguous commits and safe ownership transfer")
	}
}

func TestKafkaOrderingDoesNotSerializeAcrossPhysicalTopics(t *testing.T) {
	topo := transport.Topology{Namespace: "ordering", JobType: "events", Kind: transport.EventTopology}
	backend := &kafkaTransport{}
	main := processingRecord(0, 0)
	main.Topic = backend.mainTopic(topo)
	private := processingRecord(0, 0)
	private.Topic = backend.privateReadyTopic(topo, "alpha")
	coordinator := newOffsetCoordinator(&processingConsumer{}, 2)
	defer coordinator.close()
	received := make(chan *kafkaDelivery, 2)
	done := make(chan error, 1)
	go func() {
		_, err := backend.dispatchBatch(context.Background(), topo, "alpha", coordinator, nil, []*kgo.Record{main, private}, func(delivery transport.Delivery) {
			received <- delivery.(*kafkaDelivery)
		})
		done <- err
	}()
	first, second := <-received, <-received
	if first.record.Topic == second.record.Topic {
		t.Fatalf("deliveries unexpectedly share topic %q", first.record.Topic)
	}
	if err := second.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	if err := first.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
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
	coordinator := newOffsetCoordinator(t.client, capacity)
	err = t.consumeRecords(ctx, sub, t.client, coordinator, nil, capacity, deliver)
	if sub.WaitForHandlers != nil {
		sub.WaitForHandlers()
	}
	coordinator.close()
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
		if err := bus.DeclareTask("jobs"); err != nil {
			t.Fatal(err)
		}
		if err := bus.HandleTask("jobs", func(ctx context.Context, job eventbus.Job) error {
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
		if client.committedCount() != 4 {
			t.Fatal("engine did not settle all records")
		}
	})
}
