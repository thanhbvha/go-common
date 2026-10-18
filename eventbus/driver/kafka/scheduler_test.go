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
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeDelayConsumer struct {
	commits  []*kgo.Record
	rewinds  map[string]map[int32]kgo.EpochOffset
	commitFn func(*kgo.Record) error
	pollFn   func(context.Context, int) kgo.Fetches
	allowed  int
	closed   bool
}

func (c *fakeDelayConsumer) PollRecords(ctx context.Context, limit int) kgo.Fetches {
	if c.pollFn != nil {
		return c.pollFn(ctx, limit)
	}
	return nil
}
func (c *fakeDelayConsumer) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	for _, record := range records {
		if c.commitFn != nil {
			if err := c.commitFn(record); err != nil {
				return err
			}
		}
		c.commits = append(c.commits, record)
	}
	return nil
}
func (c *fakeDelayConsumer) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	c.rewinds = offsets
}
func (c *fakeDelayConsumer) AllowRebalance()         { c.allowed++ }
func (c *fakeDelayConsumer) CloseAllowingRebalance() { c.closed = true }

func delayedRecord(topic string, partition int32, offset int64, runAt time.Time) *kgo.Record {
	return &kgo.Record{
		Topic: topic, Partition: partition, Offset: offset, LeaderEpoch: 4,
		Key:     []byte("account-7"),
		Value:   []byte(fmt.Sprintf(`{"v":3,"id":"job-7","type":"task","attempt":2,"max_attempts":5,"run_at":%d,"ordering_key":"account-7","headers":{"traceparent":"synthetic"}}`, runAt.UnixMilli())),
		Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte("synthetic")}},
	}
}

func TestDelayDestinationUsesBoundedTiers(t *testing.T) {
	backend := &kafkaTransport{}
	topo := transport.Topology{Namespace: "test", JobType: "jobs"}
	now := time.Unix(1_700_000_000, 0)
	for _, test := range []struct {
		name  string
		delay time.Duration
		want  string
	}{
		{name: "due", delay: 0, want: backend.mainTopic(topo)},
		{name: "sub tier", delay: time.Second, want: backend.delayTopic(topo, 5*time.Second)},
		{name: "five seconds", delay: 5 * time.Second, want: backend.delayTopic(topo, 5*time.Second)},
		{name: "one minute", delay: time.Minute, want: backend.delayTopic(topo, time.Minute)},
		{name: "five minutes", delay: 5 * time.Minute, want: backend.delayTopic(topo, 5*time.Minute)},
		{name: "one hour", delay: time.Hour, want: backend.delayTopic(topo, 5*time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := backend.delayDestination(topo, now.Add(test.delay), now); got != test.want {
				t.Fatalf("destination = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDelaySchedulerRewindsCurrentTierWithoutHoldingRecord(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "jobs"}
	backend := &kafkaTransport{}
	produced := 0
	backend.producer = settlementProducer(func(context.Context, *kgo.Record) error { produced++; return nil })
	client := &fakeDelayConsumer{}
	record := delayedRecord(backend.delayTopic(topo, 5*time.Minute), 2, 9, now.Add(10*time.Minute))
	if err := backend.processDelayBatch(context.Background(), topo, 5*time.Minute, client, []*kgo.Record{record}, now); err != nil {
		t.Fatal(err)
	}
	if produced != 0 || len(client.commits) != 0 {
		t.Fatal("record still in its tier was transferred or committed")
	}
	if got := client.rewinds[record.Topic][record.Partition]; got.Offset != record.Offset || got.Epoch != record.LeaderEpoch {
		t.Fatalf("rewind = %#v", got)
	}
}

func TestDelaySchedulerTransfersDownTiersAndPreservesMetadata(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "jobs"}
	backend := &kafkaTransport{}
	var produced []*kgo.Record
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
		produced = append(produced, record)
		return nil
	})
	client := &fakeDelayConsumer{}
	fromFiveMinutes := delayedRecord(backend.delayTopic(topo, 5*time.Minute), 0, 1, now.Add(30*time.Second))
	due := delayedRecord(backend.delayTopic(topo, 5*time.Second), 1, 3, now.Add(-time.Millisecond))
	if err := backend.processDelayBatch(context.Background(), topo, 5*time.Minute, client, []*kgo.Record{fromFiveMinutes}, now); err != nil {
		t.Fatal(err)
	}
	if err := backend.processDelayBatch(context.Background(), topo, 5*time.Second, client, []*kgo.Record{due}, now); err != nil {
		t.Fatal(err)
	}
	if len(produced) != 2 || len(client.commits) != 2 {
		t.Fatalf("produced=%d commits=%d", len(produced), len(client.commits))
	}
	if produced[0].Topic != backend.delayTopic(topo, 5*time.Second) || produced[1].Topic != backend.mainTopic(topo) {
		t.Fatalf("destinations = %q, %q", produced[0].Topic, produced[1].Topic)
	}
	want := &kgo.Record{Topic: produced[0].Topic, Key: fromFiveMinutes.Key, Value: fromFiveMinutes.Value, Headers: fromFiveMinutes.Headers}
	if !reflect.DeepEqual(produced[0], want) {
		t.Fatalf("forwarded record = %#v, want %#v", produced[0], want)
	}
}

func TestDelaySchedulerFailureNeverCommitsAndBlocksLaterPartitionOffset(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "jobs"}
	tier := 5 * time.Second
	backend := &kafkaTransport{}
	injected := errors.New("injected publish failure")
	produceCalls := 0
	backend.producer = settlementProducer(func(_ context.Context, _ *kgo.Record) error {
		produceCalls++
		if produceCalls == 1 {
			return injected
		}
		return nil
	})
	client := &fakeDelayConsumer{}
	records := []*kgo.Record{
		delayedRecord(backend.delayTopic(topo, tier), 0, 4, now),
		delayedRecord(backend.delayTopic(topo, tier), 0, 5, now),
		delayedRecord(backend.delayTopic(topo, tier), 1, 8, now),
	}
	err := backend.processDelayBatch(context.Background(), topo, tier, client, records, now)
	if !errors.Is(err, injected) {
		t.Fatalf("error = %v", err)
	}
	if len(client.commits) != 1 || client.commits[0] != records[2] {
		t.Fatalf("commits = %#v", client.commits)
	}
	if got := client.rewinds[records[0].Topic][0].Offset; got != records[0].Offset {
		t.Fatalf("failed partition rewind = %d", got)
	}
}

func TestDelaySchedulerCommitFailureRewindsAfterConfirmedPublish(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "jobs"}
	tier := 5 * time.Second
	backend := &kafkaTransport{}
	produced := 0
	backend.producer = settlementProducer(func(context.Context, *kgo.Record) error { produced++; return nil })
	injected := errors.New("injected commit failure")
	client := &fakeDelayConsumer{commitFn: func(*kgo.Record) error { return injected }}
	record := delayedRecord(backend.delayTopic(topo, tier), 0, 4, now)
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, []*kgo.Record{record}, now); !errors.Is(err, injected) {
		t.Fatalf("error = %v", err)
	}
	if produced != 1 || len(client.commits) != 0 || client.rewinds[record.Topic][0].Offset != record.Offset {
		t.Fatal("commit failure did not retain the durable source")
	}
}

func TestDelaySchedulerMalformedRecordRemainsUncommitted(t *testing.T) {
	topo := transport.Topology{Namespace: "test", JobType: "jobs"}
	tier := 5 * time.Second
	backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { return nil })}
	client := &fakeDelayConsumer{}
	record := &kgo.Record{Topic: backend.delayTopic(topo, tier), Partition: 3, Offset: 7, Value: []byte("not-json")}
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, []*kgo.Record{record}, time.Now()); err == nil {
		t.Fatal("malformed delayed record unexpectedly succeeded")
	}
	if len(client.commits) != 0 || client.rewinds[record.Topic][3].Offset != record.Offset {
		t.Fatal("malformed source was not retained")
	}
}

func TestDelayBatchSize(t *testing.T) {
	if got := delayBatchSize(transport.Topology{}); got != defaultDelayBatchSize {
		t.Fatalf("default = %d", got)
	}
	topo := transport.Topology{Options: map[string]any{"kafka.delay_batch_size": 7}}
	if got := delayBatchSize(topo); got != 7 {
		t.Fatalf("override = %d", got)
	}
	for _, value := range []any{0, maxDelayBatchSize + 1, int32(1), "1"} {
		topology := transport.Topology{JobType: "jobs", Options: map[string]any{"kafka.delay_batch_size": value}}
		if err := (&kafkaTransport{}).ValidateTopology(topology); err == nil {
			t.Fatalf("accepted delay batch size %#v", value)
		}
	}
}

func TestDelayTierPollIsBoundedAndShutdownJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		topo := transport.Topology{
			Namespace: "test", JobType: "jobs",
			Options: map[string]any{"kafka.delay_batch_size": 7},
		}
		polled := make(chan int, 1)
		client := &fakeDelayConsumer{pollFn: func(_ context.Context, limit int) kgo.Fetches {
			polled <- limit
			return nil
		}}
		done := make(chan struct{})
		go func() {
			(&kafkaTransport{}).runDelayTier(ctx, topo, time.Second, client)
			close(done)
		}()
		if got := <-polled; got != 7 {
			t.Fatalf("PollRecords limit = %d, want 7", got)
		}
		cancel()
		<-done
		if !client.closed {
			t.Fatal("delay consumer was not closed on shutdown")
		}
	})
}
