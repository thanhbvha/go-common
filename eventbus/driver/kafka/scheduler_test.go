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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeDelayConsumer struct {
	commits     []*kgo.Record
	commitCalls int
	rewinds     map[string]map[int32]kgo.EpochOffset
	commitFn    func(*kgo.Record) error
	pollFn      func(context.Context, int) kgo.Fetches
	allowed     int
	closed      bool
}

func (c *fakeDelayConsumer) PollRecords(ctx context.Context, limit int) kgo.Fetches {
	if c.pollFn != nil {
		return c.pollFn(ctx, limit)
	}
	return nil
}
func (c *fakeDelayConsumer) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	c.commitCalls++
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

func TestRelatedTopicPartitionsWaitsForLeader(t *testing.T) {
	backend := &kafkaTransport{}
	admin := &fakeKafkaAdmin{metadata: kafkaAdminMetadata([]string{"related"}, 1, 1)}
	admin.metadata.Topics["related"].Partitions[0] = kadm.PartitionDetail{Topic: "related", Partition: 0, Leader: -1}
	backend.adm = admin

	ready, err := backend.relatedTopicPartitionsReady(context.Background(), []string{"related"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("leaderless topic reported ready")
	}
}

func TestDelaySchedulerRotatesCurrentTierWithoutHoldingRecord(t *testing.T) {
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
	if produced != 1 || len(client.commits) != 1 {
		t.Fatal("record still in its tier was not durably rotated")
	}
	if client.rewinds != nil {
		t.Fatalf("unexpected rewind = %#v", client.rewinds)
	}
}

func TestDelaySchedulerFarFutureHeadDoesNotBlockNearFutureDeadline(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "head-of-line"}
	tier := 5 * time.Minute
	backend := &kafkaTransport{}
	var produced []*kgo.Record
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
		produced = append(produced, record)
		return nil
	})
	client := &fakeDelayConsumer{}
	far := delayedRecord(backend.delayTopic(topo, tier), 0, 10, now.Add(time.Hour))
	near := delayedRecord(backend.delayTopic(topo, tier), 0, 11, now.Add(4*time.Minute))

	if err := backend.processDelayBatch(context.Background(), topo, tier, client, []*kgo.Record{far, near}, now); err != nil {
		t.Fatal(err)
	}
	if len(produced) != 2 || len(client.commits) != 1 || client.commits[0] != near {
		t.Fatalf("produced=%d commits=%#v, want one watermark at the second record", len(produced), client.commits)
	}
	if produced[0].Topic != backend.delayTopic(topo, tier) {
		t.Fatalf("far destination = %q", produced[0].Topic)
	}
	if produced[1].Topic != backend.delayTopic(topo, time.Minute) {
		t.Fatalf("near destination = %q", produced[1].Topic)
	}
	if client.rewinds != nil {
		t.Fatalf("unexpected rewind = %#v", client.rewinds)
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

func TestDelaySchedulerUsesTransportHeaderForOpaquePayload(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "opaque"}
	backend := &kafkaTransport{}
	var produced *kgo.Record
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
		produced = record
		return nil
	})
	client := &fakeDelayConsumer{}
	source := scheduledMessageRecord(backend.delayTopic(topo, time.Minute), transport.Message{
		Payload: []byte("opaque-payload"), Headers: map[string]string{"traceparent": "synthetic"},
	}, now)
	source.Partition, source.Offset = 0, 4
	if err := backend.processDelayBatch(context.Background(), topo, time.Minute, client, []*kgo.Record{source}, now); err != nil {
		t.Fatal(err)
	}
	if produced == nil || produced.Topic != backend.mainTopic(topo) || string(produced.Value) != "opaque-payload" {
		t.Fatalf("produced = %#v", produced)
	}
	if got := recordMessage(produced).Headers; !reflect.DeepEqual(got, map[string]string{"traceparent": "synthetic"}) {
		t.Fatalf("headers = %#v", got)
	}
	if len(client.commits) != 1 || client.commits[0] != source {
		t.Fatal("opaque delayed source was not committed after confirmed transfer")
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

type batchSettlementProducer struct {
	calls   int
	records [][]*kgo.Record
	errors  []error
}

func (p *batchSettlementProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	p.calls++
	p.records = append(p.records, append([]*kgo.Record(nil), records...))
	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record}
		if i < len(p.errors) {
			results[i].Err = p.errors[i]
		}
	}
	return results
}

func TestDelaySchedulerBatchesProduceAndCommitsPartitionWatermarks(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "batch"}
	tier := 5 * time.Second
	producer := &batchSettlementProducer{}
	backend := &kafkaTransport{producer: producer}
	client := &fakeDelayConsumer{}
	records := []*kgo.Record{
		delayedRecord(backend.delayTopic(topo, tier), 0, 10, now),
		delayedRecord(backend.delayTopic(topo, tier), 0, 12, now),
		delayedRecord(backend.delayTopic(topo, tier), 1, 20, now),
	}
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, records, now); err != nil {
		t.Fatal(err)
	}
	if producer.calls != 1 || len(producer.records) != 1 || len(producer.records[0]) != len(records) {
		t.Fatalf("produce calls=%d records=%#v", producer.calls, producer.records)
	}
	if client.commitCalls != 1 || !reflect.DeepEqual(client.commits, []*kgo.Record{records[1], records[2]}) {
		t.Fatalf("commit calls=%d watermarks=%#v", client.commitCalls, client.commits)
	}
	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || status[0].Transfers != 3 || status[0].Commits != 3 {
		t.Fatalf("status=%#v", status)
	}
}

func TestDelaySchedulerCommitsOnlyConfirmedPartitionPrefix(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "prefix"}
	tier := 5 * time.Second
	injected := errors.New("injected middle failure")
	producer := &batchSettlementProducer{errors: []error{nil, injected, nil, nil}}
	backend := &kafkaTransport{producer: producer}
	client := &fakeDelayConsumer{}
	records := []*kgo.Record{
		delayedRecord(backend.delayTopic(topo, tier), 0, 10, now),
		delayedRecord(backend.delayTopic(topo, tier), 0, 12, now),
		delayedRecord(backend.delayTopic(topo, tier), 0, 14, now),
		delayedRecord(backend.delayTopic(topo, tier), 1, 20, now),
	}
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, records, now); !errors.Is(err, injected) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(client.commits, []*kgo.Record{records[0], records[3]}) {
		t.Fatalf("watermarks=%#v", client.commits)
	}
	if got := client.rewinds[records[1].Topic][0].Offset; got != records[1].Offset {
		t.Fatalf("rewind=%d, want %d", got, records[1].Offset)
	}
	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || status[0].Transfers != 3 || status[0].Commits != 2 {
		t.Fatalf("status=%#v", status)
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

func TestTargetedDelaySchedulerRoutesOnlyToPrivateReadyAndStripsInternalHeaders(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "events", Kind: transport.EventTopology, Options: map[string]any{kafkaEventLayoutOption: kafkaTargetedLayoutV1}}
	tier := time.Minute
	backend := &kafkaTransport{}
	admin := &fakeKafkaAdmin{}
	provisionKafkaSubscriber(backend, admin, topo, "alpha")
	backend.adm = admin
	var produced *kgo.Record
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
		produced = record
		return nil
	})
	message := transport.Message{Payload: []byte("retry"), OrderingKey: "account-7", Headers: map[string]string{
		"traceparent": "synthetic", kafkaTargetGroupHeader: "alpha",
	}}
	record := scheduledMessageRecord(backend.targetedDelayTopic(topo, tier), message, now)
	record.Partition, record.Offset = 0, 3
	client := &fakeDelayConsumer{}
	if err := backend.processDelayBatchFor(context.Background(), topo, tier, true, client, []*kgo.Record{record}, now); err != nil {
		t.Fatal(err)
	}
	if produced == nil || produced.Topic != backend.privateReadyTopic(topo, "alpha") {
		t.Fatalf("target = %#v", produced)
	}
	if got := recordMessage(produced).Headers; !reflect.DeepEqual(got, map[string]string{"traceparent": "synthetic"}) {
		t.Fatalf("ready headers = %#v", got)
	}
	if len(client.commits) != 1 || client.commits[0] != record {
		t.Fatal("targeted source was not committed after confirmed private publish")
	}
}

func TestTargetedDelaySchedulerFailsClosedWithoutTargetGroup(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "events", Kind: transport.EventTopology, Options: map[string]any{kafkaEventLayoutOption: kafkaTargetedLayoutV1}}
	tier := time.Minute
	backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error {
		t.Fatal("record without a target group was published")
		return nil
	})}
	record := scheduledMessageRecord(backend.targetedDelayTopic(topo, tier), transport.Message{Payload: []byte("old")}, now)
	record.Partition, record.Offset = 1, 4
	client := &fakeDelayConsumer{}
	if err := backend.processDelayBatchFor(context.Background(), topo, tier, true, client, []*kgo.Record{record}, now); err == nil {
		t.Fatal("targetless record unexpectedly succeeded")
	}
	if len(client.commits) != 0 || client.rewinds[record.Topic][record.Partition].Offset != record.Offset {
		t.Fatal("targetless record did not remain durable and uncommitted")
	}
}

func TestKafkaSchedulerHealthRecordsAndClearsFailures(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "health"}
	tier := 5 * time.Second
	injected := errors.New("synthetic scheduler failure")
	backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { return injected })}
	client := &fakeDelayConsumer{}
	record := delayedRecord(backend.delayTopic(topo, tier), 0, 1, now)
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, []*kgo.Record{record}, now); !errors.Is(err, injected) {
		t.Fatalf("error = %v", err)
	}
	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || !status[0].Degraded || status[0].Retries == 0 || status[0].LastErrorOperation == "" {
		t.Fatalf("failure status = %#v", status)
	}
	backend.producer = settlementProducer(func(context.Context, *kgo.Record) error { return nil })
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, []*kgo.Record{record}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	status = backend.SchedulerHealth(topo)
	if len(status) != 1 || status[0].Degraded || status[0].ConsecutiveErrors != 0 || status[0].LastSuccess.IsZero() {
		t.Fatalf("recovered status = %#v", status)
	}
}

func TestKafkaSchedulerHealthCountsPartialBatchProgress(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	topo := transport.Topology{Namespace: "test", JobType: "partial-health"}
	tier := 5 * time.Second
	injected := errors.New("synthetic second publish failure")
	produced := 0
	backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error {
		produced++
		if produced == 2 {
			return injected
		}
		return nil
	})}
	client := &fakeDelayConsumer{}
	records := []*kgo.Record{
		delayedRecord(backend.delayTopic(topo, tier), 0, 0, now),
		delayedRecord(backend.delayTopic(topo, tier), 0, 1, now),
	}
	if err := backend.processDelayBatch(context.Background(), topo, tier, client, records, now); !errors.Is(err, injected) {
		t.Fatalf("error = %v", err)
	}
	status := backend.SchedulerHealth(topo)
	if len(status) != 1 || status[0].Transfers != 1 || status[0].Commits != 1 || status[0].Retries != 1 || !status[0].Degraded {
		t.Fatalf("partial status = %#v", status)
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

func TestTopologyFetchOptions(t *testing.T) {
	topo := transport.Topology{Options: map[string]any{
		"kafka.fetch_max_wait":            25 * time.Millisecond,
		"kafka.fetch_min_bytes":           int32(1024),
		"kafka.fetch_max_bytes":           int32(8 << 20),
		"kafka.fetch_max_partition_bytes": int32(2 << 20),
		"kafka.max_buffered_records":      300,
		"kafka.max_buffered_bytes":        16 << 20,
	}}
	client, err := kgo.NewClient(topologyFetchOptions(topo)...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for option, want := range map[string]any{
		"FetchMaxWait":           25 * time.Millisecond,
		"FetchMinBytes":          int32(1024),
		"FetchMaxBytes":          int32(8 << 20),
		"FetchMaxPartitionBytes": int32(2 << 20),
		"MaxBufferedRecords":     300,
		"MaxBufferedBytes":       16 << 20,
	} {
		var key any
		switch option {
		case "FetchMaxWait":
			key = kgo.FetchMaxWait
		case "FetchMinBytes":
			key = kgo.FetchMinBytes
		case "FetchMaxBytes":
			key = kgo.FetchMaxBytes
		case "FetchMaxPartitionBytes":
			key = kgo.FetchMaxPartitionBytes
		case "MaxBufferedRecords":
			key = kgo.MaxBufferedRecords
		case "MaxBufferedBytes":
			key = kgo.MaxBufferedBytes
		}
		got := client.OptValue(key)
		if reflect.ValueOf(got).Int() != reflect.ValueOf(want).Int() {
			t.Fatalf("%s = %#v, want %#v", option, got, want)
		}
	}
}

func TestDelayDisabledRejectsFuturePublishBeforeBrokerIO(t *testing.T) {
	topo := transport.Topology{
		Namespace: "test", JobType: "no-delay",
		Options: map[string]any{"kafka.delay_disabled": true},
	}
	backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error {
		t.Fatal("future publish reached Kafka producer")
		return nil
	})}
	err := backend.PublishAt(context.Background(), topo, transport.Message{Payload: []byte("payload")}, time.Now().Add(time.Minute))
	if !errors.Is(err, transport.ErrNotSupported) {
		t.Fatalf("error = %v", err)
	}
}

func TestDelayDisabledRetryDoesNotCommitSource(t *testing.T) {
	topo := transport.Topology{
		Namespace: "test", JobType: "no-delay-retry",
		Options: map[string]any{"kafka.delay_disabled": true},
	}
	client := &processingConsumer{}
	coordinator := newOffsetCoordinator(client, 1)
	defer coordinator.close()
	record := processingRecord(0, 1)
	if err := coordinator.register(context.Background(), []*kgo.Record{record}); err != nil {
		t.Fatal(err)
	}
	backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error {
		t.Fatal("future retry reached Kafka producer")
		return nil
	})}
	delivery := &kafkaDelivery{transport: backend, topo: topo, coordinator: coordinator, record: record}
	err := delivery.Settle(context.Background(), transport.Decision{
		Kind: transport.DecisionRetry, Next: transport.Message{Payload: []byte("retry")}, RetryAt: time.Now().Add(time.Minute),
	})
	if !errors.Is(err, transport.ErrNotSupported) || client.committedCount() != 0 {
		t.Fatalf("error=%v committed=%d", err, client.committedCount())
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
			(&kafkaTransport{}).runDelayTier(ctx, topo, time.Second, false, client)
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

func TestDelayTierFullBatchUsesFastFollowUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topo := transport.Topology{
			Namespace: "test", JobType: "fast-follow-up",
			Options: map[string]any{"kafka.delay_batch_size": 2},
		}
		tier := 5 * time.Second
		backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { return nil })}
		polls := make(chan time.Time, 2)
		calls := 0
		client := &fakeDelayConsumer{pollFn: func(_ context.Context, _ int) kgo.Fetches {
			calls++
			polls <- time.Now()
			if calls == 1 {
				records := []*kgo.Record{
					delayedRecord(backend.delayTopic(topo, tier), 0, 1, time.Now()),
					delayedRecord(backend.delayTopic(topo, tier), 0, 2, time.Now()),
				}
				return fakeFetches(records)
			}
			cancel()
			return nil
		}}
		done := make(chan struct{})
		go func() {
			backend.runDelayTier(ctx, topo, tier, false, client)
			close(done)
		}()
		first, second := <-polls, <-polls
		if elapsed := second.Sub(first); elapsed != delayActiveInterval {
			t.Fatalf("follow-up interval = %v, want %v", elapsed, delayActiveInterval)
		}
		<-done
	})
}

func TestDelayTierShortRotationReturnsToIdleCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		topo := transport.Topology{
			Namespace: "test", JobType: "idle-rotation",
			Options: map[string]any{"kafka.delay_batch_size": 2},
		}
		tier := 5 * time.Minute
		backend := &kafkaTransport{producer: settlementProducer(func(context.Context, *kgo.Record) error { return nil })}
		polls := make(chan time.Time, 2)
		calls := 0
		client := &fakeDelayConsumer{pollFn: func(_ context.Context, _ int) kgo.Fetches {
			calls++
			polls <- time.Now()
			if calls == 1 {
				return fakeFetches([]*kgo.Record{delayedRecord(backend.delayTopic(topo, tier), 0, 1, time.Now().Add(time.Hour))})
			}
			cancel()
			return nil
		}}
		done := make(chan struct{})
		go func() {
			backend.runDelayTier(ctx, topo, tier, false, client)
			close(done)
		}()
		first, second := <-polls, <-polls
		if elapsed := second.Sub(first); elapsed != delaySchedulerInterval {
			t.Fatalf("follow-up interval = %v, want %v", elapsed, delaySchedulerInterval)
		}
		<-done
	})
}

func fakeFetches(records []*kgo.Record) kgo.Fetches {
	partitions := make(map[int32]kgo.FetchPartition)
	for _, record := range records {
		partition := partitions[record.Partition]
		partition.Partition = record.Partition
		partition.Records = append(partition.Records, record)
		partitions[record.Partition] = partition
	}
	fetchPartitions := make([]kgo.FetchPartition, 0, len(partitions))
	for _, partition := range partitions {
		fetchPartitions = append(fetchPartitions, partition)
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: records[0].Topic, Partitions: fetchPartitions}}}}
}
