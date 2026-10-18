package kafka

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafkaAdmin struct {
	starts    map[string]map[int32]int64
	ends      map[string]map[int32]int64
	commits   map[string]map[string]map[int32]int64
	groups    []string
	commitErr error
}

func (*fakeKafkaAdmin) CreateTopics(context.Context, int32, int16, map[string]*string, ...string) (kadm.CreateTopicResponses, error) {
	return nil, nil
}
func (*fakeKafkaAdmin) DeleteTopics(context.Context, ...string) (kadm.DeleteTopicResponses, error) {
	return nil, nil
}
func (*fakeKafkaAdmin) Metadata(context.Context, ...string) (kadm.Metadata, error) {
	return kadm.Metadata{}, nil
}
func (a *fakeKafkaAdmin) ListStartOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return a.listed(a.starts, topics), nil
}
func (a *fakeKafkaAdmin) ListEndOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	return a.listed(a.ends, topics), nil
}
func (a *fakeKafkaAdmin) ListGroups(context.Context, ...string) (kadm.ListedGroups, error) {
	groups := make(kadm.ListedGroups, len(a.groups))
	for _, group := range a.groups {
		groups[group] = kadm.ListedGroup{Group: group}
	}
	return groups, nil
}
func (a *fakeKafkaAdmin) FetchOffsetsForTopics(_ context.Context, group string, topics ...string) (kadm.OffsetResponses, error) {
	responses := make(kadm.OffsetResponses)
	for _, topic := range topics {
		responses[topic] = make(map[int32]kadm.OffsetResponse)
		for partition := range a.ends[topic] {
			at := int64(-1)
			if a.commits[group] != nil {
				if value, ok := a.commits[group][topic][partition]; ok {
					at = value
				}
			}
			responses[topic][partition] = kadm.OffsetResponse{Offset: kadm.Offset{Topic: topic, Partition: partition, At: at, LeaderEpoch: -1}}
		}
	}
	return responses, nil
}
func (a *fakeKafkaAdmin) CommitOffsets(_ context.Context, group string, offsets kadm.Offsets) (kadm.OffsetResponses, error) {
	if a.commitErr != nil {
		return nil, a.commitErr
	}
	if a.commits == nil {
		a.commits = make(map[string]map[string]map[int32]int64)
	}
	if a.commits[group] == nil {
		a.commits[group] = make(map[string]map[int32]int64)
	}
	responses := make(kadm.OffsetResponses)
	for topic, partitions := range offsets {
		if a.commits[group][topic] == nil {
			a.commits[group][topic] = make(map[int32]int64)
		}
		responses[topic] = make(map[int32]kadm.OffsetResponse)
		for partition, offset := range partitions {
			a.commits[group][topic][partition] = offset.At
			responses[topic][partition] = kadm.OffsetResponse{Offset: offset}
		}
	}
	return responses, nil
}

func (a *fakeKafkaAdmin) listed(values map[string]map[int32]int64, topics []string) kadm.ListedOffsets {
	listed := make(kadm.ListedOffsets)
	for _, topic := range topics {
		listed[topic] = make(map[int32]kadm.ListedOffset)
		for partition, offset := range values[topic] {
			listed[topic][partition] = kadm.ListedOffset{Topic: topic, Partition: partition, Offset: offset, LeaderEpoch: 1}
		}
	}
	return listed
}

func TestKafkaDeadLetterRecordRetainsFailureAndSourceMetadata(t *testing.T) {
	failedAt := time.UnixMilli(1_700_000_000_123)
	sourceAt := failedAt.Add(-time.Minute)
	delivery := &kafkaDelivery{record: &kgo.Record{
		Topic: "orders", Partition: 3, Offset: 41, Timestamp: sourceAt,
	}}
	message := transport.Message{
		ID: "job-1", Type: "order", Payload: []byte(`{"v":3}`), OrderingKey: "account-7",
		Headers: map[string]string{"correlation-id": "correlation-1"},
	}
	payload, err := delivery.deadLetterRecord(message, transport.Decision{
		Reason: "permanent", Cause: errors.New("synthetic failure"), Attempts: 4, FailedAt: failedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := &kgo.Record{Partition: 2, Offset: 9, Value: payload}
	deadLetter, err := decodeKafkaDLQRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if deadLetter.ID != "2:9" || !reflect.DeepEqual(deadLetter.Message, message) || deadLetter.Reason != "permanent" || deadLetter.LastError != "synthetic failure" || deadLetter.Attempts != 4 || !deadLetter.FailedAt.Equal(failedAt) || deadLetter.Source != "orders" || deadLetter.SourcePosition != "3:41" {
		t.Fatalf("dead letter = %#v", deadLetter)
	}
	var encoded kafkaDLQRecord
	if err := json.Unmarshal(payload, &encoded); err != nil {
		t.Fatal(err)
	}
	if encoded.SourceTimestamp != sourceAt.UnixMilli() {
		t.Fatalf("source timestamp = %d", encoded.SourceTimestamp)
	}
}

func TestKafkaDLQAdminUsesLogicalAppendOnlyCursor(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "jobs"}
	backend := &kafkaTransport{}
	topic := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts:  map[string]map[int32]int64{topic: {0: 0, 1: 0}},
		ends:    map[string]map[int32]int64{topic: {0: 2, 1: 1}},
		commits: make(map[string]map[string]map[int32]int64),
	}
	backend.adm = admin
	records := map[int32][]*kgo.Record{
		0: {kafkaAdminRecord(t, topic, 0, 0, "zero"), kafkaAdminRecord(t, topic, 0, 1, "one")},
		1: {kafkaAdminRecord(t, topic, 1, 0, "other")},
	}
	backend.scanDLQ = func(_ context.Context, _ string, partition int32, start, end int64, limit int) ([]*kgo.Record, error) {
		var selected []*kgo.Record
		for _, record := range records[partition] {
			if record.Offset >= start && record.Offset < end && len(selected) < limit {
				selected = append(selected, record)
			}
		}
		return selected, nil
	}
	var published []string
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
		published = append(published, string(record.Value))
		return nil
	})

	ctx := context.Background()
	if count, err := backend.Count(ctx, topo); err != nil || count != 3 {
		t.Fatalf("count = %d, %v", count, err)
	}
	listed, err := backend.List(ctx, topo, 1, 2)
	if err != nil || len(listed) != 2 || listed[0].ID != "0:1" || listed[1].ID != "1:0" {
		t.Fatalf("list = %#v, %v", listed, err)
	}
	if err := backend.Requeue(ctx, topo, []string{"0:1"}); err == nil {
		t.Fatal("non-prefix requeue succeeded")
	}
	if err := backend.Requeue(ctx, topo, []string{"0:0", "0:1"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(published, []string{"zero", "one"}) {
		t.Fatalf("published = %q", published)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 1 {
		t.Fatalf("count after requeue = %d, %v", count, err)
	}
	if err := backend.Requeue(ctx, topo, []string{"0:0", "0:1"}); err != nil {
		t.Fatalf("repeated requeue = %v", err)
	}
	if len(published) != 2 {
		t.Fatalf("repeated requeue published again: %q", published)
	}
	if err := backend.Purge(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 0 {
		t.Fatalf("count after purge = %d, %v", count, err)
	}
}

func TestKafkaDLQRequeueFailureNeverAdvancesCursor(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "failure"}
	backend := &kafkaTransport{}
	topic := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts:  map[string]map[int32]int64{topic: {0: 0}},
		ends:    map[string]map[int32]int64{topic: {0: 1}},
		commits: make(map[string]map[string]map[int32]int64),
	}
	backend.adm = admin
	record := kafkaAdminRecord(t, topic, 0, 0, "payload")
	backend.scanDLQ = func(context.Context, string, int32, int64, int64, int) ([]*kgo.Record, error) {
		return []*kgo.Record{record}, nil
	}
	produceErr := errors.New("produce unavailable")
	backend.producer = settlementProducer(func(context.Context, *kgo.Record) error { return produceErr })
	if err := backend.Requeue(context.Background(), topo, []string{"0:0"}); !errors.Is(err, produceErr) {
		t.Fatalf("produce failure = %v", err)
	}
	if count, err := backend.Count(context.Background(), topo); err != nil || count != 1 {
		t.Fatalf("count after produce failure = %d, %v", count, err)
	}

	backend.producer = settlementProducer(func(context.Context, *kgo.Record) error { return nil })
	admin.commitErr = errors.New("commit unavailable")
	if err := backend.Requeue(context.Background(), topo, []string{"0:0"}); !errors.Is(err, admin.commitErr) {
		t.Fatalf("commit failure = %v", err)
	}
	admin.commitErr = nil
	if count, err := backend.Count(context.Background(), topo); err != nil || count != 1 {
		t.Fatalf("count after commit failure = %d, %v", count, err)
	}
}

func TestKafkaStatsUsesEndAndCommittedOffsets(t *testing.T) {
	topo := transport.Topology{Namespace: "stats", JobType: "jobs"}
	backend := &kafkaTransport{}
	main := backend.mainTopic(topo)
	dlq := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts:  make(map[string]map[int32]int64),
		ends:    make(map[string]map[int32]int64),
		commits: make(map[string]map[string]map[int32]int64),
	}
	admin.starts[main], admin.ends[main] = map[int32]int64{0: 0, 1: 2}, map[int32]int64{0: 10, 1: 5}
	admin.commits[topo.Name()+"-group"] = map[string]map[int32]int64{main: {0: 4}}
	for i, tier := range delayTiers {
		topic := backend.delayTopic(topo, tier)
		admin.starts[topic], admin.ends[topic] = map[int32]int64{0: 0}, map[int32]int64{0: int64(i + 1)}
		group := topo.Name() + "-delay-" + tier.String() + "-group"
		admin.commits[group] = map[string]map[int32]int64{topic: {0: int64(i)}}
	}
	admin.starts[dlq], admin.ends[dlq] = map[int32]int64{0: 0}, map[int32]int64{0: 2}
	backend.adm = admin

	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 9 || stats.InFlight != 0 || stats.Lag != 9 || !stats.LagKnown || stats.Delayed != 3 || stats.DLQ != 2 || stats.OldestAgeKnown {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestKafkaEventStatsAggregateOnlySubscriberGroups(t *testing.T) {
	topo := transport.Topology{Namespace: "stats", JobType: "events", Kind: transport.EventTopology}
	backend := &kafkaTransport{}
	main := backend.mainTopic(topo)
	dlq := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts:  map[string]map[int32]int64{main: {0: 0}, dlq: {0: 0}},
		ends:    map[string]map[int32]int64{main: {0: 5}, dlq: {0: 0}},
		commits: make(map[string]map[string]map[int32]int64),
		groups: []string{
			topo.Name() + "-alpha", topo.Name() + "-beta", topo.Name() + "-dlq-admin",
			"unrelated-group",
		},
	}
	admin.commits[topo.Name()+"-alpha"] = map[string]map[int32]int64{main: {0: 3}}
	admin.commits[topo.Name()+"-beta"] = map[string]map[int32]int64{main: {0: 4}}
	for _, tier := range delayTiers {
		topic := backend.delayTopic(topo, tier)
		admin.starts[topic], admin.ends[topic] = map[int32]int64{0: 0}, map[int32]int64{0: 0}
	}
	backend.adm = admin
	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 3 || stats.Lag != 3 {
		t.Fatalf("event stats = %#v", stats)
	}
}

func kafkaAdminRecord(t *testing.T, topic string, partition int32, offset int64, payload string) *kgo.Record {
	t.Helper()
	encoded, err := json.Marshal(kafkaDLQRecord{
		Version:  kafkaDLQRecordVersion,
		Message:  transport.Message{Payload: []byte(payload)},
		FailedAt: time.Now().UnixMilli(), SourceTopic: "source", SourcePartition: partition,
		SourceOffset: offset, SourceTimestamp: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: topic, Partition: partition, Offset: offset, Value: encoded}
}
