package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeKafkaAdmin struct {
	starts             map[string]map[int32]int64
	ends               map[string]map[int32]int64
	commits            map[string]map[string]map[int32]int64
	groups             []string
	commitErr          error
	metadata           kadm.Metadata
	configs            kadm.ResourceConfigs
	altered            map[string]map[string]string
	partitionResponses kadm.CreatePartitionsResponses
	partitionCalls     []string
	partitionErrTopic  string
	listEndHook        func()
	listStartResponses []kadm.ListedOffsets
	listStartCalls     int
	listStartTotal     int
	listEndCalls       int
	fetchOffsetCalls   map[string]int
	createTopicCalls   []createTopicCall
}

type createTopicCall struct {
	partitions int32
	topics     []string
}

func (a *fakeKafkaAdmin) CreateTopics(_ context.Context, partitions int32, replicationFactor int16, configs map[string]*string, topics ...string) (kadm.CreateTopicResponses, error) {
	a.createTopicCalls = append(a.createTopicCalls, createTopicCall{partitions: partitions, topics: append([]string(nil), topics...)})
	if a.metadata.Topics == nil {
		a.metadata.Topics = make(kadm.TopicDetails)
	}
	responses := make(kadm.CreateTopicResponses, len(topics))
	for _, topic := range topics {
		if _, exists := a.metadata.Topics[topic]; exists {
			responses[topic] = kadm.CreateTopicResponse{Topic: topic, Err: kerr.TopicAlreadyExists}
			continue
		}
		detail := kadm.TopicDetail{Topic: topic, Partitions: make(kadm.PartitionDetails, partitions)}
		for partition := int32(0); partition < partitions; partition++ {
			detail.Partitions[partition] = kadm.PartitionDetail{Topic: topic, Partition: partition, Leader: 0, Replicas: make([]int32, replicationFactor)}
		}
		a.metadata.Topics[topic] = detail
		responses[topic] = kadm.CreateTopicResponse{Topic: topic}
		resource := kadm.ResourceConfig{Name: topic}
		for key, value := range configs {
			resource.Configs = append(resource.Configs, kadm.Config{Key: key, Value: value})
		}
		a.configs = append(a.configs, resource)
	}
	return responses, nil
}
func (a *fakeKafkaAdmin) DescribeTopicConfigs(context.Context, ...string) (kadm.ResourceConfigs, error) {
	return a.configs, nil
}
func (a *fakeKafkaAdmin) AlterTopicConfigs(_ context.Context, configs []kadm.AlterConfig, topics ...string) (kadm.AlterConfigsResponses, error) {
	responses := make(kadm.AlterConfigsResponses, 0, len(topics))
	for _, topic := range topics {
		if a.altered == nil {
			a.altered = make(map[string]map[string]string)
		}
		if a.altered[topic] == nil {
			a.altered[topic] = make(map[string]string)
		}
		for _, config := range configs {
			if config.Value != nil {
				a.altered[topic][config.Name] = *config.Value
			}
		}
		responses = append(responses, kadm.AlterConfigsResponse{Name: topic})
	}
	return responses, nil
}
func (a *fakeKafkaAdmin) CreatePartitions(_ context.Context, _ int, topics ...string) (kadm.CreatePartitionsResponses, error) {
	responses := make(kadm.CreatePartitionsResponses, len(topics))
	for _, topic := range topics {
		a.partitionCalls = append(a.partitionCalls, topic)
		response := kadm.CreatePartitionsResponse{Topic: topic}
		if topic == a.partitionErrTopic {
			response.Err = errors.New("synthetic partition failure")
		}
		responses[topic] = response
	}
	return responses, nil
}
func (*fakeKafkaAdmin) DeleteTopics(context.Context, ...string) (kadm.DeleteTopicResponses, error) {
	return nil, nil
}

func kafkaAdminMetadata(topics []string, partitions int, replicas int) kadm.Metadata {
	metadata := kadm.Metadata{Topics: make(kadm.TopicDetails, len(topics))}
	for _, topic := range topics {
		detail := kadm.TopicDetail{Topic: topic, Partitions: make(kadm.PartitionDetails, partitions)}
		for partition := range partitions {
			replicaIDs := make([]int32, replicas)
			detail.Partitions[int32(partition)] = kadm.PartitionDetail{Topic: topic, Partition: int32(partition), Leader: 0, Replicas: replicaIDs}
		}
		metadata.Topics[topic] = detail
	}
	return metadata
}

func kafkaAdminConfigs(topics []string, values map[string]string) kadm.ResourceConfigs {
	configs := make(kadm.ResourceConfigs, 0, len(topics))
	for _, topic := range topics {
		resource := kadm.ResourceConfig{Name: topic}
		for key, value := range values {
			value := value
			resource.Configs = append(resource.Configs, kadm.Config{Key: key, Value: &value})
		}
		configs = append(configs, resource)
	}
	return configs
}

func provisionKafkaSubscriber(backend *kafkaTransport, admin *fakeKafkaAdmin, topology transport.Topology, group string) {
	privateTopic := backend.privateReadyTopic(topology, group)
	marker := backend.subscriberMarkerTopic(topology, group)
	layout := backend.eventLayoutTopic(topology)
	admin.metadata = kafkaAdminMetadata([]string{privateTopic, marker, layout}, 1, 1)
	admin.configs = append(
		kafkaAdminConfigs([]string{privateTopic}, map[string]string{"cleanup.policy": "delete"}),
		kafkaAdminConfigs([]string{marker}, subscriberMarkerConfigs())...,
	)
}
func (a *fakeKafkaAdmin) Metadata(context.Context, ...string) (kadm.Metadata, error) {
	return a.metadata, nil
}
func (a *fakeKafkaAdmin) ListStartOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	a.listStartTotal++
	if a.listStartCalls < len(a.listStartResponses) {
		response := a.listStartResponses[a.listStartCalls]
		a.listStartCalls++
		return response, nil
	}
	return a.listed(a.starts, topics), nil
}
func (a *fakeKafkaAdmin) ListEndOffsets(_ context.Context, topics ...string) (kadm.ListedOffsets, error) {
	a.listEndCalls++
	if a.listEndHook != nil {
		a.listEndHook()
	}
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
	if a.fetchOffsetCalls == nil {
		a.fetchOffsetCalls = make(map[string]int)
	}
	a.fetchOffsetCalls[group]++
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
		Reason: "permanent", Cause: errors.New("synthetic failure"), ErrorCode: "synthetic_failure", Attempts: 4, FailedAt: failedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := &kgo.Record{Partition: 2, Offset: 9, Value: payload}
	deadLetter, err := decodeKafkaDLQRecord(raw)
	if err != nil {
		t.Fatal(err)
	}
	if deadLetter.ID != "2:9" || !reflect.DeepEqual(deadLetter.Message, message) || deadLetter.Reason != "permanent" || deadLetter.LastError != "synthetic_failure" || deadLetter.Attempts != 4 || !deadLetter.FailedAt.Equal(failedAt) || deadLetter.Source != "orders" || deadLetter.SourcePosition != "3:41" {
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

func TestKafkaReconcilesTopicConfigsAndRejectsUnsafeState(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "reconcile"}
	backend := &kafkaTransport{}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	admin := &fakeKafkaAdmin{
		metadata: kafkaAdminMetadata(topics, 1, 2),
		configs:  kafkaAdminConfigs(topics, map[string]string{"cleanup.policy": "delete", "retention.ms": "1000"}),
	}
	backend.adm = admin
	if err := backend.reconcileRelatedTopicConfigs(context.Background(), topics, 2, map[string]string{"retention.ms": "2000"}); err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		if admin.altered[topic]["retention.ms"] != "2000" {
			t.Fatalf("topic %q was not reconciled: %#v", topic, admin.altered[topic])
		}
	}
	admin.configs = kafkaAdminConfigs(topics, map[string]string{"cleanup.policy": "compact"})
	if err := backend.reconcileRelatedTopicConfigs(context.Background(), topics, 2, nil); err == nil {
		t.Fatal("unsafe compact policy was accepted")
	}
	admin.configs = kafkaAdminConfigs(topics, map[string]string{"cleanup.policy": "delete"})
	if err := backend.reconcileRelatedTopicConfigs(context.Background(), topics, 3, nil); err == nil {
		t.Fatal("replication mismatch was accepted")
	}
}

func TestKafkaExpandPartitionsReportsPartialSuccess(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "expand"}
	backend := &kafkaTransport{}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	sortedTopics := append([]string(nil), topics...)
	sort.Strings(sortedTopics)
	admin := &fakeKafkaAdmin{metadata: kafkaAdminMetadata(topics, 2, 1), partitionErrTopic: sortedTopics[2]}
	backend.adm = admin
	result, err := backend.ExpandPartitions(context.Background(), topo, 4)
	var partial *PartitionExpansionError
	if !errors.As(err, &partial) || result.Previous != 2 || result.Target != 4 || partial.Topic != sortedTopics[2] {
		t.Fatalf("result=%#v error=%v", result, err)
	}
	if len(result.Topics) != 2 || !result.Topics[sortedTopics[0]] || !result.Topics[sortedTopics[1]] {
		t.Fatalf("partial topics = %#v", result.Topics)
	}
	if _, err := backend.ExpandPartitions(context.Background(), topo, 1); err == nil {
		t.Fatal("partition decrease was accepted")
	}
}

func TestKafkaExpandPartitionsRejectsMissingTopicMetadata(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "missing-metadata"}
	backend := &kafkaTransport{}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	admin := &fakeKafkaAdmin{metadata: kafkaAdminMetadata(topics[1:], 2, 1)}
	backend.adm = admin
	_, err := backend.ExpandPartitions(context.Background(), topo, 4)
	if err == nil || !strings.Contains(err.Error(), "missing broker metadata") {
		t.Fatalf("missing metadata error = %v", err)
	}
}

func TestKafkaDeadLetterRecordSanitizesMetadataAndBoundsSize(t *testing.T) {
	delivery := &kafkaDelivery{record: &kgo.Record{Topic: "orders", Partition: 0, Offset: 1, Timestamp: time.Now()}}
	message := transport.Message{Payload: []byte(`{"v":3}`)}
	payload, err := delivery.deadLetterRecord(message, transport.Decision{Reason: "contains secret", ErrorCode: "also secret", Cause: hostileKafkaError{}})
	if err != nil {
		t.Fatal(err)
	}
	var record kafkaDLQRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatal(err)
	}
	if record.Reason != "delivery_failure" || record.LastError != "delivery_error" {
		t.Fatalf("metadata = %q/%q", record.Reason, record.LastError)
	}
	message.Payload = make([]byte, transport.MaxEncodedDLQRecordBytes+1)
	if _, err := delivery.deadLetterRecord(message, transport.Decision{Reason: "failure"}); err == nil {
		t.Fatal("oversized DLQ record unexpectedly encoded")
	}
}

type hostileKafkaError struct{}

func (hostileKafkaError) Error() string { panic("must not render") }

func TestKafkaDLQAdminUsesLogicalAppendOnlyCursor(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "jobs"}
	backend := &kafkaTransport{}
	topic := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts:  map[string]map[int32]int64{topic: {0: 0, 1: 0}},
		ends:    map[string]map[int32]int64{topic: {0: 2, 1: 1}},
		commits: make(map[string]map[string]map[int32]int64),
	}
	provisionKafkaSubscriber(backend, admin, topo, "alpha")
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

func TestKafkaDLQRequeueLeaseLossStopsBeforeNextPublishAndCursor(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "requeue-lease-loss"}
	backend := &kafkaTransport{}
	topic := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts:  map[string]map[int32]int64{topic: {0: 0}},
		ends:    map[string]map[int32]int64{topic: {0: 2}},
		commits: make(map[string]map[string]map[int32]int64),
	}
	backend.adm = admin
	backend.scanDLQ = func(context.Context, string, int32, int64, int64, int) ([]*kgo.Record, error) {
		return []*kgo.Record{kafkaAdminRecord(t, topic, 0, 0, "zero"), kafkaAdminRecord(t, topic, 0, 1, "one")}, nil
	}
	var lease *dlqAdminLease
	backend.dlqLease = func(ctx context.Context, _ transport.Topology) (*dlqAdminLease, error) {
		lease = newLocalDLQAdminLease(ctx)
		return lease, nil
	}
	var published []string
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error {
		published = append(published, string(record.Value))
		lease.lose()
		return nil
	})

	err := backend.Requeue(context.Background(), topo, []string{"0:0", "0:1"})
	if !errors.Is(err, errKafkaDLQAdminLeaseLost) {
		t.Fatalf("requeue error = %v, want lease loss", err)
	}
	if !reflect.DeepEqual(published, []string{"zero"}) {
		t.Fatalf("published after lease loss = %q", published)
	}
	if admin.commits[backend.dlqAdminGroup(topo)] != nil {
		t.Fatal("stale requeue advanced the DLQ cursor")
	}
}

func TestKafkaDLQPurgeLeaseLossStopsBeforeCursor(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "purge-lease-loss"}
	backend := &kafkaTransport{}
	topic := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		ends:    map[string]map[int32]int64{topic: {0: 2}},
		commits: make(map[string]map[string]map[int32]int64),
	}
	backend.adm = admin
	var lease *dlqAdminLease
	backend.dlqLease = func(ctx context.Context, _ transport.Topology) (*dlqAdminLease, error) {
		lease = newLocalDLQAdminLease(ctx)
		return lease, nil
	}
	admin.listEndHook = func() { lease.lose() }

	err := backend.Purge(context.Background(), topo)
	if !errors.Is(err, errKafkaDLQAdminLeaseLost) {
		t.Fatalf("purge error = %v, want lease loss", err)
	}
	if admin.commits[backend.dlqAdminGroup(topo)] != nil {
		t.Fatal("stale purge advanced the DLQ cursor")
	}
}

func TestKafkaEventDLQRequeueTargetsSourceSubscriberAndRejectsOldRecords(t *testing.T) {
	topo := transport.Topology{Namespace: "admin", JobType: "events", Kind: transport.EventTopology, Options: map[string]any{kafkaEventLayoutOption: kafkaTargetedLayoutV1}}
	backend := &kafkaTransport{}
	topic := backend.dlqTopic(topo)
	admin := &fakeKafkaAdmin{
		starts: map[string]map[int32]int64{topic: {0: 0}}, ends: map[string]map[int32]int64{topic: {0: 1}},
		commits: make(map[string]map[string]map[int32]int64),
	}
	provisionKafkaSubscriber(backend, admin, topo, "alpha")
	backend.adm = admin
	makeRecord := func(group string) *kgo.Record {
		payload, err := json.Marshal(kafkaDLQRecord{
			Version: kafkaDLQRecordVersion, Message: transport.Message{Payload: []byte("event"), Headers: map[string]string{kafkaTargetGroupHeader: "forged"}},
			FailedAt: time.Now().UnixMilli(), SourceTopic: backend.mainTopic(topo), SourceGroup: group,
			SourcePartition: 0, SourceOffset: 0, SourceTimestamp: time.Now().UnixMilli(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return &kgo.Record{Topic: topic, Partition: 0, Offset: 0, Value: payload}
	}
	current := makeRecord("alpha")
	backend.scanDLQ = func(context.Context, string, int32, int64, int64, int) ([]*kgo.Record, error) {
		return []*kgo.Record{current}, nil
	}
	var produced *kgo.Record
	backend.producer = settlementProducer(func(_ context.Context, record *kgo.Record) error { produced = record; return nil })
	if err := backend.Requeue(context.Background(), topo, []string{"0:0"}); err != nil {
		t.Fatal(err)
	}
	if produced == nil || produced.Topic != backend.privateReadyTopic(topo, "alpha") || recordMessage(produced).Headers != nil {
		t.Fatalf("requeued record = %#v", produced)
	}
	admin.commits = make(map[string]map[string]map[int32]int64)
	current = makeRecord("")
	produced = nil
	if err := backend.Requeue(context.Background(), topo, []string{"0:0"}); err == nil {
		t.Fatal("old event DLQ record without source group was requeued")
	}
	if produced != nil || admin.commits[backend.dlqAdminGroup(topo)] != nil {
		t.Fatal("failed-closed requeue mutated destination or cursor")
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
	admin.commits[topo.ConsumerName("")] = map[string]map[int32]int64{main: {0: 4}}
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
	if admin.listStartTotal != 1 || admin.listEndCalls != 1 {
		t.Fatalf("offset snapshots = %d start / %d end, want 1/1", admin.listStartTotal, admin.listEndCalls)
	}
	for group, calls := range admin.fetchOffsetCalls {
		if calls != 1 {
			t.Fatalf("group %q offset fetches = %d, want 1", group, calls)
		}
	}
}

func TestKafkaLogicalOffsetsRetriesTransientLeaderError(t *testing.T) {
	topo := transport.Topology{Namespace: "stats", JobType: "leader-retry"}
	backend := &kafkaTransport{}
	topic := backend.mainTopic(topo)
	admin := &fakeKafkaAdmin{
		starts: map[string]map[int32]int64{topic: {0: 0}},
		ends:   map[string]map[int32]int64{topic: {0: 1}},
		listStartResponses: []kadm.ListedOffsets{
			{topic: {0: kadm.ListedOffset{Topic: topic, Partition: 0, Err: kerr.LeaderNotAvailable}}},
		},
	}
	backend.adm = admin

	starts, ends, err := backend.logicalOffsets(context.Background(), topic, kafkaConsumerGroup(topo, ""))
	if err != nil {
		t.Fatal(err)
	}
	if admin.listStartCalls != 1 || starts[0] != 0 || ends[0] != 1 {
		t.Fatalf("calls=%d starts=%v ends=%v", admin.listStartCalls, starts, ends)
	}
}

func TestKafkaEventStatsAggregateOnlySubscriberGroups(t *testing.T) {
	topo := transport.Topology{Namespace: "stats", JobType: "events", Kind: transport.EventTopology, Options: map[string]any{kafkaEventLayoutOption: kafkaTargetedLayoutV1}}
	backend := &kafkaTransport{}
	main := backend.mainTopic(topo)
	dlq := backend.dlqTopic(topo)
	alphaPrivate := backend.privateReadyTopic(topo, "alpha")
	betaPrivate := backend.privateReadyTopic(topo, "beta")
	alphaMarker := backend.subscriberMarkerTopic(topo, "alpha")
	betaMarker := backend.subscriberMarkerTopic(topo, "beta")
	admin := &fakeKafkaAdmin{
		starts: map[string]map[int32]int64{
			main: {0: 0}, dlq: {0: 0}, alphaPrivate: {0: 0}, betaPrivate: {0: 0},
		},
		ends: map[string]map[int32]int64{
			main: {0: 5}, dlq: {0: 0}, alphaPrivate: {0: 2}, betaPrivate: {0: 1},
		},
		commits: make(map[string]map[string]map[int32]int64),
		groups: []string{
			topo.ConsumerName("alpha"), topo.ConsumerName("beta"), topo.Name() + "-dlq-admin",
			topo.ConsumerName("hostile"), "unrelated-group",
		},
		metadata: kafkaAdminMetadata([]string{alphaPrivate, alphaMarker, betaPrivate, betaMarker}, 1, 1),
	}
	admin.configs = append(
		kafkaAdminConfigs([]string{alphaPrivate, betaPrivate}, map[string]string{"cleanup.policy": "delete"}),
		kafkaAdminConfigs([]string{alphaMarker, betaMarker}, subscriberMarkerConfigs())...,
	)
	admin.commits[topo.ConsumerName("alpha")] = map[string]map[int32]int64{main: {0: 3}, alphaPrivate: {0: 1}}
	admin.commits[topo.ConsumerName("beta")] = map[string]map[int32]int64{main: {0: 4}, betaPrivate: {0: 1}}
	for _, tier := range delayTiers {
		topic := backend.delayTopic(topo, tier)
		admin.starts[topic], admin.ends[topic] = map[int32]int64{0: 0}, map[int32]int64{0: 0}
	}
	backend.adm = admin
	stats, err := backend.Stats(context.Background(), topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 4 || stats.Lag != 4 {
		t.Fatalf("event stats = %#v", stats)
	}
	if admin.listStartTotal != 1 || admin.listEndCalls != 1 {
		t.Fatalf("offset snapshots = %d start / %d end, want 1/1", admin.listStartTotal, admin.listEndCalls)
	}
	if admin.fetchOffsetCalls[topo.ConsumerName("alpha")] != 1 || admin.fetchOffsetCalls[topo.ConsumerName("beta")] != 1 {
		t.Fatalf("subscriber offset fetches = %#v", admin.fetchOffsetCalls)
	}
}

func TestKafkaSubscriptionStatsDoesNotAttributeSharedTargetedDelayLag(t *testing.T) {
	topo := transport.Topology{Namespace: "stats", JobType: "events", Kind: transport.EventTopology}
	backend := &kafkaTransport{}
	group := kafkaConsumerGroup(topo, "alpha")
	main, private := backend.mainTopic(topo), backend.privateReadyTopic(topo, "alpha")
	admin := &fakeKafkaAdmin{starts: make(map[string]map[int32]int64), ends: make(map[string]map[int32]int64), commits: make(map[string]map[string]map[int32]int64)}
	admin.starts[main], admin.ends[main] = map[int32]int64{0: 0}, map[int32]int64{0: 5}
	admin.starts[private], admin.ends[private] = map[int32]int64{0: 0}, map[int32]int64{0: 3}
	admin.commits[group] = map[string]map[int32]int64{main: {0: 4}, private: {0: 1}}
	for i, tier := range delayTiers {
		topic := backend.targetedDelayTopic(topo, tier)
		admin.starts[topic], admin.ends[topic] = map[int32]int64{0: 0}, map[int32]int64{0: int64(i + 2)}
		admin.commits[fmt.Sprintf("%s-target-delay-%s-group", topo.Name(), tier)] = map[string]map[int32]int64{topic: {0: int64(i)}}
	}
	backend.adm = admin
	stats, err := backend.SubscriptionStats(context.Background(), transport.Subscription{Topology: topo, Group: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 3 || stats.Lag != 3 || stats.Delayed != 0 || !stats.LagKnown || stats.OldestAgeKnown {
		t.Fatalf("subscription stats = %#v", stats)
	}
	if admin.listStartTotal != 1 || admin.listEndCalls != 1 || admin.fetchOffsetCalls[group] != 1 {
		t.Fatalf("requests = start:%d end:%d groups:%#v", admin.listStartTotal, admin.listEndCalls, admin.fetchOffsetCalls)
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
