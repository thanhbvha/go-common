package kafka

import (
	"context"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestEnsureEventTopologySeparatesDataAndControlTopicPartitions(t *testing.T) {
	admin := &fakeKafkaAdmin{}
	backend := &kafkaTransport{adm: admin}
	topology := transport.Topology{
		Namespace: "example",
		JobType:   "user_registered",
		Kind:      transport.EventTopology,
		Options: map[string]any{
			"kafka.partitions":     int32(4),
			"kafka.delay_disabled": true,
			kafkaEventLayoutOption: kafkaTargetedLayoutV1,
		},
	}

	if err := backend.EnsureTopology(context.Background(), topology); err != nil {
		t.Fatal(err)
	}
	if len(admin.createTopicCalls) != 2 {
		t.Fatalf("CreateTopics calls = %d, want 2", len(admin.createTopicCalls))
	}
	dataCall := admin.createTopicCalls[0]
	if dataCall.partitions != 4 {
		t.Fatalf("data topic partitions = %d, want 4", dataCall.partitions)
	}
	wantData := map[string]bool{backend.mainTopic(topology): true, backend.dlqTopic(topology): true}
	assertTopicSet(t, dataCall.topics, wantData)

	controlCall := admin.createTopicCalls[1]
	if controlCall.partitions != 1 {
		t.Fatalf("control topic partitions = %d, want 1", controlCall.partitions)
	}
	wantControl := map[string]bool{backend.dlqAdminLockTopic(topology): true, backend.eventLayoutTopic(topology): true}
	assertTopicSet(t, controlCall.topics, wantControl)

	for topic := range wantData {
		if got := len(admin.metadata.Topics[topic].Partitions); got != 4 {
			t.Fatalf("data topic %q partitions = %d, want 4", topic, got)
		}
	}
	for topic := range wantControl {
		if got := len(admin.metadata.Topics[topic].Partitions); got != 1 {
			t.Fatalf("control topic %q partitions = %d, want 1", topic, got)
		}
	}
}

func TestEnsureEventTopologyExplainsLegacyLayoutPartitionMismatch(t *testing.T) {
	backend := &kafkaTransport{}
	topology := transport.Topology{
		Namespace: "example",
		JobType:   "user_registered",
		Kind:      transport.EventTopology,
		Options: map[string]any{
			"kafka.partitions":     int32(4),
			"kafka.delay_disabled": true,
			kafkaEventLayoutOption: kafkaTargetedLayoutV1,
		},
	}
	layoutTopic := backend.eventLayoutTopic(topology)
	admin := &fakeKafkaAdmin{metadata: kafkaAdminMetadata([]string{layoutTopic}, 4, 1)}
	backend.adm = admin

	err := backend.EnsureTopology(context.Background(), topology)
	if err == nil {
		t.Fatal("EnsureTopology accepted a multi-partition event layout topic")
	}
	for _, want := range []string{"must have exactly 1 partition", "use a new EventBus namespace", "disposable infrastructure"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("EnsureTopology error = %q, want %q", err, want)
		}
	}
}

func TestRelatedTopicReplicationReadyRetriesLeaderTransition(t *testing.T) {
	admin := &fakeKafkaAdmin{metadata: kadm.Metadata{Topics: kadm.TopicDetails{
		"control": {Topic: "control", Err: kerr.UnknownTopicOrPartition},
	}}}
	backend := &kafkaTransport{adm: admin}
	ready, err := backend.relatedTopicReplicationReady(context.Background(), []string{"control"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Fatal("leader transition metadata reported ready")
	}
}

func assertTopicSet(t *testing.T, got []string, want map[string]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("topics = %#v, want %#v", got, want)
	}
	for _, topic := range got {
		if !want[topic] {
			t.Fatalf("unexpected topic %q in %#v", topic, got)
		}
	}
}
