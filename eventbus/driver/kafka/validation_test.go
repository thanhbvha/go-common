package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestValidateTopologyRejectsInvalidKafkaOptions(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value any
	}{
		{name: "partitions zero", key: "kafka.partitions", value: int32(0)},
		{name: "partitions wrong type", key: "kafka.partitions", value: 1},
		{name: "replication zero", key: "kafka.replication_factor", value: int16(0)},
		{name: "fetch wait negative", key: "kafka.fetch_max_wait", value: -time.Second},
		{name: "fetch bytes zero", key: "kafka.fetch_max_bytes", value: int32(0)},
		{name: "retention wrong type", key: "kafka.retention_ms", value: "forever"},
		{name: "nil topic configs", key: "kafka.topic_configs", value: map[string]string(nil)},
		{name: "blank topic config key", key: "kafka.topic_configs", value: map[string]string{" ": "value"}},
		{name: "unknown", key: "kafka.unknown", value: 1},
	}

	validator := &kafkaTransport{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := transport.Topology{JobType: "task", Options: map[string]any{tt.key: tt.value}}
			if err := validator.ValidateTopology(topology); err == nil {
				t.Fatal("ValidateTopology unexpectedly succeeded")
			}
		})
	}
}

func TestValidateTopologyEncodesInvalidKafkaCharactersAndRejectsOversizeName(t *testing.T) {
	validator := &kafkaTransport{}
	if err := validator.ValidateTopology(transport.Topology{JobType: "bad topic/一"}); err != nil {
		t.Fatalf("v2 encoded topology rejected: %v", err)
	}
	if err := validator.ValidateTopology(transport.Topology{JobType: string(make([]byte, 249))}); err == nil {
		t.Fatal("oversize encoded topic unexpectedly accepted")
	}
}

func TestKafkaLegacyConsumerGroupMatchesPreV2Layout(t *testing.T) {
	topo := transport.Topology{Namespace: "eventbus", JobType: "email", Kind: transport.EventTopology, Options: map[string]any{
		transport.ResourceNameVersionOption: transport.ResourceNameVersionLegacy,
	}}
	if got := kafkaConsumerGroup(topo, "receipt"); got != "eventbus.event.email-receipt" {
		t.Fatalf("legacy consumer group = %q", got)
	}
}

func TestKafkaRejectsCompactedQueueTopics(t *testing.T) {
	backend := &kafkaTransport{}
	for _, policy := range []string{"compact", "delete,compact", "compact,delete"} {
		topo := transport.Topology{JobType: "jobs", Options: map[string]any{"kafka.topic_configs": map[string]string{"cleanup.policy": policy}}}
		if err := backend.ValidateTopology(topo); err == nil {
			t.Fatalf("accepted cleanup.policy %q", policy)
		}
	}
	topo := transport.Topology{JobType: "jobs", Options: map[string]any{"kafka.topic_configs": map[string]string{"cleanup.policy": "delete"}}}
	if err := backend.ValidateTopology(topo); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaEventLayoutMustBeExplicit(t *testing.T) {
	backend := &kafkaTransport{}
	topology := transport.Topology{JobType: "events", Kind: transport.EventTopology}
	if err := backend.ValidateTopology(topology); err == nil {
		t.Fatal("event topology without rollout marker was accepted")
	}
	WithEventLayoutTargeted()(&topology)
	if err := backend.ValidateTopology(topology); err != nil {
		t.Fatal(err)
	}
	task := transport.Topology{JobType: "tasks", Options: topology.Options}
	if err := backend.ValidateTopology(task); err == nil {
		t.Fatal("event layout option was accepted on task topology")
	}
}

func TestKafkaDeclareEventRejectsMissingTargetedLayout(t *testing.T) {
	bus, err := eventbus.Open(eventbus.DefaultConfig(), eventbus.WithTransport(&kafkaTransport{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareEvent("events"); err == nil {
		t.Fatal("DeclareEvent without targeted layout unexpectedly succeeded")
	}
	if err := bus.DeclareEvent("events", WithEventLayoutTargeted()); err != nil {
		t.Fatalf("DeclareEvent with targeted layout: %v", err)
	}
}

func TestKafkaEventPublishPathsRejectMissingTargetedLayout(t *testing.T) {
	backend := &kafkaTransport{}
	topology := transport.Topology{JobType: "events", Kind: transport.EventTopology}
	message := transport.Message{Payload: []byte("event")}
	if err := backend.Publish(context.Background(), topology, message); err == nil {
		t.Fatal("Publish without targeted layout unexpectedly succeeded")
	}
	if err := backend.PublishAt(context.Background(), topology, message, time.Now().Add(time.Minute)); err == nil {
		t.Fatal("PublishAt without targeted layout unexpectedly succeeded")
	}
}
