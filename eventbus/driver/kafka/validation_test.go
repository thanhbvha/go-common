package kafka

import (
	"testing"
	"time"

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

func TestValidateTopologyRejectsInvalidKafkaTopicName(t *testing.T) {
	if err := (&kafkaTransport{}).ValidateTopology(transport.Topology{JobType: "bad topic"}); err == nil {
		t.Fatal("ValidateTopology unexpectedly accepted an invalid topic name")
	}
}
