package redis

import (
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestValidateTopologyRejectsInvalidRedisOptions(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value any
	}{
		{name: "maxlen zero", key: "redis.maxlen", value: int64(0)},
		{name: "batch wrong type", key: "redis.batch_size", value: 1},
		{name: "block negative", key: "redis.block", value: -time.Second},
		{name: "ack wait zero", key: "redis.ack_wait", value: time.Duration(0)},
		{name: "delay batch zero", key: "redis.delay_batch_size", value: 0},
		{name: "delay batch unbounded", key: "redis.delay_batch_size", value: 1001},
		{name: "unknown", key: "redis.unknown", value: 1},
	}

	validator := &redisTransport{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topology := transport.Topology{JobType: "task", Options: map[string]any{tt.key: tt.value}}
			if err := validator.ValidateTopology(topology); err == nil {
				t.Fatal("ValidateTopology unexpectedly succeeded")
			}
		})
	}
}

func TestConsumerBatchSizeIsBoundedByCoreCapacity(t *testing.T) {
	sub := transport.Subscription{
		Topology:    transport.Topology{Options: map[string]any{"redis.batch_size": int64(100)}},
		Concurrency: 3, Prefetch: 5,
	}
	if got := consumerBatchSize(sub); got != 8 {
		t.Fatalf("consumer batch size = %d, want 8", got)
	}
}
