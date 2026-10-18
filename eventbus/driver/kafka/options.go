package kafka

import (
	"time"

	"github.com/thanhbvha/go-common/eventbus"
)

// WithPartitions sets the number of partitions when auto-creating topics.
// Default is 1.
func WithPartitions(partitions int32) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.partitions", partitions)
}

// WithReplicationFactor sets the replication factor when auto-creating topics.
// Default is 1.
func WithReplicationFactor(replFactor int16) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.replication_factor", replFactor)
}

// WithFetchMaxWait sets the maximum amount of time the broker will wait to fill
// the fetch request before returning.
// Default is 500ms.
func WithFetchMaxWait(d time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.fetch_max_wait", d)
}

// WithFetchMaxBytes sets the maximum amount of data the server should return
// for a fetch request.
// Default is 50MB.
func WithFetchMaxBytes(bytes int32) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.fetch_max_bytes", bytes)
}

// WithDelayBatchSize bounds the records each delay tier examines per sweep.
// The default is 100 and the maximum is 1000. Remaining records stay durable
// in Kafka for a later sweep.
func WithDelayBatchSize(size int) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.delay_batch_size", size)
}

// WithRetention sets the retention time for messages in the Kafka topics.
// It translates to the "retention.ms" topic configuration.
func WithRetention(d time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.retention_ms", d)
}

// WithTopicConfigs sets arbitrary key-value configurations for the Kafka topics
// created by the topology (e.g., "cleanup.policy", "segment.bytes").
func WithTopicConfigs(configs map[string]string) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.topic_configs", configs)
}
