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

// WithFetchMinBytes sets the minimum response size Kafka should accumulate
// before returning a fetch. Higher values can improve batching at the cost of
// latency. The value applies to main and delayed consumers.
func WithFetchMinBytes(bytes int32) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.fetch_min_bytes", bytes)
}

// WithFetchMaxPartitionBytes bounds bytes returned for one partition in a
// fetch. It must still accommodate the largest accepted message.
func WithFetchMaxPartitionBytes(bytes int32) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.fetch_max_partition_bytes", bytes)
}

// WithMaxBufferedRecords bounds records buffered inside each Kafka consumer,
// independently of EventBus prefetch and PollRecords batch capacity.
func WithMaxBufferedRecords(records int) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.max_buffered_records", records)
}

// WithMaxBufferedBytes bounds bytes buffered inside each Kafka consumer.
func WithMaxBufferedBytes(bytes int) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.max_buffered_bytes", bytes)
}

// WithDelayBatchSize bounds the records each delay tier examines per sweep.
// The default is 100 and the maximum is 1000. Remaining records stay durable
// in Kafka for a later sweep.
func WithDelayBatchSize(size int) eventbus.TopologyOption {
	return eventbus.WithOption("kafka.delay_batch_size", size)
}

// WithDelayDisabled skips delay topics and scheduler clients for a topology
// that never schedules future publishes or retries. Future PublishAt and retry
// settlement fail before broker I/O. Existing/default topologies remain enabled.
func WithDelayDisabled() eventbus.TopologyOption {
	return eventbus.WithDelayDisabled()
}

// WithEventLayoutTargeted opts an event topology into subscriber-targeted
// retries. The explicit marker makes mixed-version deployments fail closed:
// older drivers reject the unknown option instead of broadcasting retries.
func WithEventLayoutTargeted() eventbus.TopologyOption {
	return eventbus.WithOption(kafkaEventLayoutOption, kafkaTargetedLayoutV1)
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
