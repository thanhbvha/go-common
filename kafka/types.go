package kafka

import (
	"context"
	"time"
)

// Header is a key-value pair attached to a Kafka message.
// Headers allow consumers to inspect metadata without deserialising the value.
type Header struct {
	Key   string
	Value []byte
}

// Message is the unit of data sent to Kafka via the producer.
type Message struct {
	// Topic is the destination Kafka topic. Required.
	Topic string

	// Partition is the target partition index.
	// Set to -1 (or leave zero) to let the partitioner decide based on Key.
	Partition int32

	// Key is used by the default partitioner to route the message to a
	// consistent partition. Messages with the same non-nil Key always land
	// on the same partition (within a stable cluster topology).
	// Also used by idempotent producers for deduplication.
	Key []byte

	// Value is the message payload. Encoded by the caller (e.g. JSON, Protobuf).
	Value []byte

	// Headers are optional metadata key-value pairs attached to the message.
	Headers []Header

	// Timestamp overrides the producer record timestamp.
	// Zero value means the broker assigns the current time.
	Timestamp time.Time
}

// ProduceResult holds the broker-assigned metadata returned after a successful
// produce operation.
type ProduceResult struct {
	// Topic is the topic the message was written to.
	Topic string

	// Partition is the partition the message was written to.
	Partition int32

	// Offset is the offset assigned by the broker.
	Offset int64

	// Timestamp is the timestamp assigned by the broker.
	Timestamp time.Time
}

// ConsumeRecord is a message received from Kafka by the consumer.
type ConsumeRecord struct {
	// Topic is the topic the record was read from.
	Topic string

	// Partition is the partition the record was read from.
	Partition int32

	// Offset is the record's offset within the partition.
	Offset int64

	// Key is the record key (may be nil).
	Key []byte

	// Value is the record payload.
	Value []byte

	// Headers are the metadata key-value pairs attached to the record.
	Headers []Header

	// Timestamp is the record's timestamp as assigned by the broker.
	Timestamp time.Time
}

// RecordHandler is the callback invoked for each ConsumeRecord.
// The offset is committed only when the handler returns nil.
// Returning a non-nil error causes the framework to retry or route to DLQ
// depending on the queue configuration.
type RecordHandler func(ctx context.Context, record ConsumeRecord) error

// TopicPartition identifies a specific partition within a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// TopicInfo holds a simplified description of a Kafka topic.
type TopicInfo struct {
	// Name is the topic name.
	Name string

	// Partitions is the number of partitions.
	Partitions int32

	// ReplicationFactor is the replication factor.
	ReplicationFactor int16

	// Configs holds topic-level configuration key-value pairs
	// (e.g. "retention.ms", "cleanup.policy").
	Configs map[string]string
}
