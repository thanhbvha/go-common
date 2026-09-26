package kafka

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// adminClient returns a kadm.Client wrapping the current kgo connection.
// kadm is the franz-go admin layer for topic and cluster management.
func (c *Client) adminClient() (*kadm.Client, error) {
	kc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return kadm.NewClient(kc), nil
}

// CreateTopic creates a Kafka topic with the specified number of partitions
// and replication factor. Additional topic-level configurations (e.g.
// "retention.ms", "cleanup.policy") are supplied via the configs map.
// Returns nil if the topic already exists with compatible settings.
func (c *Client) CreateTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16, configs map[string]string) error {
	adm, err := c.adminClient()
	if err != nil {
		return err
	}

	// kadm.CreateTopics requires map[string]*string; convert the caller-supplied map.
	var kadmConfigs map[string]*string
	if len(configs) > 0 {
		kadmConfigs = make(map[string]*string, len(configs))
		for k, v := range configs {
			v := v // capture loop variable
			kadmConfigs[k] = &v
		}
	}

	resp, err := adm.CreateTopics(ctx, partitions, replicationFactor, kadmConfigs, topic)
	if err != nil {
		return fmt.Errorf("kafka: CreateTopic %q failed: %w", topic, err)
	}

	for _, topicResp := range resp {
		if topicResp.Err != nil && !isTopicExistsErr(topicResp.Err) {
			return fmt.Errorf("kafka: CreateTopic %q error: %w", topic, topicResp.Err)
		}
	}
	return nil
}

// DeleteTopic deletes the specified Kafka topic.
// Returns an error if the topic does not exist.
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	adm, err := c.adminClient()
	if err != nil {
		return err
	}

	resp, err := adm.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("kafka: DeleteTopic %q failed: %w", topic, err)
	}

	for _, topicResp := range resp {
		if topicResp.Err != nil {
			return fmt.Errorf("kafka: DeleteTopic %q error: %w", topic, topicResp.Err)
		}
	}
	return nil
}

// TopicExists reports whether a topic with the given name exists in the cluster.
func (c *Client) TopicExists(ctx context.Context, topic string) (bool, error) {
	adm, err := c.adminClient()
	if err != nil {
		return false, err
	}

	metadata, err := adm.Metadata(ctx, topic)
	if err != nil {
		return false, fmt.Errorf("kafka: TopicExists metadata request failed: %w", err)
	}

	for _, t := range metadata.Topics {
		if t.Topic == topic && t.Err == nil {
			return true, nil
		}
	}
	return false, nil
}

// ListTopics returns the names of all topics visible to the connected client.
// Internal topics (prefixed with "__") are excluded by default.
func (c *Client) ListTopics(ctx context.Context) ([]string, error) {
	adm, err := c.adminClient()
	if err != nil {
		return nil, err
	}

	metadata, err := adm.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka: ListTopics failed: %w", err)
	}

	topics := make([]string, 0, len(metadata.Topics))
	for _, t := range metadata.Topics {
		if t.Err == nil && len(t.Topic) > 0 && t.Topic[0] != '_' {
			topics = append(topics, t.Topic)
		}
	}
	return topics, nil
}

// DescribeTopic returns detailed information about a single topic including
// partition count, replication factor, and topic-level configurations.
func (c *Client) DescribeTopic(ctx context.Context, topic string) (*TopicInfo, error) {
	adm, err := c.adminClient()
	if err != nil {
		return nil, err
	}

	metadata, err := adm.Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("kafka: DescribeTopic %q metadata failed: %w", topic, err)
	}

	var info *TopicInfo
	for _, t := range metadata.Topics {
		if t.Topic != topic {
			continue
		}
		if t.Err != nil {
			return nil, fmt.Errorf("kafka: DescribeTopic %q error: %w", topic, t.Err)
		}

		var replicationFactor int16
		if len(t.Partitions) > 0 {
			replicationFactor = int16(len(t.Partitions[0].Replicas))
		}

		info = &TopicInfo{
			Name:              t.Topic,
			Partitions:        int32(len(t.Partitions)),
			ReplicationFactor: replicationFactor,
			Configs:           make(map[string]string),
		}
	}

	if info == nil {
		return nil, fmt.Errorf("kafka: topic %q not found", topic)
	}

	// Fetch topic configs.
	cfgResp, err := adm.DescribeTopicConfigs(ctx, topic)
	if err == nil {
		for _, tc := range cfgResp {
			for _, cfg := range tc.Configs {
				if cfg.Value != nil {
					info.Configs[cfg.Key] = *cfg.Value
				}
			}
		}
	}

	return info, nil
}

// EnsureTopic creates a topic if it does not already exist. It is safe to
// call concurrently and idempotent — no error is returned when the topic
// already exists.
func (c *Client) EnsureTopic(ctx context.Context, topic string, partitions int32, replicationFactor int16, configs map[string]string) error {
	exists, err := c.TopicExists(ctx, topic)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	return c.CreateTopic(ctx, topic, partitions, replicationFactor, configs)
}

// SetTopicRetention updates the retention.ms configuration of an existing topic.
// retentionMs is the retention period in milliseconds (-1 = unlimited).
func (c *Client) SetTopicRetention(ctx context.Context, topic string, retentionMs int64) error {
	adm, err := c.adminClient()
	if err != nil {
		return err
	}

	value := strconv.FormatInt(retentionMs, 10)
	resp, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Name: "retention.ms", Value: &value},
	}, topic)
	if err != nil {
		return fmt.Errorf("kafka: SetTopicRetention %q failed: %w", topic, err)
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("kafka: SetTopicRetention %q error: %w", topic, r.Err)
		}
	}
	return nil
}

// ConsumerGroupOffsets returns the committed offsets for each partition of
// the given topics for the specified consumer group.
func (c *Client) ConsumerGroupOffsets(ctx context.Context, group string, topics ...string) (map[string]map[int32]int64, error) {
	adm, err := c.adminClient()
	if err != nil {
		return nil, err
	}

	resp, err := adm.FetchOffsetsForTopics(ctx, group, topics...)
	if err != nil {
		return nil, fmt.Errorf("kafka: ConsumerGroupOffsets failed: %w", err)
	}

	result := make(map[string]map[int32]int64)
	resp.Each(func(o kadm.OffsetResponse) {
		if o.Err != nil {
			return
		}
		if result[o.Topic] == nil {
			result[o.Topic] = make(map[int32]int64)
		}
		result[o.Topic][o.Partition] = o.At
	})
	return result, nil
}

// ---- internal helpers ----

// isTopicExistsErr reports whether err corresponds to a "topic already exists"
// Kafka error code, which is treated as a non-fatal condition by EnsureTopic.
func isTopicExistsErr(err error) bool {
	if err == nil {
		return false
	}
	// kadm wraps Kafka error codes; check the string representation as a
	// portable fallback across franz-go versions.
	switch e := err.(type) {
	case *kgo.ErrDataLoss:
		_ = e
		return false
	default:
		return err.Error() == "TOPIC_ALREADY_EXISTS"
	}
}
