package kafka

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func (t *kafkaTransport) ValidateTopology(topology transport.Topology) error {
	if t.producerErr != nil {
		return t.producerErr
	}
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	name := topology.Name()
	if len(name) == 0 || len(name) > 249 || !kafkaTopicName.MatchString(name) {
		return fmt.Errorf("kafka: topology produces invalid topic name %q", name)
	}
	for key, value := range topology.Options {
		switch key {
		case "kafka.partitions":
			v, ok := value.(int32)
			if !ok || v <= 0 {
				return fmt.Errorf("kafka: %s must be a positive int32", key)
			}
		case "kafka.replication_factor":
			v, ok := value.(int16)
			if !ok || v <= 0 {
				return fmt.Errorf("kafka: %s must be a positive int16", key)
			}
		case "kafka.fetch_max_wait", "kafka.retention_ms":
			v, ok := value.(time.Duration)
			if !ok || v <= 0 {
				return fmt.Errorf("kafka: %s must be a positive duration", key)
			}
		case "kafka.fetch_max_bytes":
			v, ok := value.(int32)
			if !ok || v <= 0 {
				return fmt.Errorf("kafka: %s must be a positive int32", key)
			}
		case "kafka.delay_batch_size":
			v, ok := value.(int)
			if !ok || v < 1 || v > maxDelayBatchSize {
				return fmt.Errorf("kafka: %s must be an int between 1 and %d", key, maxDelayBatchSize)
			}
		case "kafka.topic_configs":
			configs, ok := value.(map[string]string)
			if !ok || configs == nil {
				return fmt.Errorf("kafka: %s must be a non-nil map[string]string", key)
			}
			for configKey, configValue := range configs {
				if strings.TrimSpace(configKey) == "" || configKey != strings.TrimSpace(configKey) {
					return fmt.Errorf("kafka: %s contains an invalid key", key)
				}
				if configKey == "cleanup.policy" && !cleanupPolicyDeleteOnly(configValue) {
					return fmt.Errorf("kafka: cleanup.policy must be delete-only for EventBus topics")
				}
			}
		default:
			if strings.HasPrefix(key, "kafka.") {
				return fmt.Errorf("kafka: unknown option %q", key)
			}
		}
	}
	return nil
}

func cleanupPolicyDeleteOnly(value string) bool {
	parts := strings.Split(value, ",")
	return len(parts) == 1 && strings.TrimSpace(parts[0]) == "delete"
}
