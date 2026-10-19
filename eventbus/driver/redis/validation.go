package redis

import (
	"fmt"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *redisTransport) ValidateTopology(topology transport.Topology) error {
	return t.validateTopologyOptions(topology, true)
}

func (t *redisTransport) validateTopologyOptions(topology transport.Topology, requireEventLayout bool) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	for key, value := range topology.Options {
		switch key {
		case redisEventLayoutOption:
			if topology.Kind != transport.EventTopology || value != redisTargetedLayoutV1 {
				return fmt.Errorf("redis: %s must be %q on event topologies", key, redisTargetedLayoutV1)
			}
		case "redis.maxlen":
			return fmt.Errorf("redis: redis.maxlen is unsafe for durable ready streams and is no longer supported")
		case "redis.dlq_maxlen", "redis.batch_size", "redis.max_ready_entries",
			"redis.max_delayed_entries", "redis.max_dlq_entries", "redis.retention_batch_size":
			v, ok := value.(int64)
			if !ok || v <= 0 {
				return fmt.Errorf("redis: %s must be a positive int64", key)
			}
			if key == "redis.retention_batch_size" && v > 100_000 {
				return fmt.Errorf("redis: redis.retention_batch_size must not exceed 100000")
			}
		case "redis.block", "redis.ack_wait", "redis.retention_interval":
			v, ok := value.(time.Duration)
			if !ok || v <= 0 {
				return fmt.Errorf("redis: %s must be a positive duration", key)
			}
		case "redis.retention_min_age":
			v, ok := value.(time.Duration)
			if !ok || v < 0 {
				return fmt.Errorf("redis: %s must be a non-negative duration", key)
			}
		case "redis.delay_batch_size":
			v, ok := value.(int)
			if !ok || v <= 0 || v > 1000 {
				return fmt.Errorf("redis: %s must be an int between 1 and 1000", key)
			}
		default:
			if strings.HasPrefix(key, "redis.") {
				return fmt.Errorf("redis: unknown option %q", key)
			}
		}
	}
	if requireEventLayout && topology.Kind == transport.EventTopology && topology.Options[redisEventLayoutOption] != redisTargetedLayoutV1 {
		return fmt.Errorf("redis: event topology requires WithEventLayoutTargeted")
	}
	return nil
}
