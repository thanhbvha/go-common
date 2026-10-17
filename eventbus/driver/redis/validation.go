package redis

import (
	"fmt"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *redisTransport) ValidateTopology(topology transport.Topology) error {
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	for key, value := range topology.Options {
		switch key {
		case "redis.maxlen", "redis.dlq_maxlen", "redis.batch_size":
			v, ok := value.(int64)
			if !ok || v <= 0 {
				return fmt.Errorf("redis: %s must be a positive int64", key)
			}
		case "redis.block", "redis.ack_wait":
			v, ok := value.(time.Duration)
			if !ok || v <= 0 {
				return fmt.Errorf("redis: %s must be a positive duration", key)
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
	return nil
}
