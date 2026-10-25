package redis

import (
	"context"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *redisTransport) validateEventLayout(ctx context.Context, topo transport.Topology) error {
	if topo.Kind != transport.EventTopology {
		return nil
	}
	marker, err := t.rc.HGet(ctx, t.metadataKey(topo), "event_layout").Result()
	if errors.Is(err, goredis.Nil) {
		return fmt.Errorf("redis: event layout marker is missing; run EnsureTopology with WithEventLayoutTargeted")
	}
	if err != nil {
		return fmt.Errorf("redis: inspect event layout marker: %w", err)
	}
	if marker != redisTargetedLayoutV1 {
		return fmt.Errorf("redis: event layout marker mismatch: got %q, want %q", marker, redisTargetedLayoutV1)
	}
	return nil
}
