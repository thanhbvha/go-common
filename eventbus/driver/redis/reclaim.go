package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *redisTransport) reclaimPage(ctx context.Context, sub transport.Subscription, consumer, cursor string, deliver func(transport.Delivery)) (string, error) {
	idle := 5 * time.Minute
	if value, ok := sub.Topology.Options["redis.ack_wait"].(time.Duration); ok {
		idle = value
	}
	limit := int64(100)
	if capacity := int64(sub.Concurrency + sub.Prefetch); capacity > 0 {
		limit = min(limit, capacity)
	}
	messages, next, err := t.rc.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream: t.streamKey(sub.Topology), Group: t.groupName(sub), Consumer: consumer,
		Start: cursor, MinIdle: idle, Count: limit,
	}).Result()
	if err != nil {
		return cursor, err
	}
	for _, message := range messages {
		if err := t.deliverMessage(ctx, sub, message, deliver, consumer); err != nil {
			return next, err
		}
	}
	if next == "" {
		next = "0-0"
	}
	return next, nil
}

func (t *redisTransport) deliverMessage(ctx context.Context, sub transport.Subscription, message goredis.XMessage, deliver func(transport.Delivery), expectedConsumer ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stream, group := t.streamKey(sub.Topology), t.groupName(sub)
	pending, err := t.rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: stream, Group: group, Start: message.ID, End: message.ID, Count: 1}).Result()
	if err != nil {
		return fmt.Errorf("redis delivery history: %w", err)
	}
	// Another worker may have settled a stale claimed entry before dispatch.
	if len(pending) == 0 {
		return nil
	}
	if len(expectedConsumer) > 0 && pending[0].Consumer != expectedConsumer[0] {
		return nil
	}
	if pending[0].RetryCount < 1 {
		return fmt.Errorf("redis: invalid delivery count")
	}
	var payload []byte
	switch value := message.Values["payload"].(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
		// Malformed records reach the core decode-error DLQ path, never auto-ACK.
	}
	deliver(&redisDelivery{transport: t, topo: sub.Topology, stream: stream, group: group, xmsgID: message.ID,
		msg: transport.Message{Payload: payload}, consumer: pending[0].Consumer, count: int(pending[0].RetryCount)})
	return nil
}
