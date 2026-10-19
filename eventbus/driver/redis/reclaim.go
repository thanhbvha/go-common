package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *redisTransport) reclaimPage(ctx context.Context, sub transport.Subscription, consumer, cursor string, deliver func(transport.Delivery)) (string, error) {
	return t.reclaimStreamPage(ctx, sub, t.streamKey(sub.Topology), consumer, cursor, deliver, reclaimBatchSize(sub))
}

func (t *redisTransport) reclaimStreamPage(ctx context.Context, sub transport.Subscription, stream, consumer, cursor string, deliver func(transport.Delivery), limit int64) (string, error) {
	next, _, err := t.reclaimStreamPageWithRelease(ctx, sub, stream, consumer, cursor, deliver, limit, nil)
	return next, err
}

func (t *redisTransport) reclaimStreamPageWithRelease(ctx context.Context, sub transport.Subscription, stream, consumer, cursor string, deliver func(transport.Delivery), limit int64, release func()) (string, bool, error) {
	idle := 5 * time.Minute
	if value, ok := sub.Topology.Options["redis.ack_wait"].(time.Duration); ok {
		idle = value
	}
	messages, next, err := t.rc.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream: stream, Group: t.groupName(sub), Consumer: consumer,
		Start: cursor, MinIdle: idle, Count: limit,
	}).Result()
	if err != nil {
		return cursor, false, err
	}
	delivered := false
	for _, message := range messages {
		var current bool
		current, err = t.deliverStreamMessageWithRelease(ctx, sub, stream, message, deliver, release, consumer)
		delivered = delivered || current
		if err != nil {
			return next, delivered, err
		}
	}
	if next == "" {
		next = "0-0"
	}
	return next, delivered, nil
}

func reclaimBatchSize(sub transport.Subscription) int64 {
	limit := int64(100)
	if capacity := int64(sub.Concurrency + sub.Prefetch); capacity > 0 {
		limit = min(limit, capacity)
	}
	return limit
}

func (t *redisTransport) deliverMessage(ctx context.Context, sub transport.Subscription, message goredis.XMessage, deliver func(transport.Delivery), expectedConsumer ...string) error {
	return t.deliverStreamMessage(ctx, sub, t.streamKey(sub.Topology), message, deliver, expectedConsumer...)
}

func (t *redisTransport) deliverStreamMessage(ctx context.Context, sub transport.Subscription, stream string, message goredis.XMessage, deliver func(transport.Delivery), expectedConsumer ...string) error {
	_, err := t.deliverStreamMessageWithRelease(ctx, sub, stream, message, deliver, nil, expectedConsumer...)
	return err
}

func (t *redisTransport) deliverStreamMessageWithRelease(ctx context.Context, sub transport.Subscription, stream string, message goredis.XMessage, deliver func(transport.Delivery), release func(), expectedConsumer ...string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	group := t.groupName(sub)
	pending, err := t.rc.XPendingExt(ctx, &goredis.XPendingExtArgs{Stream: stream, Group: group, Start: message.ID, End: message.ID, Count: 1}).Result()
	if err != nil {
		return false, fmt.Errorf("redis delivery history: %w", err)
	}
	// Another worker may have settled a stale claimed entry before dispatch.
	if len(pending) == 0 {
		return false, nil
	}
	if len(expectedConsumer) > 0 && pending[0].Consumer != expectedConsumer[0] {
		return false, nil
	}
	if pending[0].RetryCount < 1 {
		return false, fmt.Errorf("redis: invalid delivery count")
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
		msg: transport.Message{Payload: payload}, consumer: pending[0].Consumer, count: int(pending[0].RetryCount), release: release})
	return true, nil
}
