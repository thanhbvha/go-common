package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

var transferScript = goredis.NewScript(`
local source = KEYS[1]
local target = KEYS[2]
local mode = ARGV[3]
if mode ~= "retry" and mode ~= "dlq" then
    return redis.error_reply("invalid transfer mode")
end
if redis.call("TYPE", source).ok ~= "stream" then
    return redis.error_reply("settlement source must be a stream")
end
-- XPENDING also checks group existence before any destination write.
local pending = redis.call("XPENDING", source, ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return 0 end
if ARGV[10] ~= "" and pending[1][2] ~= ARGV[10] then return -1 end
local target_type = redis.call("TYPE", target).ok
local expected = "stream"
if mode == "retry" then expected = "zset" end
if target_type ~= "none" and target_type ~= expected then
    return redis.error_reply("invalid settlement destination type")
end
local capacity = tonumber(ARGV[11])
if mode == "retry" then
    local current = redis.call("ZCARD", target)
    if not redis.call("ZSCORE", target, ARGV[4]) and current >= capacity then return -2 end
else
    if redis.call("XLEN", target) >= capacity then return -2 end
end
if mode == "retry" then
    local score = tonumber(ARGV[5])
    if not score then return redis.error_reply("invalid retry timestamp") end
    redis.call("ZADD", target, score, ARGV[4])
else
    redis.call("XADD", target, "*", "payload", ARGV[4],
        "reason", ARGV[6], "error", ARGV[7], "attempts", ARGV[8],
        "failed_at", ARGV[9], "source_stream", source,
        "source_group", ARGV[1], "source_id", ARGV[2])
end
redis.call("XACK", source, ARGV[1], ARGV[2])
return 1
`)

func (d *redisDelivery) transfer(ctx context.Context, decision transport.Decision) error {
	if err := d.transport.ValidateTopology(d.topo); err != nil {
		return err
	}
	if d.stream != d.transport.streamKey(d.topo) || d.group == "" || d.xmsgID == "" {
		return fmt.Errorf("redis: invalid settlement source")
	}
	msg := d.msg
	if decision.Source != nil {
		msg = *decision.Source
	}
	target := d.transport.dlqKey(d.topo)
	capacity := maxDLQEntries(d.topo)
	resource := "dlq"
	if decision.Kind == transport.DecisionRetry {
		msg = decision.Next
		target = d.transport.delayedKey(d.topo)
		capacity = maxDelayedEntries(d.topo)
		resource = "delayed"
		if decision.RetryAt.IsZero() {
			return fmt.Errorf("redis: retry deadline required")
		}
	}
	if len(msg.Payload) == 0 {
		return fmt.Errorf("redis: transfer payload required")
	}
	failedAt := decision.FailedAt
	if failedAt.IsZero() {
		failedAt = time.Now()
	}
	result, err := transferScript.Run(ctx, d.transport.rc, []string{d.stream, target},
		d.group, d.xmsgID, decision.Kind.String(), msg.Payload,
		decision.RetryAt.UnixMilli(), decision.Reason, decisionError(decision),
		decision.Attempts, failedAt.UnixMilli(), d.consumer, capacity).Int64()
	if err != nil {
		return fmt.Errorf("redis: atomic %s settlement: %w", decision.Kind, err)
	}
	if result == 0 {
		return transport.ErrDeliverySettled
	}
	if result == -1 {
		return transport.ErrLeaseLost
	}
	if result == -2 {
		var current int64
		if decision.Kind == transport.DecisionRetry {
			current, _ = d.transport.rc.ZCard(ctx, target).Result()
		} else {
			current, _ = d.transport.rc.XLen(ctx, target).Result()
		}
		return d.transport.capacityError(d.topo, resource, current, capacity)
	}
	return nil
}
