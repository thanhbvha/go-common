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
local metadata = KEYS[3]
local groups = KEYS[4]
local mode = ARGV[3]
if mode ~= "retry" and mode ~= "dlq" then
    return redis.error_reply("invalid transfer mode")
end
if (mode == "retry" or ARGV[12] == "1") and redis.call("HGET", metadata, "capacity_state") ~= "ready" then
    return redis.error_reply("capacity counters are initializing")
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
    if ARGV[13] == "1" and redis.call("HGET", groups, ARGV[1]) ~= "active" then return -3 end
    local current = tonumber(redis.call("HGET", metadata, "delayed_entries"))
    if not current then return redis.error_reply("delayed capacity counter is missing") end
    if not redis.call("ZSCORE", target, ARGV[4]) and current >= capacity then return -2 end
else
    if redis.call("XLEN", target) >= capacity then return -2 end
end
if mode == "retry" then
    local score = tonumber(ARGV[5])
    if not score then return redis.error_reply("invalid retry timestamp") end
    local added = redis.call("ZADD", target, score, ARGV[4])
    if added == 1 then redis.call("HINCRBY", metadata, "delayed_entries", 1) end
else
    redis.call("XADD", target, "*", "payload", ARGV[4],
        "reason", ARGV[6], "error", ARGV[7], "attempts", ARGV[8],
        "failed_at", ARGV[9], "source_stream", source,
        "source_group", ARGV[1], "source_id", ARGV[2])
end
redis.call("XACK", source, ARGV[1], ARGV[2])
if ARGV[12] == "1" then
    local deleted = redis.call("XDEL", source, ARGV[2])
    if deleted == 1 then
        local ready = tonumber(redis.call("HGET", metadata, "ready_entries")) or 1
        redis.call("HSET", metadata, "ready_entries", math.max(0, ready - 1))
    end
end
return 1
`)

func (d *redisDelivery) transfer(ctx context.Context, decision transport.Decision) error {
	if err := d.transport.ValidateTopology(d.topo); err != nil {
		return err
	}
	if err := d.transport.ensureCapacityCounters(ctx, d.topo); err != nil {
		return err
	}
	shared := d.transport.streamKey(d.topo)
	private := d.topo.Kind == transport.EventTopology && d.stream == d.transport.subscriberRetryStreamKey(d.topo, d.group)
	if d.stream != shared && !private || d.group == "" || d.xmsgID == "" {
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
		if d.topo.Kind == transport.EventTopology {
			target = d.transport.subscriberRetryDelayedKey(d.topo, d.group)
		}
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
	reason, errorCode := transport.NormalizeDecisionMetadata(decision)
	if decision.Kind == transport.DecisionDeadLetter && len(msg.Payload)+len(reason)+len(errorCode) > transport.MaxEncodedDLQRecordBytes {
		return fmt.Errorf("redis: encoded DLQ record exceeds %d bytes", transport.MaxEncodedDLQRecordBytes)
	}
	result, err := transferScript.Run(ctx, d.transport.rc, []string{d.stream, target, d.transport.metadataKey(d.topo), d.transport.groupManifestKey(d.topo)},
		d.group, d.xmsgID, decision.Kind.String(), msg.Payload,
		decision.RetryAt.UnixMilli(), reason, errorCode,
		decision.Attempts, failedAt.UnixMilli(), d.consumer, capacity, boolInt(private), boolInt(d.topo.Kind == transport.EventTopology)).Int64()
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
		current := int64(0)
		if decision.Kind == transport.DecisionRetry {
			current, _ = d.transport.rc.HGet(ctx, d.transport.metadataKey(d.topo), "delayed_entries").Int64()
		} else {
			current, _ = d.transport.rc.XLen(ctx, target).Result()
		}
		return d.transport.capacityError(d.topo, resource, current, capacity)
	}
	if result == -3 {
		return fmt.Errorf("%w: %s", ErrSubscriptionRetiring, d.group)
	}
	return nil
}
