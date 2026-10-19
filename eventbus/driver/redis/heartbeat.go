package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

var heartbeatScript = goredis.NewScript(`
local pending = redis.call("XPENDING", KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end
local ids = redis.call("XCLAIM", KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], "RETRYCOUNT", pending[1][4], "JUSTID")
return #ids
`)

var _ transport.LeaseDelivery = (*redisDelivery)(nil)

var ackOwnedScript = goredis.NewScript(`
if ARGV[4] == "1" and redis.call("HGET", KEYS[2], "capacity_state") ~= "ready" then return redis.error_reply("capacity counters are initializing") end
local pending = redis.call("XPENDING", KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end
local acked = redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
if acked == 1 and ARGV[4] == "1" then
    local deleted = redis.call("XDEL", KEYS[1], ARGV[2])
    if deleted == 1 then
        local current = tonumber(redis.call("HGET", KEYS[2], "ready_entries")) or 1
        redis.call("HSET", KEYS[2], "ready_entries", math.max(0, current - 1))
    end
end
return acked
`)

func (d *redisDelivery) HeartbeatInterval() time.Duration {
	idle := 5 * time.Minute
	if value, ok := d.topo.Options["redis.ack_wait"].(time.Duration); ok {
		idle = value
	}
	return max(time.Nanosecond, idle/3)
}

func (d *redisDelivery) Heartbeat(ctx context.Context) error {
	if d.consumer == "" {
		return transport.ErrLeaseLost
	}
	if err := d.transport.validateEventLayout(ctx, d.topo); err != nil {
		return err
	}
	n, err := heartbeatScript.Run(ctx, d.transport.rc, []string{d.stream}, d.group, d.xmsgID, d.consumer).Int64()
	if err != nil {
		return fmt.Errorf("redis heartbeat: %w", err)
	}
	if n != 1 {
		return transport.ErrLeaseLost
	}
	return nil
}
