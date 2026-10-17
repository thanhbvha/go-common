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
local pending = redis.call("XPENDING", KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then return 0 end
return redis.call("XACK", KEYS[1], ARGV[1], ARGV[2])
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
	n, err := heartbeatScript.Run(ctx, d.transport.rc, []string{d.stream}, d.group, d.xmsgID, d.consumer).Int64()
	if err != nil {
		return fmt.Errorf("redis heartbeat: %w", err)
	}
	if n != 1 {
		return transport.ErrLeaseLost
	}
	return nil
}
