package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisFixedWindowLimiter implements a Fixed Window Rate Limiter using Redis.
// It uses a Lua script to ensure atomic increment and TTL setting.
type redisFixedWindowLimiter struct {
	client *redis.Client
	prefix string
}

// NewRedisLimiter creates a new Redis-based Limiter.
func NewRedisLimiter(client *redis.Client, prefix string) Limiter {
	if prefix == "" {
		prefix = "ratelimit"
	}
	return &redisFixedWindowLimiter{
		client: client,
		prefix: prefix,
	}
}

// Lua script for atomic fixed-window rate limiting.
// KEYS[1] = key
// ARGV[1] = limit
// ARGV[2] = window (seconds)
const fixedWindowScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = redis.call("GET", key)
if current and tonumber(current) >= limit then
	local ttl = redis.call("PTTL", key)
	return {0, 0, ttl}
end

current = redis.call("INCR", key)
local ttl = redis.call("PTTL", key)

if current == 1 then
	redis.call("PEXPIRE", key, window * 1000)
	ttl = window * 1000
end

local remaining = limit - current
return {1, remaining, ttl}
`

func (r *redisFixedWindowLimiter) Allow(ctx context.Context, key string, cfg Config) (*Result, error) {
	fullKey := r.prefix + ":" + key
	windowSecs := int64(cfg.Window.Seconds())
	if windowSecs == 0 {
		windowSecs = 1 // Prevent 0-second window
	}

	res, err := r.client.Eval(ctx, fixedWindowScript, []string{fullKey}, cfg.Limit, windowSecs).Result()
	if err != nil {
		return nil, err
	}

	// Lua script returns {allowed (1 or 0), remaining, ttl_in_ms}
	vals, ok := res.([]interface{})
	if !ok || len(vals) < 3 {
		return nil, fmt.Errorf("ratelimit: unexpected response format from Redis script")
	}
	allowedVal, ok := vals[0].(int64)
	if !ok {
		return nil, fmt.Errorf("ratelimit: unexpected type for 'allowed' field in Redis response")
	}
	allowed := allowedVal == 1
	remaining, ok := vals[1].(int64)
	if !ok {
		return nil, fmt.Errorf("ratelimit: unexpected type for 'remaining' field in Redis response")
	}
	ttlMs, ok := vals[2].(int64)
	if !ok {
		return nil, fmt.Errorf("ratelimit: unexpected type for 'ttl' field in Redis response")
	}
	
	if ttlMs < 0 {
		ttlMs = 0
	}

	return &Result{
		Allowed:    allowed,
		Remaining:  int(remaining),
		ResetAfter: time.Duration(ttlMs) * time.Millisecond,
	}, nil
}
