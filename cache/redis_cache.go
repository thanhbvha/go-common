package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements the Cache interface using Redis.
type RedisCache struct {
	client redis.UniversalClient
}

// NewRedisCache creates a new remote cache instance using a provided Redis client.
// This allows reusing the connection pool from the existing db/redis module.
func NewRedisCache(client redis.UniversalClient) *RedisCache {
	return &RedisCache{
		client: client,
	}
}

// Get retrieves a string value from Redis.
func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	return val, nil
}

// Set stores a string value in Redis with an optional TTL.
// A TTL of 0 means the key does not expire.
func (r *RedisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Delete removes a key from Redis.
func (r *RedisCache) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// Clear flushes the current Redis database.
// WARNING: Use with caution.
func (r *RedisCache) Clear(ctx context.Context) error {
	return r.client.FlushDB(ctx).Err()
}
