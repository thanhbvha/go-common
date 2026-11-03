package cache

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements the Cache interface using Redis.
type RedisCache struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisCache creates a new remote cache instance using a provided Redis client.
// This allows reusing the connection pool from the existing db/redis module.
func NewRedisCache(client redis.UniversalClient, namespace string) (*RedisCache, error) {
	if nilDependency(client) || strings.TrimSpace(namespace) == "" {
		return nil, errors.New("cache: client and namespace required")
	}
	return &RedisCache{client: client, prefix: "cache:" + base64.RawURLEncoding.EncodeToString([]byte(namespace)) + ":"}, nil
}

// Get retrieves a string value from Redis.
func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, r.prefix+key).Result()
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
	if ttl < 0 {
		return errors.New("cache: TTL must not be negative")
	}
	return r.client.Set(ctx, r.prefix+key, value, ttl).Err()
}

// Delete removes a key from Redis.
func (r *RedisCache) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, r.prefix+key).Err()
}

// Clear removes this namespace only. Concurrent writes may survive this
// best-effort scan; Clear is not an atomic cache invalidation barrier.
func (r *RedisCache) Clear(ctx context.Context) error {
	clear := func(client redis.Cmdable) error {
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, r.prefix+"*", 100).Result()
			if err != nil {
				return err
			}
			// Delete per key so this also works across Redis Cluster hash slots.
			for _, key := range keys {
				if err := client.Del(ctx, key).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}
	if cluster, ok := r.client.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, c *redis.Client) error { return clear(c) })
	}
	return clear(r.client)
}

// Close is a no-op for RedisCache. The lifecycle of the underlying Redis client
// is managed externally (e.g. by the redis.Client wrapper); do not close it here.
func (r *RedisCache) Close() error {
	return nil
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Func, reflect.Map, reflect.Slice, reflect.Interface:
		return v.IsNil()
	}
	return false
}
