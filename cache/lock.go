package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrLockAcquisitionFailed is returned when the lock is already held by another client.
	ErrLockAcquisitionFailed = errors.New("cache: failed to acquire distributed lock")
)

// RedisLock implements DistributedLock using Redis SETNX with a random token to prevent accidental release.
type RedisLock struct {
	client redis.UniversalClient
	token  string // unique identifier for the current lock holder
}

// NewRedisLock creates a new DistributedLock instance.
func NewRedisLock(client redis.UniversalClient) *RedisLock {
	// Generate a unique token for this lock instance
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	token := hex.EncodeToString(b)

	return &RedisLock{
		client: client,
		token:  token,
	}
}

// Acquire attempts to acquire a lock for a given key with a specified TTL.
func (l *RedisLock) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	success, err := l.client.SetNX(ctx, key, l.token, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis error during SetNX: %w", err)
	}
	return success, nil
}

// Release releases the lock using a Lua script to ensure atomic check-and-delete.
// It ensures that a client only deletes the lock if it holds the correct token.
func (l *RedisLock) Release(ctx context.Context, key string) error {
	script := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
	`
	res, err := l.client.Eval(ctx, script, []string{key}, l.token).Result()
	if err != nil {
		return fmt.Errorf("redis error during Release script: %w", err)
	}

	if val, ok := res.(int64); ok && val == 0 {
		return errors.New("cache: failed to release lock (not holding the lock or lock expired)")
	}

	return nil
}

// Extend extends the TTL of a lock, ensuring the caller actually holds the lock via token check.
func (l *RedisLock) Extend(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	script := `
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("pexpire", KEYS[1], ARGV[2])
	else
		return 0
	end
	`
	// ARGV[2] is the ttl in milliseconds
	ttlMs := int64(ttl / time.Millisecond)
	res, err := l.client.Eval(ctx, script, []string{key}, l.token, ttlMs).Result()
	if err != nil {
		return false, fmt.Errorf("redis error during Extend script: %w", err)
	}

	if val, ok := res.(int64); ok && val == 1 {
		return true, nil
	}

	return false, nil
}
