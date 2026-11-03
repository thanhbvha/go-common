package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// Ownership and validation errors. Contention itself returns (false, nil).
	ErrEntropyUnavailable    = errors.New("cache: ownership token entropy unavailable")
	ErrLeaseLost             = errors.New("cache: lease lost")
	ErrLockSessionUsed       = errors.New("cache: ownership session already used for key; create NewSession")
	ErrInvalidLock           = errors.New("cache: client, key and TTL >= 1ms required")
	ErrLockAcquisitionFailed = errors.New("cache: failed to acquire distributed lock")
)

// RedisLock implements DistributedLock using Redis SETNX with a random token to prevent accidental release.
type RedisLock struct {
	client  redis.UniversalClient
	token   string // unique identifier for this ownership session
	mu      sync.Mutex
	used    map[string]bool
	entropy io.Reader
}

// NewRedisLock creates an ownership session with a cryptographically random
// token. It returns client-validation or entropy errors without creating a lock.
func NewRedisLock(client redis.UniversalClient) (*RedisLock, error) {
	return newRedisLock(client, rand.Reader)
}

// newRedisLock supplies an instance-local entropy seam for deterministic failure
// tests. Production sessions use crypto/rand.Reader, which supports concurrency.
func newRedisLock(client redis.UniversalClient, entropy io.Reader) (*RedisLock, error) {
	if nilDependency(client) {
		return nil, ErrInvalidLock
	}
	if nilDependency(entropy) {
		return nil, ErrEntropyUnavailable
	}
	tokenBytes := make([]byte, 16)
	if _, err := io.ReadFull(entropy, tokenBytes); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrEntropyUnavailable, err)
	}
	return &RedisLock{client: client, token: hex.EncodeToString(tokenBytes), used: make(map[string]bool), entropy: entropy}, nil
}

// NewSession returns an independent ownership token backed by the same client.
// Use a new session for each execution so a stale owner cannot renew/release a
// later execution's lease, even when one RedisLock instance is shared.
func (l *RedisLock) NewSession() (*RedisLock, error) {
	if l == nil || nilDependency(l.client) {
		return nil, errors.New("cache: nil Redis lock/client")
	}
	return newRedisLock(l.client, l.entropy)
}

// Acquire attempts to acquire a lock with TTL >= 1ms. Use a fresh NewSession
// after a successful or ambiguous attempt for this key; ownership is never reused.
func (l *RedisLock) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if l == nil || nilDependency(l.client) || key == "" || ttl < time.Millisecond {
		return false, ErrInvalidLock
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.used[key] {
		return false, ErrLockSessionUsed
	}
	success, err := l.client.SetNX(ctx, key, l.token, ttl).Result()
	if success || err != nil {
		l.used[key] = true
	}
	if err != nil {
		return false, fmt.Errorf("redis error during SetNX: %w", err)
	}
	return success, nil
}

// Release releases the lock using a Lua script to ensure atomic check-and-delete.
// It ensures that a client only deletes the lock if it holds the correct token.
func (l *RedisLock) Release(ctx context.Context, key string) error {
	if l == nil || nilDependency(l.client) || key == "" {
		return ErrInvalidLock
	}
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
		return ErrLeaseLost
	}

	return nil
}

// Extend extends the TTL of a lock, ensuring the caller actually holds the lock via token check.
func (l *RedisLock) Extend(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if l == nil || nilDependency(l.client) || key == "" || ttl < time.Millisecond {
		return false, ErrInvalidLock
	}
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
