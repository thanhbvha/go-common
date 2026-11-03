package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrInvalidRefreshToken = errors.New("auth: invalid refresh token or session")
	ErrRefreshReplay       = errors.New("auth: refresh token replay; session revoked")
	ErrSessionExhausted    = errors.New("auth: session rotation budget exhausted; sign in again")
)

// RefreshSession contains the only plaintext copy of a newly generated refresh
// token. Never log this value. ExpiresAt is an absolute session lifetime; rotation
// does not extend it. Bind ID to access tokens to enforce session revocation.
type RefreshSession struct {
	ID           string
	Subject      string
	RefreshToken string
	ExpiresAt    time.Time
}

// SessionStore owns refresh credentials independently from JWT signing.
type SessionStore interface {
	Create(context.Context, string, time.Duration) (*RefreshSession, error)
	Rotate(context.Context, string, string) (*RefreshSession, error)
	Revoke(context.Context, string) error
	IsRevoked(context.Context, string) (bool, error)
}

type SessionStoreOptions struct {
	Redis     redis.UniversalClient
	Namespace string
	// MaxRotations bounds consumed-token digests kept for replay detection.
	// Zero uses 1024. Sessions exceeding the budget require a new sign-in.
	MaxRotations int
}

type RedisSessionStore struct {
	client       redis.UniversalClient
	prefix       string
	maxRotations int
}

func NewRedisSessionStore(opts SessionStoreOptions) (*RedisSessionStore, error) {
	if opts.Redis == nil || (reflect.ValueOf(opts.Redis).Kind() == reflect.Ptr && reflect.ValueOf(opts.Redis).IsNil()) {
		return nil, ErrRedisNotConfigured
	}
	if strings.TrimSpace(opts.Namespace) == "" || strings.ContainsAny(opts.Namespace, "{}") {
		return nil, errors.New("auth: session namespace is required and must not contain braces")
	}
	if opts.MaxRotations < 0 || opts.MaxRotations > 10000 {
		return nil, errors.New("auth: session rotation budget must be between 1 and 10000")
	}
	if opts.MaxRotations == 0 {
		opts.MaxRotations = 1024
	}
	return &RedisSessionStore{client: opts.Redis, prefix: opts.Namespace + ":session:", maxRotations: opts.MaxRotations}, nil
}

const createSessionScript = `
if redis.call('EXISTS',KEYS[1]) == 1 then return {0} end
local now = redis.call('TIME')
local expiry = tonumber(now[1])*1000 + math.floor(tonumber(now[2])/1000) + tonumber(ARGV[3])
redis.call('HSET',KEYS[1],'subject',ARGV[1],'current',ARGV[2],'state','active','rotations','0','limit',ARGV[4],'expires_at',expiry)
redis.call('PEXPIREAT',KEYS[1],expiry)
return {1,expiry}
`

const rotateSessionScript = `
if redis.call('HGET',KEYS[1],'state') ~= 'active' then return {0} end
local ttl = redis.call('PTTL',KEYS[1])
if ttl <= 0 then return {0} end
local current = redis.call('HGET',KEYS[1],'current')
if current == ARGV[1] then
 local rotations = tonumber(redis.call('HGET',KEYS[1],'rotations'))
 local limit = tonumber(redis.call('HGET',KEYS[1],'limit'))
 if rotations >= limit then
  redis.call('HSET',KEYS[1],'state','revoked')
  redis.call('HDEL',KEYS[1],'current')
  return {-2}
 end
 redis.call('HSET',KEYS[1],'used:'..ARGV[1],'1','current',ARGV[2],'rotations',rotations+1)
 return {1,redis.call('HGET',KEYS[1],'subject'),tonumber(redis.call('HGET',KEYS[1],'expires_at'))}
end
if redis.call('HEXISTS',KEYS[1],'used:'..ARGV[1]) == 1 then
 redis.call('HSET',KEYS[1],'state','revoked')
 redis.call('HDEL',KEYS[1],'current')
 return {-1}
end
return {0}
`

const revokeSessionScript = `
if redis.call('EXISTS',KEYS[1]) == 1 then
 redis.call('HSET',KEYS[1],'state','revoked')
 redis.call('HDEL',KEYS[1],'current')
end
return 1
`

func randomHex(size int) (string, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
func digestRefresh(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
func (s *RedisSessionStore) key(id string) (string, error) {
	if len(id) != 32 {
		return "", ErrInvalidRefreshToken
	}
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 || strings.ToLower(id) != id {
		return "", ErrInvalidRefreshToken
	}
	// One hash per session; the explicit hash tag also supports future atomic
	// operations within the same Redis Cluster slot without embedding user input.
	return s.prefix + "{" + id + "}", nil
}

func (s *RedisSessionStore) Create(ctx context.Context, subject string, ttl time.Duration) (*RefreshSession, error) {
	if strings.TrimSpace(subject) == "" || len(subject) > 1024 {
		return nil, ErrInvalidIdentity
	}
	if ttl < time.Millisecond || ttl%time.Millisecond != 0 {
		return nil, ErrInvalidDuration
	}
	for range 3 {
		id, err := randomHex(16)
		if err != nil {
			return nil, err
		}
		token, err := randomHex(32)
		if err != nil {
			return nil, err
		}
		key, _ := s.key(id)
		values, err := s.client.Eval(ctx, createSessionScript, []string{key}, subject, digestRefresh(token), ttl.Milliseconds(), s.maxRotations).Slice()
		if err != nil {
			return nil, fmt.Errorf("auth: create session: %w", err)
		}

		if len(values) == 1 && values[0] == int64(0) {
			continue
		}
		if len(values) != 2 || values[0] != int64(1) {
			return nil, errors.New("auth: malformed session creation result")
		}
		expiry, ok := values[1].(int64)
		if !ok || expiry <= 0 {
			return nil, errors.New("auth: malformed session expiry")
		}
		return &RefreshSession{ID: id, Subject: subject, RefreshToken: token, ExpiresAt: time.UnixMilli(expiry)}, nil
	}
	return nil, errors.New("auth: could not allocate a session identity")
}

func (s *RedisSessionStore) Rotate(ctx context.Context, id, token string) (*RefreshSession, error) {
	key, err := s.key(id)
	if err != nil {
		return nil, err
	}
	if len(token) != 64 {
		return nil, ErrInvalidRefreshToken
	}
	if _, err := hex.DecodeString(token); err != nil {
		return nil, ErrInvalidRefreshToken
	}
	replacement, err := randomHex(32)
	if err != nil {
		return nil, err
	}
	values, err := s.client.Eval(ctx, rotateSessionScript, []string{key}, digestRefresh(token), digestRefresh(replacement)).Slice()
	if err != nil {
		return nil, fmt.Errorf("auth: rotate session: %w", err)
	}
	if len(values) == 0 {
		return nil, errors.New("auth: malformed session rotation result")
	}
	code, ok := values[0].(int64)
	if !ok {
		return nil, errors.New("auth: malformed session rotation result")
	}
	switch code {
	case -1:
		return nil, ErrRefreshReplay
	case -2:
		return nil, ErrSessionExhausted
	case 1:
		if len(values) != 3 {
			return nil, errors.New("auth: malformed session rotation result")
		}
		subject, subjectOK := values[1].(string)
		expiry, expiryOK := values[2].(int64)
		if !subjectOK || !expiryOK || expiry <= 0 {
			return nil, errors.New("auth: malformed session rotation result")
		}
		return &RefreshSession{ID: id, Subject: subject, RefreshToken: replacement, ExpiresAt: time.UnixMilli(expiry)}, nil
	default:
		return nil, ErrInvalidRefreshToken
	}
}
func (s *RedisSessionStore) Revoke(ctx context.Context, id string) error {
	key, err := s.key(id)
	if err != nil {
		return err
	}
	return s.client.Eval(ctx, revokeSessionScript, []string{key}).Err()
}

// IsRevoked also reports unknown/expired sessions as revoked, and propagates
// backend errors. GenericManager can use this with a TokenID extractor returning
// the access token's session ID instead of its per-token JTI.
func (s *RedisSessionStore) IsRevoked(ctx context.Context, id string) (bool, error) {
	key, err := s.key(id)
	if err != nil {
		return true, err
	}
	state, err := s.client.HGet(ctx, key, "state").Result()
	if errors.Is(err, redis.Nil) {
		return true, nil
	}
	if err != nil {
		return true, classifyRevocationError(err)
	}
	return state != "active", nil
}
