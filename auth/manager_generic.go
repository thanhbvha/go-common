package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
)

const (
	refreshTokenPrefix = "auth:rt:"
	blacklistPrefix    = "auth:bl:"
)

// GenericManager defines the interface for managing generic JWT tokens,
// refresh tokens, and token revocation via a Redis backend.
type GenericManager[T jwt.Claims] interface {
	// GenerateToken creates a signed JWT from the provided claims.
	GenerateToken(claims T) (string, error)

	// VerifyToken parses and validates a signed JWT string.
	// The provided emptyClaims instance will be populated if valid.
	VerifyToken(tokenString string, emptyClaims T) error

	// GenerateRefreshToken creates a secure random string for use as a refresh token
	// and stores it in Redis with the given expiration.
	GenerateRefreshToken(ctx context.Context, userID string, duration time.Duration) (string, error)

	// VerifyRefreshToken checks if the refresh token is valid in Redis.
	VerifyRefreshToken(ctx context.Context, userID, token string) (bool, error)

	// RevokeRefreshToken deletes a refresh token from Redis.
	RevokeRefreshToken(ctx context.Context, userID string) error

	// RevokeToken adds a JWT token ID (jti) to the Redis blacklist until it expires.
	RevokeToken(ctx context.Context, tokenID string, expiration time.Duration) error

	// IsRevoked checks if a JWT token ID (jti) is in the Redis blacklist.
	IsRevoked(ctx context.Context, tokenID string) (bool, error)
}

type genericManager[T jwt.Claims] struct {
	secretKey   []byte
	redisClient redis.UniversalClient
}

// GenericOptions holds configuration for creating a GenericManager.
type GenericOptions struct {
	SecretKey string
	Redis     redis.UniversalClient // Optional: Required only for refresh tokens & revocation
}

// NewGenericManager creates a new GenericManager instance.
func NewGenericManager[T jwt.Claims](opts GenericOptions) (GenericManager[T], error) {
	if len(opts.SecretKey) < 32 {
		return nil, ErrInvalidKey
	}

	return &genericManager[T]{
		secretKey:   []byte(opts.SecretKey),
		redisClient: opts.Redis,
	}, nil
}

func (m *genericManager[T]) GenerateToken(claims T) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString(m.secretKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}
	return signedToken, nil
}

func (m *genericManager[T]) VerifyToken(tokenString string, claims T) error {
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secretKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return ErrExpiredToken
		}
		return fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	if !token.Valid {
		return ErrInvalidToken
	}

	return nil
}

func (m *genericManager[T]) GenerateRefreshToken(ctx context.Context, userID string, duration time.Duration) (string, error) {
	if m.redisClient == nil {
		return "", errors.New("auth: redis client is not configured for refresh tokens")
	}

	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	rt := hex.EncodeToString(b)

	key := refreshTokenPrefix + userID
	if err := m.redisClient.Set(ctx, key, rt, duration).Err(); err != nil {
		return "", fmt.Errorf("failed to store refresh token: %w", err)
	}

	return rt, nil
}

func (m *genericManager[T]) VerifyRefreshToken(ctx context.Context, userID, token string) (bool, error) {
	if m.redisClient == nil {
		return false, errors.New("auth: redis client is not configured for refresh tokens")
	}

	key := refreshTokenPrefix + userID
	storedToken, err := m.redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil // not found
		}
		return false, err
	}

	return storedToken == token, nil
}

func (m *genericManager[T]) RevokeRefreshToken(ctx context.Context, userID string) error {
	if m.redisClient == nil {
		return errors.New("auth: redis client is not configured")
	}
	key := refreshTokenPrefix + userID
	return m.redisClient.Del(ctx, key).Err()
}

func (m *genericManager[T]) RevokeToken(ctx context.Context, tokenID string, expiration time.Duration) error {
	if m.redisClient == nil {
		return errors.New("auth: redis client is not configured for revocation")
	}
	if tokenID == "" {
		return errors.New("auth: tokenID (jti) is required for revocation")
	}
	key := blacklistPrefix + tokenID
	return m.redisClient.Set(ctx, key, "revoked", expiration).Err()
}

func (m *genericManager[T]) IsRevoked(ctx context.Context, tokenID string) (bool, error) {
	if m.redisClient == nil {
		// If redis is not configured, we assume no tokens are revoked.
		return false, nil
	}
	if tokenID == "" {
		return false, nil // Can't check without ID
	}

	key := blacklistPrefix + tokenID
	err := m.redisClient.Get(ctx, key).Err()
	if err == nil {
		return true, nil // found in blacklist
	}
	if errors.Is(err, redis.Nil) {
		return false, nil // not found
	}
	return false, err
}
