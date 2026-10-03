package auth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/utils/crypt"
)

// GenericEncryptedManager defines the interface for managing generic encrypted JWTs,
// refresh tokens, and token revocation via a Redis backend.
type GenericEncryptedManager[T any] interface {
	// GenerateToken encrypts the payload using AES-256 GCM and creates a signed JWT.
	GenerateToken(payload T, duration time.Duration, aad []byte) (string, error)

	// VerifyToken validates the JWT signature, decrypts the payload, and unmarshals it into emptyPayload.
	VerifyToken(tokenString string, emptyPayload *T, aad []byte) error

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

type genericEncryptedManager[T any] struct {
	jwtSecret   []byte
	aesKey      []byte
	redisClient redis.UniversalClient
}

// GenericEncryptedOptions holds configuration for creating a GenericEncryptedManager.
type GenericEncryptedOptions struct {
	SecretKey string
	AESKey    string
	Redis     redis.UniversalClient // Optional: Required only for refresh tokens & revocation
}

// NewGenericEncryptedManager creates a new GenericEncryptedManager instance.
func NewGenericEncryptedManager[T any](opts GenericEncryptedOptions) (GenericEncryptedManager[T], error) {
	if len(opts.SecretKey) < 32 {
		return nil, ErrInvalidKey
	}
	keyBytes := []byte(opts.AESKey)
	if len(keyBytes) != 32 {
		return nil, crypt.ErrInvalidKeySize
	}

	return &genericEncryptedManager[T]{
		jwtSecret:   []byte(opts.SecretKey),
		aesKey:      keyBytes,
		redisClient: opts.Redis,
	}, nil
}

func (m *genericEncryptedManager[T]) GenerateToken(payload T, duration time.Duration, aad []byte) (string, error) {
	// 1. Serialize T to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	// 2. Encrypt the JSON payload using AES-256 GCM
	ciphertext, err := crypt.EncryptAESGCM(m.aesKey, payloadBytes, aad)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt payload: %w", err)
	}

	// 3. Encode to Base64 to safely embed in JSON
	payloadB64 := base64.StdEncoding.EncodeToString(ciphertext)

	now := time.Now()
	claims := EncryptedClaims{
		Payload: payloadB64,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(duration)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	// 4. Sign the JWT
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString(m.jwtSecret)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return signedToken, nil
}

func (m *genericEncryptedManager[T]) VerifyToken(tokenString string, emptyPayload *T, aad []byte) error {
	// 1. Parse and validate the JWT signature
	token, err := jwt.ParseWithClaims(tokenString, &EncryptedClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.jwtSecret, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return ErrExpiredToken
		}
		return fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*EncryptedClaims)
	if !ok || !token.Valid {
		return ErrInvalidToken
	}

	// 2. Decode the Base64 payload
	ciphertext, err := base64.StdEncoding.DecodeString(claims.Payload)
	if err != nil {
		return fmt.Errorf("%w: invalid base64 encoding", ErrDecryptionFailed)
	}

	// 3. Decrypt the payload using AES-256 GCM
	plaintext, err := crypt.DecryptAESGCM(m.aesKey, ciphertext, aad)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDecryptionFailed, err)
	}

	// 4. Deserialize JSON back to generic type T
	if err := json.Unmarshal(plaintext, emptyPayload); err != nil {
		return fmt.Errorf("%w: invalid json structure", ErrDecryptionFailed)
	}

	return nil
}

// --- Redis Methods (Identical to GenericManager) ---

func (m *genericEncryptedManager[T]) GenerateRefreshToken(ctx context.Context, userID string, duration time.Duration) (string, error) {
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

func (m *genericEncryptedManager[T]) VerifyRefreshToken(ctx context.Context, userID, token string) (bool, error) {
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

func (m *genericEncryptedManager[T]) RevokeRefreshToken(ctx context.Context, userID string) error {
	if m.redisClient == nil {
		return errors.New("auth: redis client is not configured")
	}
	key := refreshTokenPrefix + userID
	return m.redisClient.Del(ctx, key).Err()
}

func (m *genericEncryptedManager[T]) RevokeToken(ctx context.Context, tokenID string, expiration time.Duration) error {
	if m.redisClient == nil {
		return errors.New("auth: redis client is not configured for revocation")
	}
	if tokenID == "" {
		return errors.New("auth: tokenID (jti) is required for revocation")
	}
	key := blacklistPrefix + tokenID
	return m.redisClient.Set(ctx, key, "revoked", expiration).Err()
}

func (m *genericEncryptedManager[T]) IsRevoked(ctx context.Context, tokenID string) (bool, error) {
	if m.redisClient == nil {
		return false, nil
	}
	if tokenID == "" {
		return false, nil
	}

	key := blacklistPrefix + tokenID
	err := m.redisClient.Get(ctx, key).Err()
	if err == nil {
		return true, nil
	}
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return false, err
}
