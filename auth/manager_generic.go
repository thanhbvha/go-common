package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
)

const (
	refreshTokenPrefix     = "auth:rt:"
	blacklistPrefix        = "auth:bl:"
	refreshTokenHashPrefix = "sha256:"
)

// GenericManager defines the interface for managing generic JWT tokens,
// refresh tokens, and token revocation via a Redis backend.
type GenericManager[T jwt.Claims] interface {
	// GenerateToken creates a signed JWT from the provided claims.
	GenerateToken(claims T) (string, error)

	// VerifyToken parses and validates a signed JWT string.
	// The provided emptyClaims instance will be populated if valid.
	VerifyToken(tokenString string, emptyClaims T) error

	// GenerateRefreshToken creates a secure random refresh token and stores only
	// its digest in Redis with the given expiration.
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

// ContextGenericManager extends GenericManager with context-aware signing and
// validation for remote key providers and revocation backends.
type ContextGenericManager[T jwt.Claims] interface {
	GenericManager[T]
	GenerateTokenContext(ctx context.Context, claims T) (string, error)
	VerifyTokenContext(ctx context.Context, tokenString string, emptyClaims T) error
}

type genericManager[T jwt.Claims] struct {
	signingKeys       KeyProvider
	redisClient       redis.UniversalClient
	revocation        RevocationChecker
	tokenID           TokenIDExtractor[T]
	issuer            string
	audience          []string
	leeway            time.Duration
	maxLifetime       time.Duration
	clock             Clock
	maxTokenSize      int
	maxPayloadSize    int
	allowLegacyClaims bool
	parser            *jwt.Parser
}

// GenericOptions holds configuration for creating a GenericManager.
//
// Deprecated: use GenericManagerOptions and NewGenericManagerWithOptions.
type GenericOptions struct {
	SecretKey string
	Redis     redis.UniversalClient // Optional: Required only for refresh tokens & revocation
}

// GenericManagerOptions configures generic JWT issuance, validation, key
// rotation, size limits, and optional revocation.
type GenericManagerOptions[T jwt.Claims] struct {
	SecretKey         string
	SigningKeys       KeyProvider
	Redis             redis.UniversalClient
	Revocation        RevocationChecker
	TokenID           TokenIDExtractor[T]
	Issuer            string
	Audience          []string
	Leeway            time.Duration
	MaxLifetime       time.Duration
	Clock             Clock
	MaxTokenSize      int
	MaxPayloadSize    int
	AllowLegacyClaims bool
}

// NewGenericManager creates a new GenericManager instance.
//
// Deprecated: use NewGenericManagerWithOptions. This compatibility constructor
// accepts legacy claims without exp, iat, nbf, or sub and has no key rotation.
func NewGenericManager[T jwt.Claims](opts GenericOptions) (GenericManager[T], error) {
	return NewGenericManagerWithOptions(GenericManagerOptions[T]{
		SecretKey:         opts.SecretKey,
		Redis:             opts.Redis,
		AllowLegacyClaims: true,
	})
}

// NewGenericManagerWithOptions creates a policy-driven generic JWT manager.
func NewGenericManagerWithOptions[T jwt.Claims](opts GenericManagerOptions[T]) (ContextGenericManager[T], error) {
	if opts.MaxTokenSize < 0 || opts.MaxPayloadSize < 0 {
		return nil, ErrInvalidDuration
	}
	if opts.Revocation != nil && opts.TokenID == nil {
		return nil, ErrTokenIDExtractorRequired
	}
	if opts.TokenID != nil && opts.Revocation == nil && opts.Redis == nil {
		return nil, ErrRedisNotConfigured
	}
	provider := opts.SigningKeys
	if provider == nil {
		if len(opts.SecretKey) < 32 {
			return nil, ErrInvalidKey
		}
		var err error
		provider, err = singleKeyProvider(opts.SecretKey)
		if err != nil {
			return nil, err
		}
	}
	issuer, audience, leeway, maxLifetime, clock, err := normalizeTokenOptions(opts.Issuer, opts.Audience, opts.Leeway, opts.MaxLifetime, opts.Clock)
	if err != nil {
		return nil, err
	}
	return &genericManager[T]{
		signingKeys:       provider,
		redisClient:       opts.Redis,
		revocation:        opts.Revocation,
		tokenID:           opts.TokenID,
		issuer:            issuer,
		audience:          audience,
		leeway:            leeway,
		maxLifetime:       maxLifetime,
		clock:             clock,
		maxTokenSize:      normalizeSizeLimit(opts.MaxTokenSize, DefaultMaxTokenSize),
		maxPayloadSize:    normalizeSizeLimit(opts.MaxPayloadSize, DefaultMaxPayloadSize),
		allowLegacyClaims: opts.AllowLegacyClaims,
		parser:            jwt.NewParser(genericParserOptions(issuer, audience, leeway, clock, !opts.AllowLegacyClaims)...),
	}, nil
}

func (m *genericManager[T]) GenerateToken(claims T) (string, error) {
	return m.GenerateTokenContext(context.Background(), claims)
}

func (m *genericManager[T]) GenerateTokenContext(ctx context.Context, claims T) (string, error) {
	if m == nil {
		return "", ErrInvalidManager
	}
	if isNilValue(claims) {
		return "", fmt.Errorf("%w: claims must not be nil", ErrInvalidToken)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("auth: marshal claims: %w", err)
	}
	if len(payload) > m.maxPayloadSize {
		return "", ErrPayloadTooLarge
	}
	if err := validateGenericClaims(claims, m.maxLifetime, m.allowLegacyClaims, m.issuer, m.audience, m.clock(), m.leeway); err != nil {
		return "", err
	}
	if m.tokenID != nil {
		tokenID, err := m.tokenID(claims)
		if err != nil || strings.TrimSpace(tokenID) == "" {
			return "", fmt.Errorf("%w: invalid token ID", ErrInvalidToken)
		}
	}
	key, err := currentKey(ctx, m.signingKeys, false)
	if err != nil {
		return "", err
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = key.ID
	signedToken, err := token.SignedString(key.Value)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}
	if len(signedToken) > m.maxTokenSize {
		return "", ErrTokenTooLarge
	}
	return signedToken, nil
}

func (m *genericManager[T]) VerifyToken(tokenString string, claims T) error {
	return m.VerifyTokenContext(context.Background(), tokenString, claims)
}

func (m *genericManager[T]) VerifyTokenContext(ctx context.Context, tokenString string, claims T) error {
	if m == nil {
		return ErrInvalidManager
	}
	if strings.TrimSpace(tokenString) == "" || isNilValue(claims) {
		return ErrInvalidToken
	}
	if len(tokenString) > m.maxTokenSize {
		return ErrTokenTooLarge
	}
	token, err := m.parser.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		keyID, _ := t.Header["kid"].(string)
		return resolveKey(ctx, m.signingKeys, keyID, false)
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return ErrExpiredToken
		}
		return fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	if !token.Valid {
		return ErrInvalidToken
	}
	if err := validateGenericClaims(claims, m.maxLifetime, m.allowLegacyClaims, m.issuer, m.audience, m.clock(), m.leeway); err != nil {
		return err
	}
	if m.tokenID != nil {
		tokenID, err := m.tokenID(claims)
		if err != nil || strings.TrimSpace(tokenID) == "" {
			return fmt.Errorf("%w: invalid token ID", ErrInvalidToken)
		}
		checker := m.revocation
		if checker == nil {
			checker = m
		}
		if err := checkRevocation(ctx, checker, tokenID); err != nil {
			return err
		}
	}

	return nil
}

func (m *genericManager[T]) verifyRequestToken(ctx context.Context, tokenString string, claims T) (bool, error) {
	err := m.VerifyTokenContext(ctx, tokenString, claims)
	return m.tokenID != nil, err
}

func (m *genericManager[T]) GenerateRefreshToken(ctx context.Context, userID string, duration time.Duration) (string, error) {
	if m.redisClient == nil {
		return "", ErrRedisNotConfigured
	}
	if strings.TrimSpace(userID) == "" {
		return "", ErrInvalidIdentity
	}
	if duration <= 0 {
		return "", ErrInvalidDuration
	}

	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	rt := hex.EncodeToString(b)

	key := refreshTokenPrefix + userID
	if err := m.redisClient.Set(ctx, key, hashRefreshToken(rt), duration).Err(); err != nil {
		return "", fmt.Errorf("failed to store refresh token: %w", err)
	}

	return rt, nil
}

func (m *genericManager[T]) VerifyRefreshToken(ctx context.Context, userID, token string) (bool, error) {
	if m.redisClient == nil {
		return false, ErrRedisNotConfigured
	}
	if strings.TrimSpace(userID) == "" || token == "" {
		return false, nil
	}

	key := refreshTokenPrefix + userID
	storedToken, err := m.redisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil // not found
		}
		return false, err
	}

	want := hashRefreshToken(token)
	if strings.HasPrefix(storedToken, refreshTokenHashPrefix) {
		return constantTimeEqual(storedToken, want), nil
	}

	// Accept refresh tokens created before hashed-at-rest storage was introduced.
	return constantTimeEqual(storedToken, token), nil
}

func (m *genericManager[T]) RevokeRefreshToken(ctx context.Context, userID string) error {
	if m.redisClient == nil {
		return ErrRedisNotConfigured
	}
	if strings.TrimSpace(userID) == "" {
		return ErrInvalidIdentity
	}
	key := refreshTokenPrefix + userID
	return m.redisClient.Del(ctx, key).Err()
}

func (m *genericManager[T]) RevokeToken(ctx context.Context, tokenID string, expiration time.Duration) error {
	if m.redisClient == nil {
		return ErrRedisNotConfigured
	}
	if strings.TrimSpace(tokenID) == "" {
		return ErrInvalidIdentity
	}
	if expiration <= 0 {
		return ErrInvalidDuration
	}
	key := blacklistPrefix + tokenID
	return m.redisClient.Set(ctx, key, "revoked", expiration).Err()
}

func (m *genericManager[T]) IsRevoked(ctx context.Context, tokenID string) (bool, error) {
	if m.redisClient == nil {
		return false, ErrRedisNotConfigured
	}
	if strings.TrimSpace(tokenID) == "" {
		return false, ErrInvalidIdentity
	}

	key := blacklistPrefix + tokenID
	count, err := m.redisClient.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check token revocation: %w", err)
	}
	return count > 0, nil
}

func hashRefreshToken(token string) string {
	digest := sha256.Sum256([]byte(token))
	return refreshTokenHashPrefix + hex.EncodeToString(digest[:])
}

func constantTimeEqual(left, right string) bool {
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

func isNilValue[T any](value T) bool {
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func genericParserOptions(issuer string, audience []string, leeway time.Duration, clock Clock, strict bool) []jwt.ParserOption {
	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithTimeFunc(clock),
	}
	if strict {
		options = append(options, jwt.WithExpirationRequired(), jwt.WithIssuedAt())
	}
	if issuer != "" {
		options = append(options, jwt.WithIssuer(issuer))
	}
	if len(audience) > 0 {
		options = append(options, jwt.WithAllAudiences(audience...))
	}
	if leeway > 0 {
		options = append(options, jwt.WithLeeway(leeway))
	}
	return options
}

func validateGenericClaims[T jwt.Claims](claims T, maxLifetime time.Duration, allowLegacy bool, expectedIssuer string, expectedAudience []string, now time.Time, leeway time.Duration) error {
	expiresAt, err := claims.GetExpirationTime()
	if err != nil {
		return fmt.Errorf("%w: invalid expiration", ErrInvalidToken)
	}
	issuedAt, err := claims.GetIssuedAt()
	if err != nil {
		return fmt.Errorf("%w: invalid issued-at", ErrInvalidToken)
	}
	notBefore, err := claims.GetNotBefore()
	if err != nil {
		return fmt.Errorf("%w: invalid not-before", ErrInvalidToken)
	}
	subject, err := claims.GetSubject()
	if err != nil {
		return fmt.Errorf("%w: invalid subject", ErrInvalidToken)
	}
	if !allowLegacy && (expiresAt == nil || issuedAt == nil || notBefore == nil || strings.TrimSpace(subject) == "") {
		return ErrInvalidToken
	}
	issuer, err := claims.GetIssuer()
	if err != nil || (expectedIssuer != "" && issuer != expectedIssuer) {
		return ErrInvalidToken
	}
	audience, err := claims.GetAudience()
	if err != nil {
		return ErrInvalidToken
	}
	for _, expected := range expectedAudience {
		found := false
		for _, actual := range audience {
			if actual == expected {
				found = true
				break
			}
		}
		if !found {
			return ErrInvalidToken
		}
	}
	if expiresAt != nil && issuedAt != nil {
		if expiresAt.Before(issuedAt.Time) || (maxLifetime > 0 && expiresAt.Sub(issuedAt.Time) > maxLifetime) {
			return ErrInvalidToken
		}
	}
	if expiresAt != nil && notBefore != nil && expiresAt.Before(notBefore.Time) {
		return ErrInvalidToken
	}
	if !allowLegacy {
		if !expiresAt.After(now.Add(-leeway)) || issuedAt.After(now.Add(leeway)) || notBefore.After(now.Add(leeway)) {
			return ErrInvalidToken
		}
	}
	return nil
}
