// Package auth provides enterprise-grade authentication tools.
// It supports standard JWT and encrypted JWT with optional contextual AAD
// binding. Applications must still apply TLS, secure storage, XSS/CSRF controls,
// rotation, and revocation policies.
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken             = errors.New("auth: invalid token")
	ErrExpiredToken             = errors.New("auth: token is expired")
	ErrTokenRevoked             = errors.New("auth: token is revoked")
	ErrInvalidKey               = errors.New("auth: invalid signing key")
	ErrInvalidDuration          = errors.New("auth: duration must be positive")
	ErrInvalidIdentity          = errors.New("auth: identity must not be empty")
	ErrRedisNotConfigured       = errors.New("auth: redis client is not configured")
	ErrTokenTooLarge            = errors.New("auth: token exceeds configured size limit")
	ErrPayloadTooLarge          = errors.New("auth: payload exceeds configured size limit")
	ErrInvalidManager           = errors.New("auth: manager is nil")
	ErrTokenIDExtractorRequired = errors.New("auth: token ID extractor is required for revocation")
)

// Manager is the core component for standard JWT operations.
type Manager struct {
	signingKeys       KeyProvider
	revocation        RevocationChecker
	issuer            string
	audience          []string
	leeway            time.Duration
	maxLifetime       time.Duration
	clock             Clock
	maxTokenSize      int
	maxPayloadSize    int
	parser            *jwt.Parser
	allowLegacyClaims bool
}

// NewManager creates a new JWT Manager.
// secretKey must be at least 32 characters for adequate HMAC-SHA256 security.
//
// Deprecated: use NewManagerWithOptions. This compatibility constructor
// accepts legacy claims and cannot configure issuer, audience, or revocation.
func NewManager(secretKey string) (*Manager, error) {
	return NewManagerWithOptions(ManagerOptions{SecretKey: secretKey, AllowLegacyClaims: true})
}

// NewManagerWithOptions creates a JWT manager with claim-validation policy.
func NewManagerWithOptions(opts ManagerOptions) (*Manager, error) {
	if opts.MaxTokenSize < 0 || opts.MaxPayloadSize < 0 {
		return nil, ErrInvalidDuration
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
	return &Manager{
		signingKeys:       provider,
		revocation:        opts.Revocation,
		issuer:            issuer,
		audience:          audience,
		leeway:            leeway,
		maxLifetime:       maxLifetime,
		clock:             clock,
		maxTokenSize:      normalizeSizeLimit(opts.MaxTokenSize, DefaultMaxTokenSize),
		maxPayloadSize:    normalizeSizeLimit(opts.MaxPayloadSize, DefaultMaxPayloadSize),
		parser:            jwt.NewParser(parserOptions(issuer, audience, leeway, clock)...),
		allowLegacyClaims: opts.AllowLegacyClaims,
	}, nil
}

// GenerateToken creates a signed JWT for the given user information.
func (m *Manager) GenerateToken(user UserInfo, duration time.Duration) (string, error) {
	return m.GenerateTokenContext(context.Background(), user, duration)
}

// GenerateTokenContext creates a signed JWT using the provider's active key.
func (m *Manager) GenerateTokenContext(ctx context.Context, user UserInfo, duration time.Duration) (string, error) {
	if m == nil {
		return "", ErrInvalidManager
	}
	if strings.TrimSpace(user.ID) == "" {
		return "", ErrInvalidIdentity
	}
	if err := validateLifetime(duration, m.maxLifetime); err != nil {
		return "", err
	}
	if err := validatePayloadSize(user, m.maxPayloadSize); err != nil {
		return "", err
	}
	tokenID, err := newTokenID()
	if err != nil {
		return "", err
	}
	now := m.clock()
	key, err := currentKey(ctx, m.signingKeys, false)
	if err != nil {
		return "", err
	}
	claims := CustomClaims{
		UserInfo: user,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(duration)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    m.issuer,
			Audience:  jwt.ClaimStrings(m.audience),
			Subject:   user.ID,
			ID:        tokenID,
		},
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

// ValidateToken parses and validates a signed JWT string.
func (m *Manager) ValidateToken(tokenString string) (*CustomClaims, error) {
	return m.ValidateTokenContext(context.Background(), tokenString)
}

// ValidateTokenContext validates a token using the key selected by its kid.
func (m *Manager) ValidateTokenContext(ctx context.Context, tokenString string) (*CustomClaims, error) {
	if m == nil {
		return nil, ErrInvalidManager
	}
	if len(tokenString) == 0 {
		return nil, ErrInvalidToken
	}
	if len(tokenString) > m.maxTokenSize {
		return nil, ErrTokenTooLarge
	}
	token, err := m.parser.ParseWithClaims(tokenString, &CustomClaims{}, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		keyID, _ := t.Header["kid"].(string)
		return resolveKey(ctx, m.signingKeys, keyID, false)
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}
	if err := validateUserClaims(claims.UserInfo, claims.RegisteredClaims, m.maxLifetime, m.allowLegacyClaims); err != nil {
		return nil, err
	}
	if err := checkRevocation(ctx, m.revocation, claims.ID); err != nil {
		return nil, err
	}

	return claims, nil
}

// ExtractUserInfo is a convenient wrapper around ValidateToken to return just the UserInfo.
//
// Deprecated: use ValidateTokenContext to retain registered claims needed for
// authorization, auditing, and revocation.
func (m *Manager) ExtractUserInfo(tokenString string) (*UserInfo, error) {
	return m.ExtractUserInfoContext(context.Background(), tokenString)
}

// ExtractUserInfoContext validates a token, including optional revocation,
// using the caller's context.
func (m *Manager) ExtractUserInfoContext(ctx context.Context, tokenString string) (*UserInfo, error) {
	claims, err := m.ValidateTokenContext(ctx, tokenString)
	if err != nil {
		return nil, err
	}
	return &claims.UserInfo, nil
}
