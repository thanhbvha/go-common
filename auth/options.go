package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
)

const (
	DefaultMaxTokenSize   = 16 * 1024
	DefaultMaxPayloadSize = 8 * 1024
)

// Clock returns the current time. Supplying one makes token lifetime behavior
// deterministic in tests and centralizes clock-skew handling.
type Clock func() time.Time

// RevocationChecker checks whether a token ID has been revoked. Redis-backed
// GenericManager implementations satisfy this interface directly.
type RevocationChecker interface {
	IsRevoked(ctx context.Context, tokenID string) (bool, error)
}

// ManagerOptions configures standard JWT issuance and validation.
type ManagerOptions struct {
	SecretKey         string
	SigningKeys       KeyProvider
	Revocation        RevocationChecker
	Issuer            string
	Audience          []string
	Leeway            time.Duration
	MaxLifetime       time.Duration
	Clock             Clock
	MaxTokenSize      int
	MaxPayloadSize    int
	AllowLegacyClaims bool
}

// EncryptedManagerOptions configures encrypted JWT issuance and validation.
type EncryptedManagerOptions struct {
	SecretKey         string
	AESKey            string
	SigningKeys       KeyProvider
	EncryptionKeys    KeyProvider
	Revocation        RevocationChecker
	AADPolicy         AADPolicy
	Issuer            string
	Audience          []string
	Leeway            time.Duration
	MaxLifetime       time.Duration
	Clock             Clock
	MaxTokenSize      int
	MaxPayloadSize    int
	AllowLegacyClaims bool
}

func normalizeTokenOptions(issuer string, audience []string, leeway, maxLifetime time.Duration, clock Clock) (string, []string, time.Duration, time.Duration, Clock, error) {
	if leeway < 0 || maxLifetime < 0 {
		return "", nil, 0, 0, nil, ErrInvalidDuration
	}
	if clock == nil {
		clock = time.Now
	}
	return issuer, append([]string(nil), audience...), leeway, maxLifetime, clock, nil
}

func parserOptions(issuer string, audience []string, leeway time.Duration, clock Clock) []jwt.ParserOption {
	options := []jwt.ParserOption{
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithTimeFunc(clock),
		jwt.WithExpirationRequired(),
		jwt.WithIssuedAt(),
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

func validateLifetime(duration, maxLifetime time.Duration) error {
	if duration <= 0 {
		return ErrInvalidDuration
	}
	if maxLifetime > 0 && duration > maxLifetime {
		return errors.Join(ErrInvalidDuration, errors.New("auth: token lifetime exceeds configured maximum"))
	}
	return nil
}

func newTokenID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("auth: generate token ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func validateUserClaims(user UserInfo, claims jwt.RegisteredClaims, maxLifetime time.Duration, allowLegacy bool) error {
	if strings.TrimSpace(user.ID) == "" || claims.ExpiresAt == nil || claims.IssuedAt == nil || claims.NotBefore == nil {
		return ErrInvalidToken
	}
	legacyClaims := claims.Subject == "" && claims.ID == ""
	if (!allowLegacy || !legacyClaims) && (claims.Subject == "" || claims.Subject != user.ID || claims.ID == "") {
		return ErrInvalidToken
	}
	if claims.ExpiresAt.Before(claims.IssuedAt.Time) || claims.ExpiresAt.Before(claims.NotBefore.Time) {
		return ErrInvalidToken
	}
	if maxLifetime > 0 && claims.ExpiresAt.Sub(claims.IssuedAt.Time) > maxLifetime {
		return ErrInvalidToken
	}
	return nil
}

func checkRevocation(ctx context.Context, checker RevocationChecker, tokenID string) error {
	if checker == nil {
		return nil
	}
	revoked, err := checker.IsRevoked(ctx, tokenID)
	if err != nil {
		return fmt.Errorf("auth: revocation check failed: %w", err)
	}
	if revoked {
		return ErrTokenRevoked
	}
	return nil
}

func normalizeSizeLimit(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func validatePayloadSize(value any, maxSize int) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("auth: marshal payload: %w", err)
	}
	if len(payload) > maxSize {
		return ErrPayloadTooLarge
	}
	return nil
}
