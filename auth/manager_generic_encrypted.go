package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
)

// GenericEncryptedManagerOptions applies the same policy and key providers as
// EncryptedManager. Subject binds the encrypted payload to signed identity.
type GenericEncryptedManagerOptions[T any] struct {
	EncryptedManagerOptions
	Subject func(T) (string, error)
}

type GenericEncryptedManager[T any] struct {
	engine  *EncryptedManager
	subject func(T) (string, error)
}

type EncryptedPayload[T any] struct {
	Payload T
	jwt.RegisteredClaims
	EncryptionKeyID string
	FormatVersion   int
}

func NewGenericEncryptedManagerWithOptions[T any](opts GenericEncryptedManagerOptions[T]) (*GenericEncryptedManager[T], error) {
	if opts.Subject == nil {
		return nil, fmt.Errorf("auth: subject extractor is required")
	}
	engine, err := NewEncryptedManagerWithOptions(opts.EncryptedManagerOptions)
	if err != nil {
		return nil, err
	}
	return &GenericEncryptedManager[T]{engine: engine, subject: opts.Subject}, nil
}

func (m *GenericEncryptedManager[T]) GenerateTokenContext(ctx context.Context, payload T, duration time.Duration, aad []byte) (string, error) {
	if m == nil {
		return "", ErrInvalidManager
	}
	if isNilValue(payload) {
		return "", ErrInvalidIdentity
	}
	subject, err := m.subject(payload)
	if err != nil {
		return "", fmt.Errorf("%w: subject unavailable", ErrInvalidIdentity)
	}
	return m.engine.generatePayload(ctx, subject, payload, duration, aad)
}

func (m *GenericEncryptedManager[T]) ValidateTokenContext(ctx context.Context, token string, aad []byte) (*EncryptedPayload[T], error) {
	if m == nil {
		return nil, ErrInvalidManager
	}
	plaintext, claims, err := m.engine.validatePayload(ctx, token, aad)
	if err != nil {
		return nil, err
	}
	var payload T
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON structure", ErrDecryptionFailed)
	}
	if isNilValue(payload) {
		return nil, ErrInvalidIdentity
	}
	subject, err := m.subject(payload)
	if err != nil {
		return nil, ErrInvalidIdentity
	}
	if err := validateUserClaims(UserInfo{ID: subject}, claims.RegisteredClaims, m.engine.maxLifetime); err != nil {
		return nil, err
	}
	if err := checkRevocation(ctx, m.engine.revocation, claims.ID); err != nil {
		return nil, err
	}
	return &EncryptedPayload[T]{Payload: payload, RegisteredClaims: claims.RegisteredClaims, EncryptionKeyID: claims.EncryptionKeyID, FormatVersion: claims.Version}, nil
}
