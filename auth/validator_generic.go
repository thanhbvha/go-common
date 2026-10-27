package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// ContextTokenValidator validates a bearer token and returns the authenticated
// value that middleware should place in the request context.
type ContextTokenValidator[T any] interface {
	ValidateRequestToken(ctx context.Context, token string) (T, error)
}

// ClaimsFactory returns a fresh claims value for each request. Returning the
// same pointer across requests is unsafe because JWT parsing mutates it.
type ClaimsFactory[T jwt.Claims] func() T

// TokenIDExtractor returns the JWT ID used for Redis revocation checks. It may
// be nil when the application does not use access-token revocation.
type TokenIDExtractor[T jwt.Claims] func(T) (string, error)

type genericTokenValidator[T jwt.Claims] struct {
	manager       GenericManager[T]
	claimsFactory ClaimsFactory[T]
	tokenID       TokenIDExtractor[T]
}

// NewGenericTokenValidator adapts GenericManager for framework middleware.
// When tokenID is non-nil, validation fails closed on Redis errors and rejects
// revoked tokens. A policy-driven GenericManager can perform this check itself,
// in which case tokenID should be nil. The factory must allocate fresh claims
// for every call.
func NewGenericTokenValidator[T jwt.Claims](manager GenericManager[T], claimsFactory ClaimsFactory[T], tokenID TokenIDExtractor[T]) (ContextTokenValidator[T], error) {
	if isNilValue(manager) {
		return nil, errors.New("auth: generic manager is required")
	}
	if claimsFactory == nil {
		return nil, errors.New("auth: claims factory is required")
	}
	return &genericTokenValidator[T]{manager: manager, claimsFactory: claimsFactory, tokenID: tokenID}, nil
}

func (v *genericTokenValidator[T]) ValidateRequestToken(ctx context.Context, token string) (T, error) {
	var zero T
	claims := v.claimsFactory()
	if isNilValue(claims) {
		return zero, fmt.Errorf("%w: claims factory returned nil", ErrInvalidToken)
	}
	revocationChecked := false
	if contextual, ok := v.manager.(interface {
		verifyRequestToken(context.Context, string, T) (bool, error)
	}); ok {
		var err error
		revocationChecked, err = contextual.verifyRequestToken(ctx, token, claims)
		if err != nil {
			return zero, err
		}
	} else if err := v.manager.VerifyToken(token, claims); err != nil {
		return zero, err
	}
	if v.tokenID == nil || revocationChecked {
		return claims, nil
	}

	tokenID, err := v.tokenID(claims)
	if err != nil {
		return zero, fmt.Errorf("%w: invalid token ID: %w", ErrInvalidToken, err)
	}
	if tokenID == "" {
		return zero, fmt.Errorf("%w: token ID is required", ErrInvalidToken)
	}
	revoked, err := v.manager.IsRevoked(ctx, tokenID)
	if err != nil {
		return zero, fmt.Errorf("auth: revocation check failed: %w", err)
	}
	if revoked {
		return zero, ErrTokenRevoked
	}
	return claims, nil
}
