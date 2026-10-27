package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/thanhbvha/go-common/auth"
)

func TestGenericTokenValidatorChecksRevocation(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	claims := &TestClaims{
		UserID: "user-123",
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        "access-token-1",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token, err := mgr.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := auth.NewGenericTokenValidator(
		mgr,
		func() *TestClaims { return &TestClaims{} },
		func(claims *TestClaims) (string, error) { return claims.ID, nil },
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := validator.ValidateRequestToken(context.Background(), token)
	if err != nil || got.UserID != claims.UserID {
		t.Fatalf("initial validation = %#v, %v", got, err)
	}
	if err := mgr.RevokeToken(context.Background(), claims.ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := validator.ValidateRequestToken(context.Background(), token); !errors.Is(err, auth.ErrTokenRevoked) {
		t.Fatalf("revoked token error = %v", err)
	}
}

func TestGenericTokenValidatorFailsClosedAndRejectsNilFactoryValue(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	claims := &TestClaims{
		UserID: "user-123",
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        "access-token-2",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token, err := mgr.GenerateToken(claims)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := auth.NewGenericTokenValidator(
		mgr,
		func() *TestClaims { return &TestClaims{} },
		func(claims *TestClaims) (string, error) { return claims.ID, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	mr.Close()
	if _, err := validator.ValidateRequestToken(context.Background(), token); err == nil {
		t.Fatal("expected Redis revocation lookup failure")
	}

	nilFactoryValidator, err := auth.NewGenericTokenValidator[*TestClaims](mgr, func() *TestClaims { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nilFactoryValidator.ValidateRequestToken(context.Background(), token); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("nil factory value error = %v", err)
	}
}
