package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestManagerOptionsClaimsPolicyAndClock(t *testing.T) {
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	manager, err := NewManagerWithOptions(ManagerOptions{
		SecretKey:   "jwt-secret-key-that-is-at-least-32-bytes",
		Issuer:      "identity-service",
		Audience:    []string{"api"},
		MaxLifetime: time.Hour,
		Clock:       func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := manager.GenerateToken(UserInfo{ID: "user-1"}, 30*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Issuer != "identity-service" || len(claims.Audience) != 1 || claims.Audience[0] != "api" || !claims.IssuedAt.Time.Equal(now) || claims.Subject != "user-1" || claims.ID == "" {
		t.Fatalf("claims policy not applied: %#v", claims.RegisteredClaims)
	}
	if _, err := manager.GenerateToken(UserInfo{ID: "user-1"}, 2*time.Hour); !errors.Is(err, ErrInvalidDuration) {
		t.Fatalf("max lifetime error = %v", err)
	}
}

func TestManagerOptionsRejectWrongIssuer(t *testing.T) {
	const secret = "jwt-secret-key-that-is-at-least-32-bytes"
	issuer, err := NewManagerWithOptions(ManagerOptions{SecretKey: secret, Issuer: "issuer-a"})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewManagerWithOptions(ManagerOptions{SecretKey: secret, Issuer: "issuer-b"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := issuer.GenerateToken(UserInfo{ID: "user-1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.ValidateToken(token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("wrong issuer error = %v", err)
	}
}

func TestManagerRejectsIncomingLifetimeAbovePolicy(t *testing.T) {
	const secret = "jwt-secret-key-that-is-at-least-32-bytes"
	issuer, err := NewManagerWithOptions(ManagerOptions{SecretKey: secret})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewManagerWithOptions(ManagerOptions{SecretKey: secret, MaxLifetime: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	token, err := issuer.GenerateToken(UserInfo{ID: "user-1"}, 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.ValidateToken(token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("oversized incoming lifetime error = %v", err)
	}
}

func TestNilManagersReturnStableError(t *testing.T) {
	var manager *Manager
	if _, err := manager.GenerateToken(UserInfo{ID: "user-1"}, time.Hour); !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("nil manager generate error = %v", err)
	}
	if _, err := manager.ValidateToken("token"); !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("nil manager validate error = %v", err)
	}

	var encrypted *EncryptedManager
	if _, err := encrypted.GenerateToken(UserInfo{ID: "user-1"}, time.Hour, nil); !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("nil encrypted manager generate error = %v", err)
	}
	if _, err := encrypted.ValidateToken("token", nil); !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("nil encrypted manager validate error = %v", err)
	}
}

func TestRejectsTokensMissingIdentityOrKeyID(t *testing.T) {
	const secret = "jwt-secret-key-that-is-at-least-32-bytes"
	now := time.Now()
	legacyClaims := CustomClaims{
		UserInfo: UserInfo{ID: "user-1"},
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, legacyClaims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	strictManager, err := NewManagerWithOptions(ManagerOptions{SecretKey: secret})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := strictManager.ValidateToken(token); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("strict manager legacy token error = %v", err)
	}
}

func TestAllRequiredJWTFields(t *testing.T) {
	secret := "jwt-secret-key-that-is-at-least-32-bytes"
	manager, err := NewManagerWithOptions(ManagerOptions{SecretKey: secret})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for _, missing := range []string{"kid", "exp", "iat", "nbf", "sub", "jti"} {
		t.Run(missing, func(t *testing.T) {
			claims := CustomClaims{UserInfo: UserInfo{ID: "user"}, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), Subject: "user", ID: "token-id"}}
			switch missing {
			case "exp":
				claims.ExpiresAt = nil
			case "iat":
				claims.IssuedAt = nil
			case "nbf":
				claims.NotBefore = nil
			case "sub":
				claims.Subject = ""
			case "jti":
				claims.ID = ""
			}
			token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
			if missing != "kid" {
				token.Header["kid"] = "default"
			}
			encoded, err := token.SignedString([]byte(secret))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := manager.ValidateToken(encoded); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("missing %s accepted: %v", missing, err)
			}
		})
	}
}
