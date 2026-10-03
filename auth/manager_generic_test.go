package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/auth"
)

type TestClaims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

func setupGenericManager(t *testing.T) (auth.GenericManager[*TestClaims], *miniredis.Miniredis) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	mgr, err := auth.NewGenericManager[*TestClaims](auth.GenericOptions{
		SecretKey: "super-secret-jwt-key-must-be-long",
		Redis:     client,
	})
	if err != nil {
		t.Fatalf("failed to create generic manager: %v", err)
	}

	return mgr, s
}

func TestGenericManager_GenerateAndVerifyToken(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	claims := &TestClaims{
		UserID: "user_123",
		Role:   "admin",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
		},
	}

	token, err := mgr.GenerateToken(claims)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if token == "" {
		t.Fatal("expected a valid token string")
	}

	// Verify
	var parsedClaims TestClaims
	err = mgr.VerifyToken(token, &parsedClaims)
	if err != nil {
		t.Fatalf("expected no error during verification, got %v", err)
	}

	if parsedClaims.UserID != "user_123" {
		t.Errorf("expected UserID to be 'user_123', got '%s'", parsedClaims.UserID)
	}
	if parsedClaims.Role != "admin" {
		t.Errorf("expected Role to be 'admin', got '%s'", parsedClaims.Role)
	}
}

func TestGenericManager_RefreshToken(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	ctx := context.Background()
	userID := "user_456"

	rt, err := mgr.GenerateRefreshToken(ctx, userID, 24*time.Hour)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(rt) != 64 {
		t.Errorf("expected refresh token to be 64 hex characters, got length %d", len(rt))
	}

	isValid, err := mgr.VerifyRefreshToken(ctx, userID, rt)
	if err != nil {
		t.Fatalf("expected no error verifying refresh token, got %v", err)
	}
	if !isValid {
		t.Fatal("expected refresh token to be valid")
	}

	err = mgr.RevokeRefreshToken(ctx, userID)
	if err != nil {
		t.Fatalf("expected no error revoking refresh token, got %v", err)
	}

	isValid, _ = mgr.VerifyRefreshToken(ctx, userID, rt)
	if isValid {
		t.Fatal("expected refresh token to be invalid after revocation")
	}
}

func TestGenericManager_TokenRevocation(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	ctx := context.Background()
	jti := "unique-jti-789"

	isRevoked, err := mgr.IsRevoked(ctx, jti)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if isRevoked {
		t.Fatal("expected token to not be revoked yet")
	}

	err = mgr.RevokeToken(ctx, jti, 15*time.Minute)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	isRevoked, err = mgr.IsRevoked(ctx, jti)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !isRevoked {
		t.Fatal("expected token to be revoked")
	}
}
