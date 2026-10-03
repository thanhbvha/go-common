package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/auth"
)

type TestEncryptedPayload struct {
	AccountID string `json:"account_id"`
	SecretPin string `json:"secret_pin"`
}

func setupGenericEncryptedManager(t *testing.T) (auth.GenericEncryptedManager[TestEncryptedPayload], *miniredis.Miniredis) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	mgr, err := auth.NewGenericEncryptedManager[TestEncryptedPayload](auth.GenericEncryptedOptions{
		SecretKey: "super-secret-jwt-key-must-be-long",
		AESKey:    "01234567890123456789012345678901",
		Redis:     client,
	})
	if err != nil {
		t.Fatalf("failed to create generic encrypted manager: %v", err)
	}

	return mgr, s
}

func TestGenericEncryptedManager_GenerateAndVerifyToken(t *testing.T) {
	mgr, mr := setupGenericEncryptedManager(t)
	defer mr.Close()

	payload := TestEncryptedPayload{
		AccountID: "acc_999",
		SecretPin: "123456",
	}

	aad := []byte("device_iphone_12")

	token, err := mgr.GenerateToken(payload, 1*time.Hour, aad)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if token == "" {
		t.Fatal("expected a valid token string")
	}

	// Verify success
	var decoded TestEncryptedPayload
	err = mgr.VerifyToken(token, &decoded, aad)
	if err != nil {
		t.Fatalf("expected no error during verification, got %v", err)
	}

	if decoded.AccountID != "acc_999" {
		t.Errorf("expected AccountID to be 'acc_999', got '%s'", decoded.AccountID)
	}
	if decoded.SecretPin != "123456" {
		t.Errorf("expected SecretPin to be '123456', got '%s'", decoded.SecretPin)
	}

	// Verify failure with wrong AAD
	var decodedWrong TestEncryptedPayload
	err = mgr.VerifyToken(token, &decodedWrong, []byte("wrong_device"))
	if err == nil {
		t.Fatal("expected error when verifying with wrong AAD, got nil")
	}
}

func TestGenericEncryptedManager_RefreshToken(t *testing.T) {
	mgr, mr := setupGenericEncryptedManager(t)
	defer mr.Close()

	ctx := context.Background()
	userID := "user_456"

	rt, err := mgr.GenerateRefreshToken(ctx, userID, 24*time.Hour)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
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

func TestGenericEncryptedManager_TokenRevocation(t *testing.T) {
	mgr, mr := setupGenericEncryptedManager(t)
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
