package auth

import (
	"strings"
	"testing"
	"time"
)

func TestEncryptedManager(t *testing.T) {
	jwtSecret := "my-jwt-secret-key"
	// AES-256 key must be exactly 32 bytes
	aesKey := "12345678901234567890123456789012" 

	manager, err := NewEncryptedManager(jwtSecret, aesKey)
	if err != nil {
		t.Fatalf("Failed to create EncryptedManager: %v", err)
	}

	user := UserInfo{
		ID:    "u_999",
		Role:  "superuser",
		Email: "super@example.com",
	}

	// 1. Generate Token
	token, err := manager.GenerateToken(user, 1*time.Hour)
	if err != nil {
		t.Fatalf("Failed to generate encrypted token: %v", err)
	}

	// Check that the token does NOT contain the raw UserInfo in plaintext.
	// We'll just do a very basic check that 'superuser' string is not in the base64 part.
	if strings.Contains(token, "superuser") {
		t.Error("Token appears to contain unencrypted payload string")
	}

	// 2. Validate Token
	extractedUser, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate encrypted token: %v", err)
	}

	if extractedUser.ID != user.ID {
		t.Errorf("Expected ID %s, got %s", user.ID, extractedUser.ID)
	}
	if extractedUser.Role != user.Role {
		t.Errorf("Expected Role %s, got %s", user.Role, extractedUser.Role)
	}

	// 3. Test Invalid AES Key Size Initialization
	_, err = NewEncryptedManager(jwtSecret, "short-key")
	if err == nil {
		t.Error("Expected error when initializing with invalid AES key size")
	}
}

func TestEncryptedManager_WithAAD(t *testing.T) {
	jwtSecret := "my-jwt-secret-key"
	aesKey := "12345678901234567890123456789012"
	aad := []byte("tenant-id-123")

	manager, err := NewEncryptedManager(jwtSecret, aesKey, WithAAD(aad))
	if err != nil {
		t.Fatalf("Failed to create EncryptedManager with AAD: %v", err)
	}

	user := UserInfo{
		ID: "u_aad",
	}

	token, err := manager.GenerateToken(user, 1*time.Hour)
	if err != nil {
		t.Fatalf("Failed to generate token with AAD: %v", err)
	}

	extractedUser, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate token with AAD: %v", err)
	}

	if extractedUser.ID != user.ID {
		t.Errorf("Expected ID %s, got %s", user.ID, extractedUser.ID)
	}

	// Try to validate with a manager that has a different AAD or no AAD
	managerNoAAD, _ := NewEncryptedManager(jwtSecret, aesKey)
	_, err = managerNoAAD.ValidateToken(token)
	if err == nil {
		t.Error("Expected error when validating AAD-encrypted token with no AAD")
	}

	managerWrongAAD, _ := NewEncryptedManager(jwtSecret, aesKey, WithAAD([]byte("wrong-tenant")))
	_, err = managerWrongAAD.ValidateToken(token)
	if err == nil {
		t.Error("Expected error when validating AAD-encrypted token with wrong AAD")
	}
}
