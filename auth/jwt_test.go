package auth

import (
	"testing"
	"time"
)

func TestJWTManager(t *testing.T) {
	manager := NewManager("my-super-secret-key")

	user := UserInfo{
		ID:    "u_123",
		Role:  "admin",
		Email: "admin@example.com",
		Metadata: map[string]interface{}{
			"department": "IT",
		},
	}

	// 1. Generate Token
	token, err := manager.GenerateToken(user, 1*time.Hour)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	// 2. Validate Token
	claims, err := manager.ValidateToken(token)
	if err != nil {
		t.Fatalf("Failed to validate token: %v", err)
	}

	if claims.UserInfo.ID != user.ID {
		t.Errorf("Expected ID %s, got %s", user.ID, claims.UserInfo.ID)
	}
	if claims.UserInfo.Role != user.Role {
		t.Errorf("Expected Role %s, got %s", user.Role, claims.UserInfo.Role)
	}

	// 3. Extract UserInfo Directly
	extractedUser, err := manager.ExtractUserInfo(token)
	if err != nil {
		t.Fatalf("Failed to extract UserInfo: %v", err)
	}
	if extractedUser.Email != user.Email {
		t.Errorf("Expected Email %s, got %s", user.Email, extractedUser.Email)
	}

	// 4. Test Expired Token
	expiredToken, _ := manager.GenerateToken(user, -1*time.Hour)
	_, err = manager.ValidateToken(expiredToken)
	if err != ErrExpiredToken {
		t.Errorf("Expected ErrExpiredToken, got %v", err)
	}

	// 5. Test Invalid Key
	badManager := NewManager("wrong-key")
	_, err = badManager.ValidateToken(token)
	if err == nil {
		t.Error("Expected error when validating with wrong key")
	}
}
