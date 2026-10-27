package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestManagerSizeLimits(t *testing.T) {
	manager, err := NewManagerWithOptions(ManagerOptions{
		SecretKey:      "jwt-secret-key-that-is-at-least-32-bytes",
		MaxPayloadSize: 128,
		MaxTokenSize:   256,
	})
	if err != nil {
		t.Fatal(err)
	}
	user := UserInfo{ID: "user-1", Metadata: map[string]interface{}{"data": strings.Repeat("x", 256)}}
	if _, err := manager.GenerateToken(user, time.Hour); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized payload error = %v", err)
	}
	if _, err := manager.ValidateToken(strings.Repeat("x", 257)); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("oversized token error = %v", err)
	}
}

func TestEncryptedManagerSizeLimits(t *testing.T) {
	manager, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{
		SecretKey:      "jwt-secret-key-that-is-at-least-32-bytes",
		AESKey:         "01234567890123456789012345678901",
		MaxPayloadSize: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	user := UserInfo{ID: "user-1", Metadata: map[string]interface{}{"data": strings.Repeat("x", 256)}}
	if _, err := manager.GenerateToken(user, time.Hour, nil); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("oversized encrypted payload error = %v", err)
	}
}
