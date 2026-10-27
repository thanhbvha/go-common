package auth

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestBoundEncryptedManagerRequiresAAD(t *testing.T) {
	manager, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{
		SecretKey: "jwt-secret-key-that-is-at-least-32-bytes",
		AESKey:    "01234567890123456789012345678901",
		AADPolicy: RequireAAD,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GenerateToken(UserInfo{ID: "user-1"}, time.Hour, nil); !errors.Is(err, ErrAADRequired) {
		t.Fatalf("missing AAD error = %v", err)
	}
	aad, err := (AADContext{Issuer: "identity", Audience: "api", SessionID: "session-1"}).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	token, err := manager.GenerateToken(UserInfo{ID: "user-1"}, time.Hour, aad)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ValidateToken(token, aad); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ValidateToken(token, nil); !errors.Is(err, ErrAADRequired) {
		t.Fatalf("validation without AAD error = %v", err)
	}
}

func TestAADContextIsDeterministic(t *testing.T) {
	value := AADContext{Issuer: "identity", Audience: "api", SessionID: "session-1", DeviceID: "device-1"}
	first, err := value.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	second, err := value.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("AAD encoding is not deterministic: %q != %q", first, second)
	}
}
