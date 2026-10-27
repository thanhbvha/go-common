package main

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/auth"
)

func TestEncryptedAADBindsSession(t *testing.T) {
	first, err := encryptedAAD("session-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := encryptedAAD("session-2")
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) {
		t.Fatal("different sessions produced identical AAD")
	}
}

func TestIssueGenericAccessToken(t *testing.T) {
	manager, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*exampleClaims]{
		SecretKey:   "jwt-secret-key-that-is-at-least-32-bytes",
		Issuer:      exampleIssuer,
		Audience:    []string{exampleAudience},
		MaxLifetime: accessTokenLifetime,
	})
	if err != nil {
		t.Fatal(err)
	}
	token, issued, err := issueGenericAccessToken(context.Background(), manager, "user-1", "admin")
	if err != nil {
		t.Fatal(err)
	}
	parsed := &exampleClaims{}
	if err := manager.VerifyTokenContext(context.Background(), token, parsed); err != nil {
		t.Fatal(err)
	}
	if issued.ID == "" || parsed.ID != issued.ID || parsed.UserID != "user-1" || parsed.Role != "admin" {
		t.Fatalf("issued = %#v, parsed = %#v", issued, parsed)
	}
	if time.Until(parsed.ExpiresAt.Time) <= 0 {
		t.Fatal("issued access token is already expired")
	}
}

func TestRotationDemo(t *testing.T) {
	t.Setenv("AUTH_SIGNING_KEY_V1", "signing-key-v1-that-is-at-least-32-bytes")
	t.Setenv("AUTH_SIGNING_KEY_V2", "signing-key-v2-that-is-at-least-32-bytes")
	t.Setenv("AUTH_ENCRYPTION_KEY_V1", "01234567890123456789012345678901")
	t.Setenv("AUTH_ENCRYPTION_KEY_V2", "12345678901234567890123456789012")
	if err := runRotationDemo(); err != nil {
		t.Fatal(err)
	}
}
