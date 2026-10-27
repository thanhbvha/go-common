package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func BenchmarkManagerValidateToken(b *testing.B) {
	manager, err := NewManagerWithOptions(ManagerOptions{SecretKey: "jwt-secret-key-that-is-at-least-32-bytes"})
	if err != nil {
		b.Fatal(err)
	}
	token, err := manager.GenerateToken(UserInfo{ID: "user-1", Role: "admin"}, time.Hour)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := manager.ValidateToken(token); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenericManagerVerifyToken(b *testing.B) {
	manager, err := NewGenericManagerWithOptions(GenericManagerOptions[*benchmarkClaims]{
		SecretKey:   "jwt-secret-key-that-is-at-least-32-bytes",
		MaxLifetime: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Now()
	token, err := manager.GenerateToken(&benchmarkClaims{
		UserID: "user-1",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Subject:   "user-1",
			ID:        "token-1",
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := manager.VerifyToken(token, &benchmarkClaims{}); err != nil {
			b.Fatal(err)
		}
	}
}

type benchmarkClaims struct {
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

func BenchmarkEncryptedManagerValidateToken(b *testing.B) {
	manager, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{
		SecretKey: "jwt-secret-key-that-is-at-least-32-bytes",
		AESKey:    "01234567890123456789012345678901",
	})
	if err != nil {
		b.Fatal(err)
	}
	aad := []byte("session-1")
	token, err := manager.GenerateToken(UserInfo{ID: "user-1", Role: "admin"}, time.Hour, aad)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := manager.ValidateToken(token, aad); err != nil {
			b.Fatal(err)
		}
	}
}
