package crypt

import (
	"errors"
	"strings"
	"testing"
)

func TestPasswordRoundTrip(t *testing.T) {
	password := "superSecretPassword123"
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword() error = %v", err)
	}
	match, err := VerifyPassword(password, hash)
	if err != nil || !match {
		t.Fatalf("VerifyPassword() = %v, %v", match, err)
	}
	match, err = VerifyPassword("wrongPassword", hash)
	if err != nil || match {
		t.Fatalf("VerifyPassword(wrong) = %v, %v", match, err)
	}
	needsUpgrade, err := PasswordHashNeedsUpgrade(hash)
	if err != nil || needsUpgrade {
		t.Fatalf("PasswordHashNeedsUpgrade() = %v, %v", needsUpgrade, err)
	}
}

func TestVerifyPasswordRejectsUnsafeHashes(t *testing.T) {
	validHash := strings.Repeat("A", 43)
	validSalt := strings.Repeat("A", 22)
	tests := []string{
		"invalid-hash-format",
		"$argon2i$v=19$m=19456,t=2,p=1$" + validSalt + "$" + validHash,
		"$argon2id$v=19$m=19456,t=0,p=1$" + validSalt + "$" + validHash,
		"$argon2id$v=19$m=19456,t=2,p=0$" + validSalt + "$" + validHash,
		"$argon2id$v=19$m=999999,t=2,p=1$" + validSalt + "$" + validHash,
		strings.Repeat("x", maxEncodedHashLength+1),
	}
	for _, encoded := range tests {
		if _, err := VerifyPassword("password", encoded); !errors.Is(err, ErrInvalidPasswordHash) {
			t.Errorf("VerifyPassword(%q) error = %v", encoded, err)
		}
	}
}

func FuzzParsePasswordHash(f *testing.F) {
	f.Add("invalid")
	f.Add("$argon2id$v=19$m=19456,t=2,p=1$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	f.Fuzz(func(t *testing.T, encoded string) {
		_, _ = parsePasswordHash(encoded)
	})
}
