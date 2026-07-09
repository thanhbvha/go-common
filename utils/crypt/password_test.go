package crypt

import (
	"testing"
)

func TestArgon2id(t *testing.T) {
	password := "superSecretPassword123"

	// Hash password
	hash, err := HashPasswordArgon2id(password)
	if err != nil {
		t.Fatalf("unexpected error hashing password: %v", err)
	}

	// Verify correct password
	match, err := VerifyPasswordArgon2id(password, hash)
	if err != nil {
		t.Fatalf("unexpected error verifying password: %v", err)
	}
	if !match {
		t.Error("password should have matched")
	}

	// Verify incorrect password
	match, err = VerifyPasswordArgon2id("wrongPassword", hash)
	if err != nil {
		t.Fatalf("unexpected error verifying wrong password: %v", err)
	}
	if match {
		t.Error("wrong password should not have matched")
	}

	// Invalid hash format
	_, err = VerifyPasswordArgon2id(password, "invalid-hash-format")
	if err == nil {
		t.Error("expected error for invalid hash format")
	}
}
