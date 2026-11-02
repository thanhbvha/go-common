package crypt

import (
	"errors"
	"testing"
)

func TestGenerateRandomBytes(t *testing.T) {
	b, err := GenerateRandomBytes(16)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(b) != 16 {
		t.Errorf("expected length 16, got %d", len(b))
	}
}

func TestGenerateKey32(t *testing.T) {
	key, err := GenerateKey32()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(key) != 32 {
		t.Errorf("expected length 32, got %d", len(key))
	}
}

func TestGenerateRandomString(t *testing.T) {
	s, err := GenerateRandomString(16)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 16 bytes = 32 hex characters
	if len(s) != 32 {
		t.Errorf("expected length 32, got %d", len(s))
	}
}

func TestGenerateRandomRejectsNegativeLength(t *testing.T) {
	if _, err := GenerateRandomBytes(-1); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("GenerateRandomBytes(-1) error = %v", err)
	}
	if _, err := GenerateRandomString(-1); !errors.Is(err, ErrInvalidLength) {
		t.Fatalf("GenerateRandomString(-1) error = %v", err)
	}
}
