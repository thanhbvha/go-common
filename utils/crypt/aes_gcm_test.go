package crypt

import (
	"bytes"
	"testing"
)

func TestAESGCM(t *testing.T) {
	key, err := GenerateKey32()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	plaintext := []byte("Hello, AES GCM with AAD!")
	aad := []byte("metadata-id-123")

	// Encrypt
	ciphertext, err := EncryptAESGCM(key, plaintext, aad)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Decrypt
	decrypted, err := DecryptAESGCM(key, ciphertext, aad)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("decrypted data doesn't match original. expected %s, got %s", string(plaintext), string(decrypted))
	}

	// Decrypt with wrong AAD should fail
	_, err = DecryptAESGCM(key, ciphertext, []byte("wrong-aad"))
	if err == nil {
		t.Error("decryption should have failed with incorrect AAD")
	}

	// Decrypt with tampered ciphertext should fail
	tamperedCiphertext := make([]byte, len(ciphertext))
	copy(tamperedCiphertext, ciphertext)
	tamperedCiphertext[len(tamperedCiphertext)-1] ^= 0xFF

	_, err = DecryptAESGCM(key, tamperedCiphertext, aad)
	if err == nil {
		t.Error("decryption should have failed with tampered ciphertext")
	}
}

func TestAESGCM_InvalidKeySize(t *testing.T) {
	key := []byte("too-short")
	_, err := EncryptAESGCM(key, []byte("data"), nil)
	if err != ErrInvalidKeySize {
		t.Errorf("expected ErrInvalidKeySize, got %v", err)
	}
}
