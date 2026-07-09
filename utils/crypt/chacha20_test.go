package crypt

import (
	"bytes"
	"testing"
)

func TestChaCha20(t *testing.T) {
	key, err := GenerateKey32()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	plaintext := []byte("Hello, ChaCha20-Poly1305!")
	aad := []byte("metadata-id-456")

	// Encrypt
	ciphertext, err := EncryptChaCha20(key, plaintext, aad)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Decrypt
	decrypted, err := DecryptChaCha20(key, ciphertext, aad)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("decrypted data doesn't match original. expected %s, got %s", string(plaintext), string(decrypted))
	}

	// Decrypt with wrong AAD should fail
	_, err = DecryptChaCha20(key, ciphertext, []byte("wrong-aad"))
	if err == nil {
		t.Error("decryption should have failed with incorrect AAD")
	}

	// Decrypt with tampered ciphertext should fail
	tamperedCiphertext := make([]byte, len(ciphertext))
	copy(tamperedCiphertext, ciphertext)
	tamperedCiphertext[len(tamperedCiphertext)-1] ^= 0xFF

	_, err = DecryptChaCha20(key, tamperedCiphertext, aad)
	if err == nil {
		t.Error("decryption should have failed with tampered ciphertext")
	}
}
