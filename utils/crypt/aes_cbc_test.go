package crypt

import (
	"bytes"
	"testing"
)

func TestAESCBC(t *testing.T) {
	key, err := GenerateKey32()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	plaintext := []byte("Hello, AES CBC with PKCS#7 Padding!")

	// Encrypt
	ciphertext, err := EncryptAESCBC(key, plaintext)
	if err != nil {
		t.Fatalf("encryption failed: %v", err)
	}

	// Decrypt
	decrypted, err := DecryptAESCBC(key, ciphertext)
	if err != nil {
		t.Fatalf("decryption failed: %v", err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Errorf("decrypted data doesn't match original. expected %s, got %s", string(plaintext), string(decrypted))
	}
}

func TestPKCS7(t *testing.T) {
	blockSize := 16
	data := []byte("test")
	
	padded := pkcs7Pad(data, blockSize)
	if len(padded) != 16 {
		t.Errorf("expected padded length 16, got %d", len(padded))
	}
	if padded[len(padded)-1] != 12 {
		t.Errorf("expected padding byte 12, got %d", padded[len(padded)-1])
	}

	unpadded, err := pkcs7Unpad(padded, blockSize)
	if err != nil {
		t.Fatalf("unpad failed: %v", err)
	}

	if !bytes.Equal(data, unpadded) {
		t.Errorf("unpadded data doesn't match original")
	}

	// Invalid padding
	invalidPadded := make([]byte, 16)
	copy(invalidPadded, padded)
	invalidPadded[15] = 99 // Corrupt the padding byte
	_, err = pkcs7Unpad(invalidPadded, blockSize)
	if err == nil {
		t.Error("expected error for invalid padding")
	}
}
