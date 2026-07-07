package crypt

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateRandomBytes generates cryptographically secure random bytes of given length.
func GenerateRandomBytes(length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := rand.Read(b)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random bytes: %w", err)
	}
	return b, nil
}

// GenerateKey32 generates a 32-byte (256-bit) cryptographically secure random key.
func GenerateKey32() ([]byte, error) {
	return GenerateRandomBytes(32)
}

// GenerateRandomString generates a cryptographically secure random hex string of given byte length.
// Note: The resulting string will be twice as long as byteLength.
func GenerateRandomString(byteLength int) (string, error) {
	b, err := GenerateRandomBytes(byteLength)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
