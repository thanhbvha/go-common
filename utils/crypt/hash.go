package crypt

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/sha3"
)

// HashSHA256 returns the lowercase hexadecimal SHA-256 digest of data.
func HashSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// HashSHA512 returns the lowercase hexadecimal SHA-512 digest of data.
func HashSHA512(data []byte) string {
	hash := sha512.Sum512(data)
	return hex.EncodeToString(hash[:])
}

// HashSHA3_256 returns the lowercase hexadecimal SHA3-256 digest of data.
func HashSHA3_256(data []byte) string {
	hash := sha3.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// HashBLAKE2b returns the lowercase hexadecimal BLAKE2b-256 digest of data.
func HashBLAKE2b(data []byte) string {
	hash := blake2b.Sum256(data)
	return hex.EncodeToString(hash[:])
}
