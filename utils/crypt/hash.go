package crypt

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"

	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/sha3"
)

// HashMD5 calculates the MD5 hash (Not recommended for security).
func HashMD5(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}

// HashSHA1 calculates the SHA-1 hash (Not recommended for security).
func HashSHA1(data []byte) string {
	hash := sha1.Sum(data)
	return hex.EncodeToString(hash[:])
}

// HashSHA256 calculates the SHA-256 hash (Current industry standard).
func HashSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// HashSHA512 calculates the SHA-512 hash.
func HashSHA512(data []byte) string {
	hash := sha512.Sum512(data)
	return hex.EncodeToString(hash[:])
}

// HashSHA3_256 calculates the SHA3-256 hash (Modern, highly secure).
func HashSHA3_256(data []byte) string {
	hash := sha3.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// HashBLAKE2b calculates the BLAKE2b-256 hash (Fast, highly secure).
// Returns an error if initialization fails.
func HashBLAKE2b(data []byte) (string, error) {
	hash, err := blake2b.New256(nil)
	if err != nil {
		return "", err
	}
	hash.Write(data)
	return hex.EncodeToString(hash.Sum(nil)), nil
}
