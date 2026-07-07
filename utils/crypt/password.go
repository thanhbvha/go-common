package crypt

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters (OWASP recommended defaults for 2023+)
const (
	time        = 1      // 1 iteration
	memory      = 64 * 1024 // 64 MB
	threads     = 4      // 4 threads
	keyLen      = 32     // 32 bytes key length
	saltLen     = 16     // 16 bytes salt length
)

var (
	ErrInvalidHash         = errors.New("crypt: the encoded hash is not in the correct format")
	ErrIncompatibleVersion = errors.New("crypt: incompatible version of argon2")
)

// HashPasswordArgon2id hashes a password using Argon2id with recommended parameters.
// Returns a base64 encoded string in the format: $argon2id$v=19$m=65536,t=1,p=4$<salt>$<hash>
func HashPasswordArgon2id(password string) (string, error) {
	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	hash := argon2.IDKey([]byte(password), salt, time, memory, uint8(threads), keyLen)

	b64Salt := base64.RawStdEncoding.EncodeToString(salt)
	b64Hash := base64.RawStdEncoding.EncodeToString(hash)

	encodedHash := fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, memory, time, threads, b64Salt, b64Hash,
	)

	return encodedHash, nil
}

// VerifyPasswordArgon2id compares a plain text password with a stored Argon2id hash.
func VerifyPasswordArgon2id(password, encodedHash string) (bool, error) {
	vals := strings.Split(encodedHash, "$")
	if len(vals) != 6 {
		return false, ErrInvalidHash
	}

	var version int
	_, err := fmt.Sscanf(vals[2], "v=%d", &version)
	if err != nil {
		return false, err
	}
	if version != argon2.Version {
		return false, ErrIncompatibleVersion
	}

	var mem, t, p int
	_, err = fmt.Sscanf(vals[3], "m=%d,t=%d,p=%d", &mem, &t, &p)
	if err != nil {
		return false, err
	}

	salt, err := base64.RawStdEncoding.DecodeString(vals[4])
	if err != nil {
		return false, err
	}

	hash, err := base64.RawStdEncoding.DecodeString(vals[5])
	if err != nil {
		return false, err
	}
	if len(hash) != keyLen {
		return false, ErrInvalidHash
	}

	comparisonHash := argon2.IDKey([]byte(password), salt, uint32(t), uint32(mem), uint8(p), uint32(len(hash)))

	return subtle.ConstantTimeCompare(hash, comparisonHash) == 1, nil
}
