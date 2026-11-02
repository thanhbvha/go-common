package crypt

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

const (
	passwordMemory         uint32 = 19 * 1024
	passwordIterations     uint32 = 2
	passwordParallelism    uint8  = 1
	passwordSaltLength            = 16
	passwordKeyLength             = 32
	maxEncodedHashLength          = 512
	maxPasswordMemory      uint64 = 64 * 1024
	maxPasswordIterations  uint64 = 5
	maxPasswordParallelism uint64 = 4
)

type passwordHash struct {
	memory      uint32
	iterations  uint32
	parallelism uint8
	salt        []byte
	hash        []byte
}

// HashPassword hashes password with Argon2id using the current production
// parameters. Each result contains a new cryptographically random salt.
func HashPassword(password string) (string, error) {
	salt := make([]byte, passwordSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("crypt: generate password salt: %w", err)
	}

	hash := argon2.IDKey([]byte(password), salt, passwordIterations, passwordMemory, passwordParallelism, passwordKeyLength)
	return fmt.Sprintf(
		"$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		passwordMemory,
		passwordIterations,
		passwordParallelism,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(hash),
	), nil
}

// VerifyPassword compares password with an encoded Argon2id hash. It validates
// and bounds every cost parameter before allocating memory.
func VerifyPassword(password, encodedHash string) (bool, error) {
	parsed, err := parsePasswordHash(encodedHash)
	if err != nil {
		return false, err
	}
	comparison := argon2.IDKey([]byte(password), parsed.salt, parsed.iterations, parsed.memory, parsed.parallelism, uint32(len(parsed.hash)))
	return subtle.ConstantTimeCompare(parsed.hash, comparison) == 1, nil
}

// PasswordHashNeedsUpgrade reports whether a valid hash uses older cost
// parameters and should be replaced after the next successful login.
func PasswordHashNeedsUpgrade(encodedHash string) (bool, error) {
	parsed, err := parsePasswordHash(encodedHash)
	if err != nil {
		return false, err
	}
	return parsed.memory != passwordMemory || parsed.iterations != passwordIterations || parsed.parallelism != passwordParallelism, nil
}

func parsePasswordHash(encoded string) (passwordHash, error) {
	invalid := func(cause error) (passwordHash, error) {
		if cause == nil {
			return passwordHash{}, ErrInvalidPasswordHash
		}
		return passwordHash{}, fmt.Errorf("%w: %v", ErrInvalidPasswordHash, cause)
	}
	if len(encoded) == 0 || len(encoded) > maxEncodedHashLength {
		return invalid(errors.New("unexpected encoded length"))
	}
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return invalid(errors.New("unexpected format or algorithm"))
	}
	version, err := parseUintField(parts[2], "v=")
	if err != nil || version != argon2.Version {
		return invalid(errors.New("unsupported Argon2 version"))
	}

	costs := strings.Split(parts[3], ",")
	if len(costs) != 3 {
		return invalid(errors.New("unexpected cost parameters"))
	}
	memory, err := parseUintField(costs[0], "m=")
	if err != nil || memory == 0 || memory > maxPasswordMemory {
		return invalid(errors.New("memory cost is outside the supported range"))
	}
	iterations, err := parseUintField(costs[1], "t=")
	if err != nil || iterations == 0 || iterations > maxPasswordIterations {
		return invalid(errors.New("iteration cost is outside the supported range"))
	}
	parallelism, err := parseUintField(costs[2], "p=")
	if err != nil || parallelism == 0 || parallelism > maxPasswordParallelism {
		return invalid(errors.New("parallelism is outside the supported range"))
	}

	salt, err := base64.RawStdEncoding.Strict().DecodeString(parts[4])
	if err != nil || len(salt) != passwordSaltLength {
		return invalid(errors.New("invalid salt"))
	}
	hash, err := base64.RawStdEncoding.Strict().DecodeString(parts[5])
	if err != nil || len(hash) != passwordKeyLength {
		return invalid(errors.New("invalid derived key"))
	}

	return passwordHash{
		memory:      uint32(memory),
		iterations:  uint32(iterations),
		parallelism: uint8(parallelism),
		salt:        salt,
		hash:        hash,
	}, nil
}

func parseUintField(value, prefix string) (uint64, error) {
	if !strings.HasPrefix(value, prefix) || len(value) == len(prefix) {
		return 0, ErrInvalidPasswordHash
	}
	return strconv.ParseUint(value[len(prefix):], 10, 32)
}
