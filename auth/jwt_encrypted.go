package auth

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/thanhbvha/go-common/utils/crypt"
)

var (
	ErrDecryptionFailed = errors.New("auth: failed to decrypt token payload")
)

// EncryptedClaims represents a JWT where the entire UserInfo is encrypted.
type EncryptedClaims struct {
	Payload string `json:"payload"` // Base64 encoded AES-GCM ciphertext
	jwt.RegisteredClaims
}

// EncryptedManager provides JWT generation and validation with AES-256 GCM encrypted payloads.
// This ensures that sensitive user information cannot be read even if the token is intercepted.
type EncryptedManager struct {
	jwtSecret string
	aesKey    []byte
}

// NewEncryptedManager creates a new manager for encrypted JWTs.
// aesKey must be exactly 32 bytes (256-bit) for AES-256 GCM.
func NewEncryptedManager(jwtSecret string, aesKey string) (*EncryptedManager, error) {
	keyBytes := []byte(aesKey)
	if len(keyBytes) != 32 {
		return nil, crypt.ErrInvalidKeySize
	}
	return &EncryptedManager{
		jwtSecret: jwtSecret,
		aesKey:    keyBytes,
	}, nil
}

// GenerateToken encrypts the user info and signs it as a JWT.
func (m *EncryptedManager) GenerateToken(user UserInfo, duration time.Duration) (string, error) {
	// 1. Serialize UserInfo to JSON
	userBytes, err := json.Marshal(user)
	if err != nil {
		return "", fmt.Errorf("failed to marshal user info: %w", err)
	}

	// 2. Encrypt the JSON payload using AES-256 GCM
	ciphertext, err := crypt.EncryptAESGCM(m.aesKey, userBytes, nil)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt payload: %w", err)
	}

	// 3. Encode to Base64 to safely embed in JSON
	payloadB64 := base64.StdEncoding.EncodeToString(ciphertext)

	now := time.Now()
	claims := EncryptedClaims{
		Payload: payloadB64,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(duration)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	// 4. Sign the JWT
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString([]byte(m.jwtSecret))
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return signedToken, nil
}

// ValidateToken validates the JWT signature, decrypts the payload, and returns the UserInfo.
func (m *EncryptedManager) ValidateToken(tokenString string) (*UserInfo, error) {
	// 1. Parse and validate the JWT signature
	token, err := jwt.ParseWithClaims(tokenString, &EncryptedClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(m.jwtSecret), nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*EncryptedClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	// 2. Decode the Base64 payload
	ciphertext, err := base64.StdEncoding.DecodeString(claims.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid base64 encoding", ErrDecryptionFailed)
	}

	// 3. Decrypt the payload using AES-256 GCM
	plaintext, err := crypt.DecryptAESGCM(m.aesKey, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecryptionFailed, err)
	}

	// 4. Deserialize JSON back to UserInfo
	var user UserInfo
	if err := json.Unmarshal(plaintext, &user); err != nil {
		return nil, fmt.Errorf("%w: invalid json structure", ErrDecryptionFailed)
	}

	return &user, nil
}
