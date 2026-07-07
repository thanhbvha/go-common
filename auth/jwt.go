package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken = errors.New("auth: invalid token")
	ErrExpiredToken = errors.New("auth: token is expired")
	ErrInvalidKey   = errors.New("auth: invalid signing key")
)

// Manager is the core component for standard JWT operations.
type Manager struct {
	secretKey []byte
}

// NewManager creates a new JWT Manager.
func NewManager(secretKey string) *Manager {
	return &Manager{
		secretKey: []byte(secretKey),
	}
}

// GenerateToken creates a signed JWT for the given user information.
func (m *Manager) GenerateToken(user UserInfo, duration time.Duration) (string, error) {
	now := time.Now()
	claims := CustomClaims{
		UserInfo: user,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(duration)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString(m.secretKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return signedToken, nil
}

// ValidateToken parses and validates a signed JWT string.
func (m *Manager) ValidateToken(tokenString string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &CustomClaims{}, func(t *jwt.Token) (interface{}, error) {
		// Ensure the signing method is what we expect
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return m.secretKey, nil
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*CustomClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}

// ExtractUserInfo is a convenient wrapper around ValidateToken to return just the UserInfo.
func (m *Manager) ExtractUserInfo(tokenString string) (*UserInfo, error) {
	claims, err := m.ValidateToken(tokenString)
	if err != nil {
		return nil, err
	}
	return &claims.UserInfo, nil
}
