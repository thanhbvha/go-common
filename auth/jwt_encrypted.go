package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"github.com/golang-jwt/jwt/v5"
	"github.com/thanhbvha/go-common/utils/crypt"
)

var (
	ErrDecryptionFailed = errors.New("auth: failed to decrypt token payload")
)

// EncryptedClaims represents a JWT where the entire UserInfo is encrypted.
type EncryptedClaims struct {
	Version         int    `json:"v,omitempty"`
	Payload         string `json:"payload"` // Base64 encoded AES-GCM ciphertext
	EncryptionKeyID string `json:"ekid,omitempty"`
	jwt.RegisteredClaims
}

// EncryptedManager provides JWT generation and validation with AES-256 GCM encrypted payloads.
// This ensures that sensitive user information cannot be read even if the token is intercepted.
type EncryptedManager struct {
	signingKeys       KeyProvider
	encryptionKeys    KeyProvider
	revocation        RevocationChecker
	issuer            string
	audience          []string
	leeway            time.Duration
	maxLifetime       time.Duration
	clock             Clock
	maxTokenSize      int
	maxPayloadSize    int
	aadPolicy         AADPolicy
	parser            *jwt.Parser
	aead              aeadCache
	allowLegacyClaims bool
}

// NewEncryptedManager creates a new manager for encrypted JWTs.
// jwtSecret must be at least 32 characters. aesKey must be exactly 32 bytes (256-bit) for AES-256 GCM.
//
// Deprecated: use NewEncryptedManagerWithOptions to configure claim policy,
// key rotation, revocation, size limits, and AAD requirements.
func NewEncryptedManager(jwtSecret string, aesKey string) (*EncryptedManager, error) {
	return NewEncryptedManagerWithOptions(EncryptedManagerOptions{SecretKey: jwtSecret, AESKey: aesKey, AllowLegacyClaims: true})
}

// NewEncryptedManagerWithOptions creates an encrypted JWT manager with
// claim-validation policy.
func NewEncryptedManagerWithOptions(opts EncryptedManagerOptions) (*EncryptedManager, error) {
	if opts.MaxTokenSize < 0 || opts.MaxPayloadSize < 0 {
		return nil, ErrInvalidDuration
	}
	signingProvider := opts.SigningKeys
	if signingProvider == nil {
		if len(opts.SecretKey) < 32 {
			return nil, ErrInvalidKey
		}
		var err error
		signingProvider, err = singleKeyProvider(opts.SecretKey)
		if err != nil {
			return nil, err
		}
	}
	encryptionProvider := opts.EncryptionKeys
	if encryptionProvider == nil {
		if len([]byte(opts.AESKey)) != 32 {
			return nil, crypt.ErrInvalidKeySize
		}
		var err error
		encryptionProvider, err = singleKeyProvider(opts.AESKey)
		if err != nil {
			return nil, err
		}
	}
	issuer, audience, leeway, maxLifetime, clock, err := normalizeTokenOptions(opts.Issuer, opts.Audience, opts.Leeway, opts.MaxLifetime, opts.Clock)
	if err != nil {
		return nil, err
	}

	return &EncryptedManager{
		signingKeys:       signingProvider,
		encryptionKeys:    encryptionProvider,
		revocation:        opts.Revocation,
		issuer:            issuer,
		audience:          audience,
		leeway:            leeway,
		maxLifetime:       maxLifetime,
		clock:             clock,
		maxTokenSize:      normalizeSizeLimit(opts.MaxTokenSize, DefaultMaxTokenSize),
		maxPayloadSize:    normalizeSizeLimit(opts.MaxPayloadSize, DefaultMaxPayloadSize),
		aadPolicy:         opts.AADPolicy,
		parser:            jwt.NewParser(parserOptions(issuer, audience, leeway, clock)...),
		allowLegacyClaims: opts.AllowLegacyClaims,
	}, nil
}

// GenerateToken encrypts the user info and signs it as a JWT.
func (m *EncryptedManager) GenerateToken(user UserInfo, duration time.Duration, aad []byte) (string, error) {
	return m.GenerateTokenContext(context.Background(), user, duration, aad)
}

// GenerateTokenContext encrypts and signs a token with the active versioned keys.
func (m *EncryptedManager) GenerateTokenContext(ctx context.Context, user UserInfo, duration time.Duration, aad []byte) (string, error) {
	if m == nil {
		return "", ErrInvalidManager
	}
	if strings.TrimSpace(user.ID) == "" {
		return "", ErrInvalidIdentity
	}
	if err := validateLifetime(duration, m.maxLifetime); err != nil {
		return "", err
	}
	if err := validateAAD(ctx, m.aadPolicy, aad); err != nil {
		return "", err
	}
	tokenID, err := newTokenID()
	if err != nil {
		return "", err
	}
	signingKey, err := currentKey(ctx, m.signingKeys, false)
	if err != nil {
		return "", err
	}
	encryptionKey, err := currentKey(ctx, m.encryptionKeys, true)
	if err != nil {
		return "", err
	}
	// 1. Serialize UserInfo to JSON
	userBytes, err := json.Marshal(user)
	if err != nil {
		return "", fmt.Errorf("failed to marshal user info: %w", err)
	}
	if len(userBytes) > m.maxPayloadSize {
		return "", ErrPayloadTooLarge
	}

	// 2. Encrypt the JSON payload using AES-256 GCM
	ciphertext, err := m.aead.encrypt(encryptionKey.ID, encryptionKey.Value, userBytes, aad)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt payload: %w", err)
	}

	// 3. Encode to Base64 to safely embed in JSON
	payloadB64 := base64.StdEncoding.EncodeToString(ciphertext)

	now := m.clock()
	claims := EncryptedClaims{
		Version:         1,
		Payload:         payloadB64,
		EncryptionKeyID: encryptionKey.ID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(duration)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    m.issuer,
			Audience:  jwt.ClaimStrings(m.audience),
			Subject:   user.ID,
			ID:        tokenID,
		},
	}

	// 4. Sign the JWT
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = signingKey.ID
	signedToken, err := token.SignedString(signingKey.Value)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}
	if len(signedToken) > m.maxTokenSize {
		return "", ErrTokenTooLarge
	}

	return signedToken, nil
}

// ValidateToken validates the JWT signature, decrypts the payload, and returns the UserInfo.
func (m *EncryptedManager) ValidateToken(tokenString string, aad []byte) (*UserInfo, error) {
	return m.ValidateTokenContext(context.Background(), tokenString, aad)
}

// ValidateTokenDetails validates an encrypted token and returns its user data
// together with registered claims.
func (m *EncryptedManager) ValidateTokenDetails(tokenString string, aad []byte) (*TokenDetails, error) {
	return m.ValidateTokenDetailsContext(context.Background(), tokenString, aad)
}

// ValidateTokenContext validates and decrypts using keys selected by kid and ekid.
func (m *EncryptedManager) ValidateTokenContext(ctx context.Context, tokenString string, aad []byte) (*UserInfo, error) {
	details, err := m.ValidateTokenDetailsContext(ctx, tokenString, aad)
	if err != nil {
		return nil, err
	}
	return &details.UserInfo, nil
}

// ValidateTokenDetailsContext returns user data and registered claims for
// authorization, auditing, and revocation-aware middleware.
func (m *EncryptedManager) ValidateTokenDetailsContext(ctx context.Context, tokenString string, aad []byte) (*TokenDetails, error) {
	if m == nil {
		return nil, ErrInvalidManager
	}
	if len(tokenString) == 0 {
		return nil, ErrInvalidToken
	}
	if len(tokenString) > m.maxTokenSize {
		return nil, ErrTokenTooLarge
	}
	if err := validateAAD(ctx, m.aadPolicy, aad); err != nil {
		return nil, err
	}
	// 1. Parse and validate the JWT signature
	token, err := m.parser.ParseWithClaims(tokenString, &EncryptedClaims{}, func(t *jwt.Token) (interface{}, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		keyID, _ := t.Header["kid"].(string)
		return resolveKey(ctx, m.signingKeys, keyID, false)
	})

	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(*EncryptedClaims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}
	if claims.Version != 0 && claims.Version != 1 {
		return nil, ErrInvalidToken
	}

	// 2. Decode the Base64 payload
	ciphertext, err := base64.StdEncoding.DecodeString(claims.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid base64 encoding", ErrDecryptionFailed)
	}

	// 3. Decrypt the payload using AES-256 GCM
	encryptionKey, err := resolveKey(ctx, m.encryptionKeys, claims.EncryptionKeyID, true)
	if err != nil {
		return nil, fmt.Errorf("%w: encryption key unavailable", ErrDecryptionFailed)
	}
	plaintext, err := m.aead.decrypt(claims.EncryptionKeyID, encryptionKey, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDecryptionFailed, err)
	}
	if len(plaintext) > m.maxPayloadSize {
		return nil, ErrPayloadTooLarge
	}

	// 4. Deserialize JSON back to UserInfo
	var user UserInfo
	if err := json.Unmarshal(plaintext, &user); err != nil {
		return nil, fmt.Errorf("%w: invalid json structure", ErrDecryptionFailed)
	}
	if err := validateUserClaims(user, claims.RegisteredClaims, m.maxLifetime, m.allowLegacyClaims); err != nil {
		return nil, err
	}
	if err := checkRevocation(ctx, m.revocation, claims.ID); err != nil {
		return nil, err
	}

	return &TokenDetails{
		UserInfo:         user,
		RegisteredClaims: claims.RegisteredClaims,
		Encrypted:        true,
		EncryptionKeyID:  claims.EncryptionKeyID,
		FormatVersion:    claims.Version,
	}, nil
}
