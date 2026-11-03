package auth_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/auth"
)

type TestClaims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

func setupGenericManager(t *testing.T) (auth.GenericManager[*TestClaims], *miniredis.Miniredis) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	mgr, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*TestClaims]{
		SecretKey: "super-secret-jwt-key-must-be-long",
		Redis:     client,
		Namespace: "test",
	})
	if err != nil {
		t.Fatalf("failed to create generic manager: %v", err)
	}

	return mgr, s
}

func TestGenericManager_GenerateAndVerifyToken(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	claims := &TestClaims{
		UserID: "user_123",
		Role:   "admin",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(1 * time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()), NotBefore: jwt.NewNumericDate(time.Now()), Subject: "user_123", ID: "jwt-1",
		},
	}

	token, err := mgr.GenerateToken(claims)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if token == "" {
		t.Fatal("expected a valid token string")
	}

	// Verify
	var parsedClaims TestClaims
	err = mgr.VerifyToken(token, &parsedClaims)
	if err != nil {
		t.Fatalf("expected no error during verification, got %v", err)
	}

	if parsedClaims.UserID != "user_123" {
		t.Errorf("expected UserID to be 'user_123', got '%s'", parsedClaims.UserID)
	}
	if parsedClaims.Role != "admin" {
		t.Errorf("expected Role to be 'admin', got '%s'", parsedClaims.Role)
	}
}

func TestGenericManager_RejectsUnsafeInputsAndAlgorithms(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	ctx := context.Background()
	if err := mgr.RevokeToken(ctx, "jti", 0); !errors.Is(err, auth.ErrInvalidDuration) {
		t.Fatalf("zero revocation TTL error = %v", err)
	}

	claims := &TestClaims{UserID: "user", RegisteredClaims: jwt.RegisteredClaims{
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}}
	unsafeToken, err := jwt.NewWithClaims(jwt.SigningMethodHS384, claims).SignedString([]byte("super-secret-jwt-key-must-be-long"))
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.VerifyToken(unsafeToken, &TestClaims{}); !errors.Is(err, auth.ErrInvalidToken) {
		t.Fatalf("HS384 verification error = %v", err)
	}
}

func TestGenericManager_RevocationRequiresRedis(t *testing.T) {
	mgr, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*TestClaims]{SecretKey: "super-secret-jwt-key-must-be-long"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.IsRevoked(context.Background(), "jti"); !errors.Is(err, auth.ErrRedisNotConfigured) {
		t.Fatalf("revocation without Redis error = %v", err)
	}
}

func TestGenericManager_TokenRevocation(t *testing.T) {
	mgr, mr := setupGenericManager(t)
	defer mr.Close()

	ctx := context.Background()
	jti := "unique-jti-789"

	isRevoked, err := mgr.IsRevoked(ctx, jti)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if isRevoked {
		t.Fatal("expected token to not be revoked yet")
	}

	err = mgr.RevokeToken(ctx, jti, 15*time.Minute)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	isRevoked, err = mgr.IsRevoked(ctx, jti)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !isRevoked {
		t.Fatal("expected token to be revoked")
	}
}

func TestGenericManagerWithOptionsPolicyRotationAndRevocation(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer func() { _ = client.Close() }()

	keys := map[string][]byte{
		"v1": []byte("generic-signing-key-v1-at-least-32-bytes"),
		"v2": []byte("generic-signing-key-v2-at-least-32-bytes"),
	}
	v1, err := auth.NewStaticKeyProvider("v1", keys)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := auth.NewStaticKeyProvider("v2", keys)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	options := func(provider auth.KeyProvider) auth.GenericManagerOptions[*TestClaims] {
		return auth.GenericManagerOptions[*TestClaims]{
			SigningKeys: provider,
			Redis:       client,
			Namespace:   "test",
			Issuer:      "identity-service",
			Audience:    []string{"api"},
			MaxLifetime: time.Hour,
			Clock:       func() time.Time { return now },
			TokenID:     func(claims *TestClaims) (string, error) { return claims.ID, nil },
		}
	}
	issuer, err := auth.NewGenericManagerWithOptions(options(v1))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := auth.NewGenericManagerWithOptions(options(v2))
	if err != nil {
		t.Fatal(err)
	}
	claims := &TestClaims{
		UserID: "user-1",
		Role:   "admin",
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(now.Add(30 * time.Minute)),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    "identity-service",
			Audience:  jwt.ClaimStrings{"api"},
			Subject:   "user-1",
			ID:        "token-1",
		},
	}
	token, err := issuer.GenerateTokenContext(context.Background(), claims)
	if err != nil {
		t.Fatal(err)
	}
	parsed := &TestClaims{}
	if err := rotated.VerifyTokenContext(context.Background(), token, parsed); err != nil {
		t.Fatalf("rotated manager rejected v1 token: %v", err)
	}
	if parsed.UserID != claims.UserID {
		t.Fatalf("parsed claims = %#v", parsed)
	}
	if err := rotated.RevokeToken(context.Background(), claims.ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := rotated.VerifyTokenContext(context.Background(), token, &TestClaims{}); !errors.Is(err, auth.ErrTokenRevoked) {
		t.Fatalf("revoked generic token error = %v", err)
	}
}

func TestGenericManagerWithOptionsRejectsIncompleteRevocationConfig(t *testing.T) {
	_, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*TestClaims]{
		SecretKey:  "super-secret-jwt-key-must-be-long",
		Revocation: stubChecker{},
	})
	if !errors.Is(err, auth.ErrTokenIDExtractorRequired) {
		t.Fatalf("missing token ID extractor error = %v", err)
	}
	_, err = auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*TestClaims]{
		SecretKey: "super-secret-jwt-key-must-be-long",
		TokenID:   func(claims *TestClaims) (string, error) { return claims.ID, nil },
	})
	if !errors.Is(err, auth.ErrRedisNotConfigured) {
		t.Fatalf("missing revocation backend error = %v", err)
	}
}

type stubChecker struct{}

func (stubChecker) IsRevoked(context.Context, string) (bool, error) { return false, nil }

func TestRedisRevocationNamespacesAreIsolated(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	makeManager := func(namespace string) auth.ContextGenericManager[*TestClaims] {
		manager, err := auth.NewGenericManagerWithOptions(auth.GenericManagerOptions[*TestClaims]{SecretKey: "super-secret-jwt-key-must-be-long", Redis: client, Namespace: namespace})
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	a, b := makeManager("project-a"), makeManager("project-b")
	if err := a.RevokeToken(context.Background(), "same-id", time.Hour); err != nil {
		t.Fatal(err)
	}
	if revoked, err := b.IsRevoked(context.Background(), "same-id"); err != nil || revoked {
		t.Fatalf("namespace crossed: %v %v", revoked, err)
	}
}
