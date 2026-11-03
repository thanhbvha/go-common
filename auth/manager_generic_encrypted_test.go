package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type genericPayload struct {
	User   string `json:"user"`
	Secret string `json:"secret"`
}

func TestGenericEncryptedSharesStrictPolicy(t *testing.T) {
	now := time.Unix(1800000000, 0)
	opts := GenericEncryptedManagerOptions[genericPayload]{EncryptedManagerOptions: EncryptedManagerOptions{SecretKey: strings.Repeat("s", 32), AESKey: strings.Repeat("e", 32), Issuer: "issuer", Audience: []string{"api"}, MaxLifetime: time.Hour, Clock: func() time.Time { return now }, AADPolicy: RequireAAD}, Subject: func(p genericPayload) (string, error) { return p.User, nil }}
	manager, err := NewGenericEncryptedManagerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	payload := genericPayload{User: "user", Secret: "sensitive"}
	for _, ttl := range []time.Duration{0, -time.Second, 2 * time.Hour} {
		if _, err := manager.GenerateTokenContext(context.Background(), payload, ttl, []byte("session")); err == nil {
			t.Fatalf("accepted TTL %s", ttl)
		}
	}
	token, err := manager.GenerateTokenContext(context.Background(), payload, time.Hour, []byte("session"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := manager.ValidateTokenContext(context.Background(), token, []byte("session"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Payload != payload || got.ID == "" || got.Subject != "user" || got.Issuer != "issuer" {
		t.Fatalf("result=%+v", got)
	}
	if _, err = manager.ValidateTokenContext(context.Background(), token, []byte("wrong")); err == nil {
		t.Fatal("wrong AAD accepted")
	}
	if _, err = manager.ValidateTokenContext(context.Background(), strings.Repeat("x", DefaultMaxTokenSize+1), []byte("session")); !errors.Is(err, ErrTokenTooLarge) {
		t.Fatalf("oversized error=%v", err)
	}
	claims := EncryptedClaims{Payload: "unused", RegisteredClaims: jwt.RegisteredClaims{Subject: "user"}}
	bad, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(strings.Repeat("s", 32)))
	if _, err = manager.ValidateTokenContext(context.Background(), bad, []byte("session")); err == nil {
		t.Fatal("missing expiration accepted")
	}
}

func TestGenericEncryptedRevocationAndRotation(t *testing.T) {
	checker := &stubRevocationChecker{}
	opts := GenericEncryptedManagerOptions[genericPayload]{EncryptedManagerOptions: EncryptedManagerOptions{SecretKey: strings.Repeat("s", 32), AESKey: strings.Repeat("e", 32), Revocation: checker}, Subject: func(p genericPayload) (string, error) { return p.User, nil }}
	manager, err := NewGenericEncryptedManagerWithOptions(opts)
	if err != nil {
		t.Fatal(err)
	}
	token, err := manager.GenerateTokenContext(context.Background(), genericPayload{User: "u"}, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	checker.revoked = true
	if _, err = manager.ValidateTokenContext(context.Background(), token, nil); !errors.Is(err, ErrTokenRevoked) {
		t.Fatalf("revoked error=%v", err)
	}
	checker.revoked = false
	cause := errors.New("revocation backend unavailable")
	checker.err = cause
	if _, err = manager.ValidateTokenContext(context.Background(), token, nil); !errors.Is(err, ErrAuthBackendUnavailable) || !errors.Is(err, cause) {
		t.Fatalf("backend classification=%v", err)
	}
}

func TestGenericEncryptedRejectsNilPayloadBeforeSubjectExtraction(t *testing.T) {
	manager, err := NewGenericEncryptedManagerWithOptions(GenericEncryptedManagerOptions[*genericPayload]{EncryptedManagerOptions: EncryptedManagerOptions{SecretKey: strings.Repeat("s", 32), AESKey: strings.Repeat("e", 32)}, Subject: func(p *genericPayload) (string, error) { return p.User, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GenerateTokenContext(context.Background(), nil, time.Hour, nil); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatal(err)
	}
	token, err := manager.engine.generatePayload(context.Background(), "u", (*genericPayload)(nil), time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ValidateTokenContext(context.Background(), token, nil); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatal(err)
	}
}
