package auth

import (
	"context"
	"errors"
	"testing"
	"time"
)

type stubRevocationChecker struct {
	revoked bool
	err     error
	ctx     context.Context
}

func (s *stubRevocationChecker) IsRevoked(ctx context.Context, _ string) (bool, error) {
	s.ctx = ctx
	return s.revoked, s.err
}

func TestManagerRevocationPolicy(t *testing.T) {
	checker := &stubRevocationChecker{}
	manager, err := NewManagerWithOptions(ManagerOptions{
		SecretKey:  "jwt-secret-key-that-is-at-least-32-bytes",
		Revocation: checker,
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := manager.GenerateToken(UserInfo{ID: "user-1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), struct{}{}, "request")
	if _, err := manager.ValidateTokenContext(ctx, token); err != nil {
		t.Fatal(err)
	}
	if checker.ctx != ctx {
		t.Fatal("request context was not propagated to revocation checker")
	}
	checker.revoked = true
	if _, err := manager.ValidateTokenContext(ctx, token); !errors.Is(err, ErrTokenRevoked) {
		t.Fatalf("revoked token error = %v", err)
	}
	checker.revoked = false
	checker.err = errors.New("redis unavailable")
	if _, err := manager.ValidateTokenContext(ctx, token); err == nil {
		t.Fatal("expected fail-closed revocation error")
	}
}
