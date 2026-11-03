package authhttp

import (
	"errors"
	"fmt"
	"github.com/thanhbvha/go-common/v2/auth"
	"strings"
	"testing"
)

func TestBearerToken(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "standard", header: "Bearer token", want: "token"},
		{name: "case insensitive", header: "bEaReR token", want: "token"},
		{name: "surrounding whitespace", header: "  Bearer   token  ", want: "token"},
		{name: "missing", header: "", want: ""},
		{name: "wrong scheme", header: "Basic token", want: ""},
		{name: "missing token", header: "Bearer", want: ""},
		{name: "extra parts", header: "Bearer token extra", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BearerToken(tt.header); got != tt.want {
				t.Fatalf("BearerToken(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}

func TestFailureDistinguishesAvailability(t *testing.T) {
	for _, err := range []error{auth.ErrInvalidToken, auth.ErrExpiredToken, auth.ErrTokenRevoked, errors.New("unclassified failure")} {
		if status, _ := Failure(err); status != 401 {
			t.Fatalf("credential status=%d", status)
		}
	}
	cause := errors.New("private backend detail")
	status, message := Failure(fmt.Errorf("%w: %w", auth.ErrAuthBackendUnavailable, cause))
	if status != 503 || strings.Contains(message, "private") {
		t.Fatalf("status=%d message=%q", status, message)
	}
}
