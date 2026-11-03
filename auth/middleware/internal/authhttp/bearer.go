// Package authhttp contains framework-neutral HTTP authentication helpers used
// by the Fiber, Gin, Echo, and net/http middleware adapters.
package authhttp

import (
	"errors"
	"net/http"
	"strings"

	"github.com/thanhbvha/go-common/v2/auth"
)

// BearerToken extracts a non-empty token from an Authorization header using
// the Bearer scheme. Scheme matching is case-insensitive.
func BearerToken(header string) string {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return parts[1]
}

// Failure exposes only a generic status/message. Diagnostic causes stay in the
// validation error for application logging and errors.Is.
func Failure(err error) (int, string) {
	if errors.Is(err, auth.ErrAuthBackendUnavailable) {
		return http.StatusServiceUnavailable, "Authentication service unavailable"
	}
	return http.StatusUnauthorized, "Invalid or expired token"
}
