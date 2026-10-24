// Package authhttp contains framework-neutral HTTP authentication helpers used
// by the Fiber, Gin, Echo, and net/http middleware adapters.
package authhttp

import "strings"

// BearerToken extracts a non-empty token from an Authorization header using
// the Bearer scheme. Scheme matching is case-insensitive.
func BearerToken(header string) string {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
		return ""
	}
	return parts[1]
}
