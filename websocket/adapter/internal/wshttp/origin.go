// Package wshttp contains framework-neutral HTTP policy shared by WebSocket
// adapters.
package wshttp

import (
	"net/url"
	"strings"
)

// CheckSameOrigin accepts requests without an Origin header and requests whose
// Origin host matches the HTTP Host header. Applications that intentionally
// support cross-origin WebSockets must provide an explicit allowlist policy.
func CheckSameOrigin(origin, host string) bool {
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	return strings.EqualFold(parsed.Host, host)
}
