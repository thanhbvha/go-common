// Package nethttplimit provides rate limiting middleware for the standard
// library's net/http package.
package nethttplimit

import (
	"net/http"
	"strconv"
	"time"

	"github.com/thanhbvha/go-common/internal/httpkit"
	"github.com/thanhbvha/go-common/ratelimit"
	httpresp "github.com/thanhbvha/go-common/web/response/nethttp"
)

const (
	headerLimit     = "X-RateLimit-Limit"
	headerRemaining = "X-RateLimit-Remaining"
	headerReset     = "X-RateLimit-Reset"
)

// KeyGenerator extracts a unique rate-limit key from a request.
type KeyGenerator func(*http.Request) string

// DefaultKeyGenerator uses the direct peer IP. It intentionally does not trust
// forwarding headers without an application-level trusted-proxy policy.
func DefaultKeyGenerator(r *http.Request) string {
	return httpkit.ClientIP(r)
}

// Middleware creates rate limiting middleware. Backend errors fail open to
// avoid turning a limiter outage into a full application outage.
func Middleware(limiter ratelimit.Limiter, cfg ratelimit.Config, keyGen KeyGenerator) func(http.Handler) http.Handler {
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyGen(r)
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}

			result, err := limiter.Allow(r.Context(), key, cfg)
			if err != nil || result == nil {
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set(headerLimit, strconv.Itoa(cfg.Limit))
			w.Header().Set(headerRemaining, strconv.Itoa(result.Remaining))
			w.Header().Set(headerReset, strconv.FormatInt(time.Now().Add(result.ResetAfter).Unix(), 10))
			if !result.Allowed {
				_ = httpresp.Error(w, r, http.StatusTooManyRequests, "Too many requests, please try again later.")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
