// Package nethttplimit provides checked HTTP rate-limit middleware.
package nethttplimit

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/thanhbvha/go-common/v2/internal/httpkit"
	"github.com/thanhbvha/go-common/v2/ratelimit"
	httpresp "github.com/thanhbvha/go-common/v2/web/response/nethttp"
)

const (
	headerLimit     = "X-RateLimit-Limit"
	headerRemaining = "X-RateLimit-Remaining"
	headerReset     = "X-RateLimit-Reset"
)

type KeyGenerator func(*http.Request) string

// DefaultKeyGenerator ignores untrusted forwarding headers.
func DefaultKeyGenerator(r *http.Request) string { return httpkit.ClientIP(r) }

func Middleware(limiter ratelimit.Limiter, cfg ratelimit.MiddlewareConfig, keyGen KeyGenerator) (func(http.Handler) http.Handler, error) {
	if err := ratelimit.ValidateMiddleware(limiter, cfg); err != nil {
		return nil, err
	}
	if keyGen == nil {
		keyGen = DefaultKeyGenerator
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyGen(r)
			var result *ratelimit.Result
			var err error
			if key == "" {
				err = errors.New("ratelimit: empty request key")
			} else {
				result, err = limiter.Allow(r.Context(), key, cfg.Policy)
			}
			if err != nil || result == nil {
				if err == nil {
					err = errors.New("ratelimit: backend returned no result")
				}
				if cfg.BackendFailure(r.Context(), err) {
					next.ServeHTTP(w, r)
				} else {
					_ = httpresp.Error(w, r, http.StatusServiceUnavailable, "Rate limit service unavailable")
				}
				return
			}
			w.Header().Set(headerLimit, strconv.Itoa(cfg.Policy.Limit))
			w.Header().Set(headerRemaining, strconv.Itoa(max(0, result.Remaining)))
			w.Header().Set(headerReset, strconv.FormatInt(time.Now().Add(result.ResetAfter).Unix(), 10))
			if !result.Allowed {
				w.Header().Set("Retry-After", strconv.Itoa(ratelimit.RetryAfterSeconds(result.ResetAfter)))
				_ = httpresp.Error(w, r, http.StatusTooManyRequests, "Too many requests")
				return
			}
			next.ServeHTTP(w, r)
		})
	}, nil
}
