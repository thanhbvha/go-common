// Package nethttp provides health check handlers for the standard library's
// net/http package.
package nethttp

import (
	"encoding/json"
	"net/http"

	"github.com/thanhbvha/go-common/health"
)

func statusCode(result health.HealthResponse) int {
	if result.Status == health.StatusOK {
		return http.StatusOK
	}
	return http.StatusServiceUnavailable
}

func writeJSON(w http.ResponseWriter, result health.HealthResponse) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode(result))
	_ = json.NewEncoder(w).Encode(result)
}

// Handler handles the overall health endpoint and runs all checks.
func Handler(checker *health.Checker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, checker.CheckHealth(r.Context()))
	}
}

// ReadyHandler handles the readiness endpoint.
func ReadyHandler(checker *health.Checker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, checker.CheckReadiness(r.Context()))
	}
}

// LiveHandler handles the liveness endpoint.
func LiveHandler(checker *health.Checker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, checker.CheckLiveness(r.Context()))
	}
}
