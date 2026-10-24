// Package nethttplog provides request ID and access logging middleware for
// the standard library's net/http package.
package nethttplog

import (
	"net/http"
	"time"

	"github.com/thanhbvha/go-common/internal/httpkit"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

const requestIDHeader = "X-Request-ID"

// Middleware logs one structured entry after each HTTP request.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/favicon.ico" || path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		writer := httpkit.NewResponseWriter(w)
		next.ServeHTTP(writer, r)

		requestID, _ := ctxkey.GetRequestID(r.Context())
		if requestID == "" {
			requestID = r.Header.Get(requestIDHeader)
		}
		entry := logger.LogEntry{
			Time:      time.Now().Format(time.RFC3339),
			RequestID: requestID,
			Method:    r.Method,
			Path:      path,
			Status:    writer.StatusCode(),
			Latency:   time.Since(start).String(),
			IP:        httpkit.ClientIP(r),
			ServerIP:  logger.LocalServerIP,
			UserAgent: r.UserAgent(),
			Request:   "",
			Response:  "",
		}

		if writer.StatusCode() >= http.StatusInternalServerError {
			entry.Error = http.StatusText(writer.StatusCode())
			logger.ErrorAsync("HTTP Request Failed", "entry", entry)
			return
		}
		logger.InfoAsync("HTTP Request OK", "entry", entry)
	})
}

// RequestIDMiddleware accepts an incoming request ID or generates one, then
// stores it in the request context and response header.
func RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := httpkit.RequestID(r.Header.Get(requestIDHeader))
		ctx := ctxkey.SetRequestID(r.Context(), requestID)
		w.Header().Set(requestIDHeader, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
