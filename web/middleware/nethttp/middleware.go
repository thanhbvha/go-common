// Package nethttpmw provides panic recovery and OpenTelemetry middleware for
// the standard library's net/http package.
package nethttpmw

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/thanhbvha/go-common/internal/httpkit"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/telemetry"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/middleware/internal/recoveryhttp"
	"go.opentelemetry.io/otel/attribute"
)

// Recover prevents handler panics from crashing the server, logs a structured
// report, and writes a safe JSON response if the response is not committed.
func Recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := httpkit.NewResponseWriter(w)
		defer panicrecovery.Catch(func(report panicrecovery.Report) {
			requestID, _ := ctxkey.GetRequestID(r.Context())
			recoveryhttp.Log(r.Context(), requestID, report)
			if writer.Committed() {
				return
			}
			payload := recoveryhttp.NewResponse(requestID)
			writer.Header().Set("Content-Type", "application/json; charset=utf-8")
			writer.WriteHeader(recoveryhttp.StatusCode)
			_ = json.NewEncoder(writer).Encode(payload)
		})
		next.ServeHTTP(writer, r)
	})
}

// Telemetry creates an OpenTelemetry server span for each incoming request.
// It extracts any upstream trace context before starting the span.
func Telemetry(operationName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			parentCtx := telemetry.ExtractHTTPHeaders(r.Context(), r.Header)
			ctx, span := telemetry.StartServerSpan(parentCtx, operationName+" "+r.URL.Path)

			attrs := []attribute.KeyValue{
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.client_ip", httpkit.ClientIP(r)),
			}
			if requestID, ok := ctxkey.GetRequestID(ctx); ok {
				attrs = append(attrs, attribute.String("http.request_id", requestID))
			}
			telemetry.SetAttributes(span, attrs...)

			writer := httpkit.NewResponseWriter(w)
			completed := false
			defer func() {
				if !completed {
					telemetry.SetAttributes(span, attribute.Int("http.status_code", http.StatusInternalServerError))
					telemetry.RecordError(span, errors.New("panic in HTTP handler"))
				}
				span.End()
			}()
			next.ServeHTTP(writer, r.WithContext(ctx))
			completed = true
			statusCode := writer.StatusCode()
			telemetry.SetAttributes(span, attribute.Int("http.status_code", statusCode))
			if statusCode >= http.StatusInternalServerError {
				telemetry.RecordError(span, errors.New(http.StatusText(statusCode)))
			}
		})
	}
}
