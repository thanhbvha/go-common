package nethttpmw

import (
	"encoding/json"
	"net/http"

	"github.com/thanhbvha/go-common/internal/httpkit"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

// HandlerFunc is a net/http handler that can return an application error.
type HandlerFunc func(http.ResponseWriter, *http.Request) error

// ErrorHandler converts errors returned by next to the shared, transport-safe
// JSON error response. It does not replace a response that is already committed.
func ErrorHandler(next HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer := httpkit.NewResponseWriter(w)
		err := next(writer, r)
		if err == nil || writer.Committed() {
			return
		}

		requestID, _ := ctxkey.GetRequestID(r.Context())
		payload := response.FromError(err, requestID)
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		writer.WriteHeader(payload.Code)
		_ = json.NewEncoder(writer).Encode(payload)
	})
}
