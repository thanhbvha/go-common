// Package nethttp writes standardized API responses with the standard
// library's net/http package.
package nethttp

import (
	"encoding/json"
	"net/http"

	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
)

func requestID(r *http.Request) string {
	requestID, _ := ctxkey.GetRequestID(r.Context())
	return requestID
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	return json.NewEncoder(w).Encode(payload)
}

// Success writes a standard HTTP 200 success response.
func Success(w http.ResponseWriter, r *http.Request, data any) error {
	return writeJSON(w, http.StatusOK, response.NewSuccess(data, requestID(r)))
}

// Error writes a standard error response with the supplied HTTP status.
func Error(w http.ResponseWriter, r *http.Request, statusCode int, message string) error {
	return writeJSON(w, statusCode, response.NewError(statusCode, message, requestID(r)))
}

// ValidationError writes a standard HTTP 400 validation response.
func ValidationError(w http.ResponseWriter, r *http.Request, errors any) error {
	return writeJSON(w, http.StatusBadRequest, response.NewValidationError(errors, requestID(r)))
}

// Created writes a standard HTTP 201 resource-created response.
func Created(w http.ResponseWriter, r *http.Request, data any) error {
	return writeJSON(w, http.StatusCreated, response.NewCreated(data, requestID(r)))
}

// Paginated writes a standard HTTP 200 paginated response.
func Paginated[T any](w http.ResponseWriter, r *http.Request, items []T, totalRows int64, totalPages, page, size int) error {
	return writeJSON(w, http.StatusOK, response.NewPaginated(items, totalRows, totalPages, page, size, requestID(r)))
}
