// Package response provides a standardized JSON response structure for API endpoints.
//
// It ensures that all HTTP responses follow a consistent format across the application,
// making it easier for client-side applications to parse successful data and errors.
//
// Basic usage:
//
//	func GetUser(c *fiber.Ctx) error {
//		// ...
//		return response.Success(c, user)
//	}
package response

import (
	"net/http"

	"github.com/thanhbvha/go-common/xerrors"
)

const internalErrorMessage = "An unexpected internal error occurred"

// Response represents the standard API response structure
type Response struct {
	Code      int         `json:"code"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	Errors    interface{} `json:"errors,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
}

// PaginatedResponse is the standard structure for paginated list API responses.
type PaginatedResponse[T any] struct {
	Code       int    `json:"code"`
	Message    string `json:"message"`
	Data       []T    `json:"data"`
	TotalRows  int64  `json:"total_rows"`
	TotalPages int    `json:"total_pages"`
	Page       int    `json:"page"`
	Size       int    `json:"size"`
	RequestID  string `json:"request_id,omitempty"`
}

// ErrorResponse is the standard transport-safe representation of an
// application or HTTP error.
type ErrorResponse struct {
	Code      int    `json:"code"`
	Status    string `json:"status"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// NewSuccess creates a standard successful response payload.
func NewSuccess(data any, requestID string) Response {
	return Response{Code: 0, Message: "Success", Data: data, RequestID: requestID}
}

// NewError creates a standard error payload used by response adapters.
func NewError(statusCode int, message, requestID string) Response {
	if statusCode >= http.StatusInternalServerError {
		message = internalErrorMessage
	}
	return Response{Code: statusCode, Message: message, RequestID: requestID}
}

// NewValidationError creates a standard validation error payload.
func NewValidationError(errors any, requestID string) Response {
	return Response{
		Code:      http.StatusBadRequest,
		Message:   "Validation failed",
		Errors:    errors,
		RequestID: requestID,
	}
}

// NewCreated creates a standard resource-created response payload.
func NewCreated(data any, requestID string) Response {
	return Response{Code: 0, Message: "Created", Data: data, RequestID: requestID}
}

// NewPaginated creates a standard paginated response payload.
func NewPaginated[T any](items []T, totalRows int64, totalPages, page, size int, requestID string) PaginatedResponse[T] {
	return PaginatedResponse[T]{
		Code:       0,
		Message:    "Success",
		Data:       items,
		TotalRows:  totalRows,
		TotalPages: totalPages,
		Page:       page,
		Size:       size,
		RequestID:  requestID,
	}
}

// NewHTTPError creates a transport-safe HTTP error payload.
func NewHTTPError(statusCode int, status, message, requestID string) ErrorResponse {
	if statusCode == http.StatusInternalServerError {
		message = internalErrorMessage
	}
	return ErrorResponse{
		Code:      statusCode,
		Status:    status,
		Message:   message,
		RequestID: requestID,
	}
}

// FromError converts an xerrors-compatible error into a transport-safe payload.
// Unknown/internal errors never expose their raw message.
func FromError(err error, requestID string) ErrorResponse {
	if err == nil {
		return NewHTTPError(http.StatusOK, "", "", requestID)
	}
	return NewHTTPError(
		xerrors.HTTPStatusCode(err),
		xerrors.GetCode(err),
		err.Error(),
		requestID,
	)
}
