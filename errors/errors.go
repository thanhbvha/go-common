package errors

import (
	"errors"
	"fmt"
)

// CustomError represents a standard application error.
type CustomError struct {
	Code       string
	Message    string
	HTTPStatus int
	Cause      error
}

// Error implements the standard error interface.
func (e *CustomError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s] %s: %v", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// Unwrap implements the interface for errors.Is and errors.As.
func (e *CustomError) Unwrap() error {
	return e.Cause
}

// New creates a new CustomError.
func New(code, message string, httpStatus int) error {
	return &CustomError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
	}
}

// Wrap wraps an existing error with a CustomError.
func Wrap(err error, code, message string, httpStatus int) error {
	if err == nil {
		return nil
	}
	return &CustomError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
		Cause:      err,
	}
}

// HTTPStatusCode extracts the HTTP status code from an error.
// If it's a CustomError, it returns its status. Otherwise, 500.
func HTTPStatusCode(err error) int {
	if err == nil {
		return 200 // OK
	}
	var customErr *CustomError
	if errors.As(err, &customErr) {
		return customErr.HTTPStatus
	}
	return 500
}

// GetCode extracts the string code from an error.
func GetCode(err error) string {
	if err == nil {
		return ""
	}
	var customErr *CustomError
	if errors.As(err, &customErr) {
		return customErr.Code
	}
	return "INTERNAL_ERROR"
}

// Is is a convenience wrapper around standard errors.Is
func Is(err, target error) bool {
	return errors.Is(err, target)
}

// As is a convenience wrapper around standard errors.As
func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

// Join is a convenience wrapper around standard errors.Join (Go 1.20+)
func Join(errs ...error) error {
	return errors.Join(errs...)
}
