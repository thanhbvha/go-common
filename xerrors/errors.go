// Package xerrors provides a structured error handling mechanism for HTTP applications.
//
// It extends the standard Go error interface by embedding HTTP status codes,
// application-specific string codes, and error chaining (cause) capabilities.
// This allows handlers to return rich errors that the global error handler
// can automatically translate into correct HTTP responses.
//
// Basic usage:
//
//	if err != nil {
//		return xerrors.Wrap(err, "DB_ERROR", "failed to query user", 500)
//	}
//	if user == nil {
//		return xerrors.New("USER_NOT_FOUND", "user does not exist", 404)
//	}
package xerrors

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
)

const maxStackDepth = 32

// Frame represents a single stack frame.
type Frame struct {
	Function string
	File     string
	Line     int
}

// CustomError represents a standard application error with optional stack trace.
type CustomError struct {
	Code       string
	Message    string
	HTTPStatus int
	Cause      error
	Fields     map[string]any // structured context (e.g. {"user_id": "123"})
	stack      []uintptr      // raw program counters (unexported)
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

// StackTrace returns the captured call stack frames, filtering out runtime internals.
func (e *CustomError) StackTrace() []Frame {
	if len(e.stack) == 0 {
		return nil
	}
	frames := runtime.CallersFrames(e.stack)
	var result []Frame
	for {
		f, more := frames.Next()
		// Filter out standard Go runtime and xerrors internals
		if !strings.Contains(f.File, "runtime/") && !strings.HasSuffix(f.File, "/xerrors/errors.go") && !strings.HasSuffix(f.File, "\\xerrors\\errors.go") {
			result = append(result, Frame{Function: f.Function, File: f.File, Line: f.Line})
		}
		if !more {
			break
		}
	}
	return result
}

// captureStack captures the current goroutine's call stack.
func captureStack(skip int) []uintptr {
	pcs := make([]uintptr, maxStackDepth)
	n := runtime.Callers(skip+2, pcs)
	return pcs[:n]
}

// New creates a new CustomError with a captured stack trace.
func New(code, message string, httpStatus int) error {
	return &CustomError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
		stack:      captureStack(1),
	}
}

// NewWithFields creates a new CustomError with structured context fields.
//
// Example:
//
//	return xerrors.NewWithFields("USER_NOT_FOUND", "user not found", 404,
//	    map[string]any{"user_id": id})
func NewWithFields(code, message string, httpStatus int, fields map[string]any) error {
	return &CustomError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
		Fields:     fields,
		stack:      captureStack(1),
	}
}

// Wrap wraps an existing error with a CustomError and a captured stack trace.
func Wrap(err error, code, message string, httpStatus int) error {
	if err == nil {
		return nil
	}
	return &CustomError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
		Cause:      err,
		stack:      captureStack(1),
	}
}

// StackTraceString formats the stack trace as a human-readable multi-line string.
// Returns "" if no stack is captured.
func StackTraceString(err error) string {
	var e *CustomError
	if !errors.As(err, &e) {
		return ""
	}
	frames := e.StackTrace()
	if len(frames) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, f := range frames {
		fmt.Fprintf(&sb, "\n\t%s\n\t\t%s:%d", f.Function, f.File, f.Line)
	}
	return sb.String()
}

// GetFields extracts the structured fields map from an error.
// Returns nil if the error is not a *CustomError.
func GetFields(err error) map[string]any {
	var e *CustomError
	if errors.As(err, &e) {
		return e.Fields
	}
	return nil
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
