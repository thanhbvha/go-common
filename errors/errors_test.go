package errors_test

import (
	goErrors "errors"
	"testing"

	"github.com/thanhbvha/go-common/errors"
)

func TestNewError(t *testing.T) {
	err := errors.New("TEST_ERR", "This is a test", 400)

	if errors.GetCode(err) != "TEST_ERR" {
		t.Errorf("expected code TEST_ERR, got %s", errors.GetCode(err))
	}
	if errors.HTTPStatusCode(err) != 400 {
		t.Errorf("expected status 400, got %d", errors.HTTPStatusCode(err))
	}
	if err.Error() != "[TEST_ERR] This is a test" {
		t.Errorf("unexpected error format: %s", err.Error())
	}
}

func TestWrapError(t *testing.T) {
	baseErr := goErrors.New("database connection lost")
	wrappedErr := errors.Wrap(baseErr, "DB_DOWN", "Service unavailable", 503)

	if errors.GetCode(wrappedErr) != "DB_DOWN" {
		t.Errorf("expected code DB_DOWN, got %s", errors.GetCode(wrappedErr))
	}
	if errors.HTTPStatusCode(wrappedErr) != 503 {
		t.Errorf("expected status 503, got %d", errors.HTTPStatusCode(wrappedErr))
	}
	if wrappedErr.Error() != "[DB_DOWN] Service unavailable: database connection lost" {
		t.Errorf("unexpected error format: %s", wrappedErr.Error())
	}

	// Test Unwrap (errors.Is)
	if !errors.Is(wrappedErr, baseErr) {
		t.Errorf("expected wrappedErr to be baseErr")
	}
}

func TestHTTPStatusCode(t *testing.T) {
	if errors.HTTPStatusCode(nil) != 200 {
		t.Errorf("expected 200 for nil error")
	}

	standardErr := goErrors.New("just a string error")
	if errors.HTTPStatusCode(standardErr) != 500 {
		t.Errorf("expected 500 for standard error")
	}
}

func TestIsAndAs(t *testing.T) {
	err := errors.ErrNotFound

	if !errors.Is(err, errors.ErrNotFound) {
		t.Errorf("expected Is to match ErrNotFound")
	}

	var customErr *errors.CustomError
	if !errors.As(err, &customErr) {
		t.Errorf("expected As to extract CustomError")
	}

	if customErr.Code != "NOT_FOUND" {
		t.Errorf("expected code NOT_FOUND, got %s", customErr.Code)
	}
}

func TestWrapNil(t *testing.T) {
	err := errors.Wrap(nil, "CODE", "msg", 500)
	if err != nil {
		t.Errorf("expected nil when wrapping nil, got %v", err)
	}
}
