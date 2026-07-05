package xerrors_test

import (
	goErrors "errors"
	"testing"

	"github.com/thanhbvha/go-common/xerrors"
)

func TestNewError(t *testing.T) {
	err := xerrors.New("TEST_ERR", "This is a test", 400)

	if xerrors.GetCode(err) != "TEST_ERR" {
		t.Errorf("expected code TEST_ERR, got %s", xerrors.GetCode(err))
	}
	if xerrors.HTTPStatusCode(err) != 400 {
		t.Errorf("expected status 400, got %d", xerrors.HTTPStatusCode(err))
	}
	if err.Error() != "[TEST_ERR] This is a test" {
		t.Errorf("unexpected error format: %s", err.Error())
	}
}

func TestWrapError(t *testing.T) {
	baseErr := goErrors.New("database connection lost")
	wrappedErr := xerrors.Wrap(baseErr, "DB_DOWN", "Service unavailable", 503)

	if xerrors.GetCode(wrappedErr) != "DB_DOWN" {
		t.Errorf("expected code DB_DOWN, got %s", xerrors.GetCode(wrappedErr))
	}
	if xerrors.HTTPStatusCode(wrappedErr) != 503 {
		t.Errorf("expected status 503, got %d", xerrors.HTTPStatusCode(wrappedErr))
	}
	if wrappedErr.Error() != "[DB_DOWN] Service unavailable: database connection lost" {
		t.Errorf("unexpected error format: %s", wrappedErr.Error())
	}

	// Test Unwrap (errors.Is)
	if !xerrors.Is(wrappedErr, baseErr) {
		t.Errorf("expected wrappedErr to be baseErr")
	}
}

func TestHTTPStatusCode(t *testing.T) {
	if xerrors.HTTPStatusCode(nil) != 200 {
		t.Errorf("expected 200 for nil error")
	}

	standardErr := goErrors.New("just a string error")
	if xerrors.HTTPStatusCode(standardErr) != 500 {
		t.Errorf("expected 500 for standard error")
	}
}

func TestIsAndAs(t *testing.T) {
	err := xerrors.ErrNotFound

	if !xerrors.Is(err, xerrors.ErrNotFound) {
		t.Errorf("expected Is to match ErrNotFound")
	}

	var customErr *xerrors.CustomError
	if !xerrors.As(err, &customErr) {
		t.Errorf("expected As to extract CustomError")
	}

	if customErr.Code != "NOT_FOUND" {
		t.Errorf("expected code NOT_FOUND, got %s", customErr.Code)
	}
}

func TestWrapNil(t *testing.T) {
	err := xerrors.Wrap(nil, "CODE", "msg", 500)
	if err != nil {
		t.Errorf("expected nil when wrapping nil, got %v", err)
	}
}
