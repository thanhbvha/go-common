package response

import (
	"errors"
	"net/http"
	"testing"

	"github.com/thanhbvha/go-common/xerrors"
)

func TestFromErrorMasksInternalError(t *testing.T) {
	got := FromError(errors.New("database password leaked"), "request-123")
	if got.Code != http.StatusInternalServerError || got.Status != "INTERNAL_ERROR" {
		t.Fatalf("unexpected error metadata: %+v", got)
	}
	if got.Message != internalErrorMessage || got.RequestID != "request-123" {
		t.Fatalf("unsafe or incomplete response: %+v", got)
	}
}

func TestFromErrorPreservesPublicCustomError(t *testing.T) {
	err := xerrors.New("NOT_FOUND", "user does not exist", http.StatusNotFound)
	got := FromError(err, "request-123")
	if got.Code != http.StatusNotFound || got.Status != "NOT_FOUND" || got.Message != err.Error() {
		t.Fatalf("unexpected response: %+v", got)
	}
}

func TestResponseConstructors(t *testing.T) {
	if got := NewSuccess("data", "request-123"); got.Code != 0 || got.Data != "data" || got.RequestID != "request-123" {
		t.Fatalf("NewSuccess() = %+v", got)
	}
	if got := NewValidationError([]string{"invalid"}, "request-123"); got.Code != http.StatusBadRequest || got.Errors == nil {
		t.Fatalf("NewValidationError() = %+v", got)
	}
	if got := NewCreated("data", "request-123"); got.Message != "Created" || got.Data != "data" {
		t.Fatalf("NewCreated() = %+v", got)
	}
	if got := NewPaginated([]int{1}, 1, 1, 1, 10, "request-123"); len(got.Data) != 1 || got.TotalRows != 1 || got.RequestID != "request-123" {
		t.Fatalf("NewPaginated() = %+v", got)
	}
}

func TestNewErrorMasksServerError(t *testing.T) {
	got := NewError(http.StatusInternalServerError, "database password leaked", "request-123")
	if got.Message != internalErrorMessage {
		t.Fatalf("NewError() exposed internal message: %+v", got)
	}
}
