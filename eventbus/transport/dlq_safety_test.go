package transport

import (
	"errors"
	"strings"
	"testing"
)

type hostileDLQError struct{}

func (hostileDLQError) Error() string { panic("must not render") }

func TestNormalizeDecisionMetadata(t *testing.T) {
	reason, code := NormalizeDecisionMetadata(Decision{
		Reason: strings.Repeat("x", MaxDLQReasonBytes+1), ErrorCode: "contains secret", Cause: hostileDLQError{},
	})
	if reason != "delivery_failure" || code != "delivery_error" {
		t.Fatalf("metadata = %q/%q", reason, code)
	}
	reason, code = NormalizeDecisionMetadata(Decision{Reason: "permanent_error", ErrorCode: "handler_error", Cause: errors.New("secret")})
	if reason != "permanent_error" || code != "handler_error" {
		t.Fatalf("metadata = %q/%q", reason, code)
	}
}
