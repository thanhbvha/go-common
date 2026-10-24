package httpkit

import (
	"strings"
	"testing"
)

func TestRequestID(t *testing.T) {
	if got := RequestID("client.id_1:trace-2"); got != "client.id_1:trace-2" {
		t.Fatalf("valid request ID changed to %q", got)
	}
	for _, invalid := range []string{"", "contains space", "line\nbreak", strings.Repeat("a", 129)} {
		if got := RequestID(invalid); got == "" || got == invalid {
			t.Fatalf("invalid request ID %q was not replaced: %q", invalid, got)
		}
	}
}
