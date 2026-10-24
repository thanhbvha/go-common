package httpkit

import (
	"net/http/httptest"
	"testing"
)

func TestClientIP(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		want       string
	}{
		{name: "IPv4", remoteAddr: "192.0.2.1:1234", want: "192.0.2.1"},
		{name: "IPv6", remoteAddr: "[2001:db8::1]:1234", want: "2001:db8::1"},
		{name: "without port", remoteAddr: "192.0.2.2", want: "192.0.2.2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remoteAddr
			if got := ClientIP(req); got != tt.want {
				t.Fatalf("ClientIP() = %q, want %q", got, tt.want)
			}
		})
	}
}
