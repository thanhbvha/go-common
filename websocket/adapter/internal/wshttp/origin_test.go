package wshttp

import "testing"

func TestCheckSameOrigin(t *testing.T) {
	tests := []struct {
		name   string
		origin string
		host   string
		want   bool
	}{
		{name: "missing origin", host: "api.example.com", want: true},
		{name: "same origin", origin: "https://api.example.com", host: "api.example.com", want: true},
		{name: "same origin case insensitive", origin: "https://API.EXAMPLE.COM", host: "api.example.com", want: true},
		{name: "same origin with port", origin: "https://api.example.com:8443", host: "api.example.com:8443", want: true},
		{name: "different host", origin: "https://evil.example.com", host: "api.example.com", want: false},
		{name: "different port", origin: "https://api.example.com:8443", host: "api.example.com", want: false},
		{name: "malformed", origin: "://bad", host: "api.example.com", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckSameOrigin(tt.origin, tt.host); got != tt.want {
				t.Fatalf("CheckSameOrigin(%q, %q) = %v, want %v", tt.origin, tt.host, got, tt.want)
			}
		})
	}
}
