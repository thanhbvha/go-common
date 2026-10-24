package authhttp

import "testing"

func TestBearerToken(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{name: "standard", header: "Bearer token", want: "token"},
		{name: "case insensitive", header: "bEaReR token", want: "token"},
		{name: "surrounding whitespace", header: "  Bearer   token  ", want: "token"},
		{name: "missing", header: "", want: ""},
		{name: "wrong scheme", header: "Basic token", want: ""},
		{name: "missing token", header: "Bearer", want: ""},
		{name: "extra parts", header: "Bearer token extra", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BearerToken(tt.header); got != tt.want {
				t.Fatalf("BearerToken(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}
