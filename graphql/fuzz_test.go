package graphql

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func FuzzGraphQLInput(f *testing.F) {
	f.Add(`{"query":"{ value }"}`)
	f.Add(`{"query":"{node{child{value}}}"}`)
	f.Add(`{`)
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > 64*1024 {
			t.Skip()
		}
		server := newTestServer(t, Config{MaxBodyBytes: 64 * 1024, ParserTokenLimit: 200, MaxDepth: 8, MaxFields: 100}, nil)
		req := httptest.NewRequest("POST", "/query", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		res := httptest.NewRecorder()
		server.ServeHTTP(res, req)
		if res.Code < 200 || res.Code > 599 {
			t.Fatalf("invalid status %d", res.Code)
		}
	})
}
