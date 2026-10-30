package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatsAuthentication(t *testing.T) {
	for _, tc := range []struct {
		token, authorization, method string
		status                       int
	}{
		{"", "Bearer test", "GET", 401},
		{"test", "", "GET", 401},
		{"test", "Bearer wrong", "GET", 401},
		{"test", "Bearer test", "POST", 405},
		{"test", "Bearer test", "GET", 200},
	} {
		called := false
		h := statsHandler(tc.token, func() any { called = true; return map[string]int{"sessions": 1} })
		r := httptest.NewRequest(tc.method, "/admin/stats", nil)
		r.Header.Set("Authorization", tc.authorization)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status || called != (tc.status == http.StatusOK) || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("status=%d snapshot=%v", w.Code, called)
		}
	}
}
