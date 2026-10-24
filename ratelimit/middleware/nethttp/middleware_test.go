package nethttplimit

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/ratelimit"
	"github.com/thanhbvha/go-common/utils/ctxkey"
)

type stubLimiter struct {
	result *ratelimit.Result
	err    error
	key    string
	ctx    context.Context
}

func (s *stubLimiter) Allow(ctx context.Context, key string, _ ratelimit.Config) (*ratelimit.Result, error) {
	s.ctx = ctx
	s.key = key
	return s.result, s.err
}

func TestMiddlewareAllowsAndSetsHeaders(t *testing.T) {
	limiter := &stubLimiter{result: &ratelimit.Result{Allowed: true, Remaining: 4, ResetAfter: time.Minute}}
	cfg := ratelimit.Config{Limit: 5, Window: time.Minute}
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	req.RemoteAddr = "192.0.2.10:1234"
	req = req.WithContext(ctxkey.SetRequestID(req.Context(), "req-limit"))
	recorder := httptest.NewRecorder()

	Middleware(limiter, cfg, nil)(next).ServeHTTP(recorder, req)

	if !called || recorder.Code != http.StatusNoContent || limiter.key != "192.0.2.10" {
		t.Fatalf("called = %v, status = %d, key = %q", called, recorder.Code, limiter.key)
	}
	if requestID, _ := ctxkey.GetRequestID(limiter.ctx); requestID != "req-limit" {
		t.Fatalf("limiter request ID = %q", requestID)
	}
	if recorder.Header().Get(headerLimit) != "5" || recorder.Header().Get(headerRemaining) != "4" {
		t.Fatalf("rate headers = %#v", recorder.Header())
	}
	if _, err := strconv.ParseInt(recorder.Header().Get(headerReset), 10, 64); err != nil {
		t.Fatalf("invalid reset header: %v", err)
	}
}

func TestMiddlewareRejectsRequest(t *testing.T) {
	limiter := &stubLimiter{result: &ratelimit.Result{Allowed: false, Remaining: 0, ResetAfter: time.Second}}
	called := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })
	recorder := httptest.NewRecorder()

	Middleware(limiter, ratelimit.Config{Limit: 1, Window: time.Minute}, func(*http.Request) string {
		return "user-1"
	})(next).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/items", nil))

	if called || recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("called = %v, status = %d", called, recorder.Code)
	}
}

func TestMiddlewareFailsOpen(t *testing.T) {
	tests := []struct {
		name    string
		limiter *stubLimiter
		keyGen  KeyGenerator
	}{
		{name: "backend error", limiter: &stubLimiter{err: errors.New("redis unavailable")}},
		{name: "nil result", limiter: &stubLimiter{}},
		{name: "empty key", limiter: &stubLimiter{result: &ratelimit.Result{Allowed: false}}, keyGen: func(*http.Request) string { return "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusNoContent)
			})
			recorder := httptest.NewRecorder()

			Middleware(tt.limiter, ratelimit.Config{Limit: 1, Window: time.Minute}, tt.keyGen)(next).
				ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

			if !called || recorder.Code != http.StatusNoContent {
				t.Fatalf("called = %v, status = %d", called, recorder.Code)
			}
		})
	}
}
