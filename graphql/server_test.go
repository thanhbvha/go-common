package graphql

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/thanhbvha/go-common/v2/xerrors"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type testSchema struct {
	run func(context.Context) *gql.Response
}

func (*testSchema) Schema() *ast.Schema {
	return gqlparser.MustLoadSchema(&ast.Source{Input: `
 type Query { value: String! node: Node! echo(input: Int!): String! }
 type Node { value: String! child: Node! }
 type Mutation { set: String! }
 type Subscription { ticks: String! }
`})
}
func (*testSchema) Complexity(context.Context, string, string, int, map[string]any) (int, bool) {
	return 0, false
}
func (s *testSchema) Exec(ctx context.Context) gql.ResponseHandler {
	return func(ctx context.Context) *gql.Response {
		if s.run != nil {
			return s.run(ctx)
		}
		return &gql.Response{Data: json.RawMessage(`{"value":"ok"}`)}
	}
}
func newTestServer(t *testing.T, cfg Config, run func(context.Context) *gql.Response) *Server {
	t.Helper()
	s, err := NewServer(&testSchema{run: run}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func request(s http.Handler, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/query", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func query(s http.Handler, q string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(map[string]any{"query": q})
	return request(s, string(b))
}
func errorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var v struct {
		Errors []struct{ Extensions struct{ Code string } }
	}
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("invalid response %d: %s", w.Code, w.Body.String())
	}
	if len(v.Errors) == 0 {
		return ""
	}
	return v.Errors[0].Extensions.Code
}
func TestServerConfig(t *testing.T) {
	if _, err := NewServer(nil, Config{}); err == nil {
		t.Fatal("nil schema accepted")
	}
	var nilSchema *testSchema
	if _, err := NewServer(nilSchema, Config{}); err == nil {
		t.Fatal("typed nil accepted")
	}
	for _, cfg := range []Config{{MaxBodyBytes: -1}, {MaxDepth: -1}, {MaxFields: -1}, {ParserTokenLimit: -1}, {RequestTimeout: -1}, {MaxConcurrentRequests: -1}, {QueryCacheSize: -1}, {ComplexityLimit: -1}} {
		if _, err := NewServer(&testSchema{}, cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
	s := newTestServer(t, Config{}, nil)
	if s.cfg.MaxBodyBytes != 1<<20 || s.cfg.MaxConcurrentRequests != 128 {
		t.Fatal(s.cfg)
	}
}
func TestServerGuards(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		body string
		code string
	}{
		{"malformed", Config{EnableTelemetry: true}, `{"query":"secret"`, "BAD_REQUEST"},
		{"invalid_envelope", Config{EnableTelemetry: true}, `{"query":123,"variables":{"password":"secret"}}`, "BAD_REQUEST"},
		{"parse", Config{}, `{"query":"{"}`, "GRAPHQL_PARSE_FAILED"},
		{"validation", Config{}, `{"query":"{ secretUnknown }"}`, "GRAPHQL_VALIDATION_FAILED"},
		{"variables", Config{}, `{"query":"query($i:Int!){echo(input:$i)}","variables":{"i":"secret"}}`, "GRAPHQL_VALIDATION_FAILED"},
		{"depth", Config{MaxDepth: 2}, `{"query":"{node{child{value}}}"}`, "QUERY_LIMIT_EXCEEDED"},
		{"aliases", Config{MaxFields: 2}, `{"query":"{a:value b:value c:value}"}`, "QUERY_LIMIT_EXCEEDED"},
		{"fragments", Config{MaxFields: 3}, `{"query":"{node{...F ...F}} fragment F on Node {value child{value}}"}`, "QUERY_LIMIT_EXCEEDED"},
		{"complexity", Config{ComplexityLimit: 1}, `{"query":"{node{value}}"}`, "COMPLEXITY_LIMIT_EXCEEDED"},
		{"tokens", Config{ParserTokenLimit: 2}, `{"query":"{a:value b:value}"}`, "GRAPHQL_PARSE_FAILED"},
		{"subscription", Config{}, `{"query":"subscription {ticks}"}`, "UNSUPPORTED_OPERATION"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestServer(t, tc.cfg, nil)
			// Repeat to exercise the query cache without bypassing per-request guards.
			for i := 0; i < 2; i++ {
				w := request(s, tc.body)
				if got := errorCode(t, w); got != tc.code {
					t.Fatalf("got %s (%d): %s", got, w.Code, w.Body.String())
				}
				if strings.Contains(w.Body.String(), "secret") {
					t.Fatal("input leaked")
				}
			}
		})
	}
}
func TestBodyAndTransportLimits(t *testing.T) {
	s := newTestServer(t, Config{MaxBodyBytes: 32}, nil)
	for _, length := range []int64{33, -1} {
		r := httptest.NewRequest("POST", "/query", strings.NewReader(strings.Repeat("x", 33)))
		r.Header.Set("Content-Type", "application/json")
		r.ContentLength = length
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != 413 {
			t.Fatal(w.Code, w.Body)
		}
	}
	for _, tc := range []struct {
		method, media, upgrade string
		status                 int
	}{
		{"GET", "", "", 405}, {"POST", "multipart/form-data", "", 415}, {"POST", "text/event-stream", "", 415}, {"GET", "", "websocket", 400}, {"PUT", "", "", 405},
	} {
		r := httptest.NewRequest(tc.method, "/query", nil)
		r.Header.Set("Content-Type", tc.media)
		r.Header.Set("Upgrade", tc.upgrade)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%+v got %d", tc, w.Code)
		}
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("OPTIONS", "/query", nil))
	if strings.Contains(w.Header().Get("Allow"), "GET") {
		t.Fatal(w.Header())
	}
	get := newTestServer(t, Config{EnableGET: true}, nil)
	for _, tc := range []struct {
		url    string
		status int
	}{
		{"/query?query=%7Bvalue%7D", 200}, {"/query?query=mutation%7Bset%7D", 406}, {"/query?query=%zz", 400},
	} {
		r := httptest.NewRequest("GET", "/query", nil)
		r.URL.RawQuery = strings.TrimPrefix(tc.url, "/query?")
		w := httptest.NewRecorder()
		get.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s got %d: %s", tc.url, w.Code, w.Body)
		}
	}
}
func TestOverloadAndDeadline(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s := newTestServer(t, Config{MaxConcurrentRequests: 1}, func(ctx context.Context) *gql.Response {
		close(entered)
		<-release
		return &gql.Response{Data: json.RawMessage(`{"value":"ok"}`)}
	})
	done := make(chan struct{})
	go func() { defer close(done); query(s, "{value}") }()
	<-entered
	w := query(s, "{value}")
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	close(release)
	<-done
	s = newTestServer(t, Config{RequestTimeout: time.Millisecond}, func(ctx context.Context) *gql.Response {
		<-ctx.Done()
		gql.AddError(ctx, ctx.Err())
		return &gql.Response{Data: json.RawMessage("null")}
	})
	w = query(s, "{value}")
	if errorCode(t, w) != "REQUEST_TIMEOUT" {
		t.Fatal(w.Body)
	}
}
func TestResolverErrorSafetyAndPartialData(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
	}{
		{errors.New("secret"), "INTERNAL_ERROR"},
		{xerrors.New("DB", "secret", 500), "INTERNAL_ERROR"},
		{xerrors.New("DENIED", "Access denied", 403), "DENIED"},
		{&gqlerror.Error{Message: "secret", Extensions: map[string]any{"code": "GRAPHQL_PARSE_FAILED", "token": "secret"}}, "INTERNAL_ERROR"},
	} {
		s := newTestServer(t, Config{}, func(ctx context.Context) *gql.Response {
			gql.AddError(ctx, tc.err)
			return &gql.Response{Data: json.RawMessage(`{"value":"partial"}`)}
		})
		w := query(s, "{value}")
		if errorCode(t, w) != tc.code || strings.Contains(w.Body.String(), "secret") || !strings.Contains(w.Body.String(), "partial") {
			t.Fatal(w.Body)
		}
	}
	s := newTestServer(t, Config{}, func(context.Context) *gql.Response { panic("secret") })
	if w := query(s, "{value}"); strings.Contains(w.Body.String(), "secret") || errorCode(t, w) != "INTERNAL_ERROR" {
		t.Fatal(w.Body)
	}
}
func TestRequestSetupAndTelemetry(t *testing.T) {
	previous := otel.GetTracerProvider()
	defer otel.SetTracerProvider(previous)
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	otel.SetTracerProvider(provider)
	key := struct{}{}
	var deadline bool
	s := newTestServer(t, Config{EnableTelemetry: true, RequestSetup: func(ctx context.Context) (context.Context, error) {
		_, deadline = ctx.Deadline()
		return context.WithValue(ctx, key, "identity"), nil
	}}, func(ctx context.Context) *gql.Response {
		if ctx.Value(key) != "identity" {
			t.Error("lost setup context")
		}
		return &gql.Response{Data: json.RawMessage(`{"value":"ok"}`)}
	})
	query(s, "query secretName {value}")
	request(s, `{"variables":"secret"`)
	panicServer := newTestServer(t, Config{EnableTelemetry: true}, func(context.Context) *gql.Response { panic("secret") })
	query(panicServer, "{value}")
	timeoutServer := newTestServer(t, Config{EnableTelemetry: true, RequestTimeout: time.Millisecond}, func(ctx context.Context) *gql.Response {
		<-ctx.Done()
		return nil
	})
	query(timeoutServer, "{value}")
	spans := recorder.Ended()
	if !deadline || len(spans) != 4 {
		t.Fatalf("deadline=%v spans=%d", deadline, len(spans))
	}
	for _, span := range spans {
		if span.Name() != "GraphQL request" {
			t.Fatal(span.Name())
		}
		for _, a := range span.Attributes() {
			if strings.Contains(a.Value.AsString(), "secret") {
				t.Fatal(a)
			}
		}
		if len(span.Events()) != 0 {
			t.Fatal("raw error events exported")
		}
	}
}
