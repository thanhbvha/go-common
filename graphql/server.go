// Package graphql provides a bounded, framework-independent gqlgen HTTP server.
package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/thanhbvha/go-common/v2/internal/httpkit"
	"github.com/vektah/gqlparser/v2/ast"
)

// Config controls per-instance limits. Zero numbers use DefaultConfig;
// negative numbers are invalid. Resolvers/backends must honor context deadlines.
type Config struct {
	// RequestSetup runs after admission/body guards with the bounded request
	// context. Use it to create request-scoped loaders after authentication.
	RequestSetup          func(context.Context) (context.Context, error)
	EnableTelemetry       bool
	EnableGET             bool
	EnableIntrospection   bool
	EnableSuggestions     bool
	MaxBodyBytes          int64
	ParserTokenLimit      int
	MaxDepth              int
	MaxFields             int
	ComplexityLimit       int
	RequestTimeout        time.Duration
	MaxConcurrentRequests int
	QueryCacheSize        int
}

func DefaultConfig() Config {
	return Config{MaxBodyBytes: 1 << 20, ParserTokenLimit: 10000, MaxDepth: 15,
		MaxFields: 1000, ComplexityLimit: 1000, RequestTimeout: 10 * time.Second,
		MaxConcurrentRequests: 128, QueryCacheSize: 256}
}

// Server implements http.Handler. Its underlying gqlgen server is private so
// adapters cannot accidentally bypass request guards.
type Server struct {
	handler   *handler.Server
	cfg       Config
	slots     chan struct{}
	telemetry *instruments
}

func NewServer(es graphql.ExecutableSchema, cfg Config) (*Server, error) {
	if es == nil || (reflect.ValueOf(es).Kind() == reflect.Ptr && reflect.ValueOf(es).IsNil()) {
		return nil, errors.New("graphql: executable schema is required")
	}
	defaults := DefaultConfig()
	ints := []struct {
		value    *int
		fallback int
	}{
		{&cfg.ParserTokenLimit, defaults.ParserTokenLimit}, {&cfg.MaxDepth, defaults.MaxDepth},
		{&cfg.MaxFields, defaults.MaxFields}, {&cfg.ComplexityLimit, defaults.ComplexityLimit},
		{&cfg.MaxConcurrentRequests, defaults.MaxConcurrentRequests}, {&cfg.QueryCacheSize, defaults.QueryCacheSize},
	}
	for _, v := range ints {
		if *v.value < 0 {
			return nil, errors.New("graphql: limits must not be negative")
		}
		if *v.value == 0 {
			*v.value = v.fallback
		}
	}
	if cfg.MaxBodyBytes < 0 || cfg.RequestTimeout < 0 {
		return nil, errors.New("graphql: limits must not be negative")
	}
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = defaults.RequestTimeout
	}
	h := handler.New(es)
	methods := []string{"POST", "OPTIONS"}
	if cfg.EnableGET {
		methods = append(methods, "GET")
	}
	h.AddTransport(transport.Options{AllowedMethods: methods})
	if cfg.EnableGET {
		h.AddTransport(transport.GET{})
	}
	h.AddTransport(transport.POST{})
	h.SetParserTokenLimit(cfg.ParserTokenLimit)
	h.SetDisableSuggestion(!cfg.EnableSuggestions)
	h.SetQueryCache(lru.New[*ast.QueryDocument](cfg.QueryCacheSize))
	h.SetErrorPresenter(ErrorPresenter)
	h.SetRecoverFunc(RecoverFunc)
	h.Use(operationLimits{depth: cfg.MaxDepth, fields: cfg.MaxFields})
	h.Use(extension.FixedComplexityLimit(cfg.ComplexityLimit))
	if cfg.EnableIntrospection {
		h.Use(extension.Introspection{})
	}
	h.Use(TelemetryInterceptor{})
	s := &Server{handler: h, cfg: cfg, slots: make(chan struct{}, cfg.MaxConcurrentRequests)}
	if cfg.EnableTelemetry {
		var err error
		s.telemetry, err = newInstruments()
		if err != nil {
			return nil, fmt.Errorf("graphql: telemetry: %w", err)
		}
	}
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	state := &requestState{telemetry: s.telemetry != nil}
	ctx = context.WithValue(ctx, stateKey{}, state)
	if s.telemetry != nil {
		var finish func()
		ctx, finish = s.telemetry.start(ctx, state)
		defer finish()
	}
	observed := httpkit.NewResponseWriter(w)
	w = observed
	defer func() {
		if p := recover(); p != nil {
			_ = RecoverFunc(ctx, p)
			state.errors++
			if !observed.Committed() {
				writeError(w, 500, "INTERNAL_ERROR", "Internal Server Error")
			}
		}
		if observed.StatusCode() >= 400 {
			state.rejected = true
			if state.errors == 0 {
				state.errors = 1
			}
		}
	}()
	r = r.WithContext(ctx)
	reject := func(status int, code, message string) {
		state.rejected = true
		state.errors++
		writeError(w, status, code, message)
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		reject(503, "OVERLOADED", "Server is busy")
		return
	}
	if ctx.Err() != nil {
		reject(408, "REQUEST_TIMEOUT", "Request cancelled or timed out")
		return
	}
	if r.Header.Get("Upgrade") != "" {
		reject(400, "UNSUPPORTED_TRANSPORT", "Transport not supported")
		return
	}
	switch r.Method {
	case http.MethodOptions:
		s.handler.ServeHTTP(w, r)
		return
	case http.MethodGet:
		if !s.cfg.EnableGET {
			w.Header().Set("Allow", "POST, OPTIONS")
			reject(405, "METHOD_NOT_ALLOWED", "Method not allowed")
			return
		}
		if int64(len(r.URL.RawQuery)) > s.cfg.MaxBodyBytes {
			reject(413, "REQUEST_TOO_LARGE", "Request is too large")
			return
		}
		if _, err := url.ParseQuery(r.URL.RawQuery); err != nil {
			reject(400, "BAD_REQUEST", "Invalid query parameters")
			return
		}
	case http.MethodPost:
		if r.Body == nil {
			reject(400, "BAD_REQUEST", "Invalid GraphQL request")
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			reject(415, "UNSUPPORTED_MEDIA_TYPE", "Expected application/json")
			return
		}
		if r.ContentLength > s.cfg.MaxBodyBytes {
			reject(413, "REQUEST_TOO_LARGE", "Request is too large")
			return
		}
		body := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
		defer body.Close()
		data, err := io.ReadAll(body)
		if err != nil {
			var sizeErr *http.MaxBytesError
			if errors.As(err, &sizeErr) {
				reject(413, "REQUEST_TOO_LARGE", "Request is too large")
			} else {
				reject(400, "BAD_REQUEST", "Could not read request body")
			}
			return
		}
		if !json.Valid(data) {
			reject(400, "BAD_REQUEST", "Invalid GraphQL request")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(data))
	default:
		allowed := "POST, OPTIONS"
		if s.cfg.EnableGET {
			allowed += ", GET"
		}
		w.Header().Set("Allow", allowed)
		reject(405, "METHOD_NOT_ALLOWED", "Method not allowed")
		return
	}
	if s.cfg.RequestSetup != nil {
		setupCtx, err := s.cfg.RequestSetup(ctx)
		if err != nil || setupCtx == nil {
			reject(500, "INTERNAL_ERROR", "Internal Server Error")
			return
		}
		r = r.WithContext(setupCtx)
	}
	s.handler.ServeHTTP(w, r)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"errors": []any{map[string]any{"message": message, "extensions": map[string]any{"code": code, "http_status": status}}}})
}
