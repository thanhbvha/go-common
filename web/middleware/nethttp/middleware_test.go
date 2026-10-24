package nethttpmw

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/web/response"
	"github.com/thanhbvha/go-common/xerrors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func requestWithID(requestID string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/items", nil)
	return req.WithContext(ctxkey.SetRequestID(req.Context(), requestID))
}

func TestRecoverReturnsSafeResponse(t *testing.T) {
	const secret = "database password is secret"
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(secret) })
	recorder := httptest.NewRecorder()

	Recover(next).ServeHTTP(recorder, requestWithID("req-recover"))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	var body response.ErrorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "INTERNAL_ERROR" || body.RequestID != "req-recover" {
		t.Fatalf("response = %#v", body)
	}
	if strings.Contains(body.Message, secret) {
		t.Fatalf("response exposed panic value: %q", body.Message)
	}
}

func TestRecoverDoesNotReplaceCommittedResponse(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("partial"))
		panic("after write")
	})
	recorder := httptest.NewRecorder()

	Recover(next).ServeHTTP(recorder, requestWithID("req-committed"))

	if recorder.Code != http.StatusAccepted || recorder.Body.String() != "partial" {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
}

func TestErrorHandler(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "application error", err: xerrors.New("INVALID", "invalid item", http.StatusUnprocessableEntity), wantStatus: http.StatusUnprocessableEntity, wantCode: "INVALID"},
		{name: "unknown error", err: errors.New("database password is secret"), wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler := ErrorHandler(func(http.ResponseWriter, *http.Request) error { return tt.err })
			handler.ServeHTTP(recorder, requestWithID("req-error"))

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			var body response.ErrorResponse
			if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Status != tt.wantCode || body.RequestID != "req-error" {
				t.Fatalf("response = %#v", body)
			}
			if tt.wantStatus == http.StatusInternalServerError && strings.Contains(body.Message, "password") {
				t.Fatalf("response exposed internal error: %q", body.Message)
			}
		})
	}
}

func TestErrorHandlerDoesNotReplaceCommittedResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	handler := ErrorHandler(func(w http.ResponseWriter, _ *http.Request) error {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("accepted"))
		return errors.New("late error")
	})

	handler.ServeHTTP(recorder, requestWithID("req-committed"))

	if recorder.Code != http.StatusAccepted || recorder.Body.String() != "accepted" {
		t.Fatalf("status = %d, body = %q", recorder.Code, recorder.Body.String())
	}
}

func TestTelemetryCreatesServerSpan(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(t.Context())
	})

	var downstream trace.SpanContext
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		downstream = trace.SpanContextFromContext(r.Context())
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	req := requestWithID("req-span")
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	recorder := httptest.NewRecorder()

	Telemetry("HTTP Request")(next).ServeHTTP(recorder, req)

	spans := spanRecorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d", len(spans))
	}
	span := spans[0]
	if span.SpanKind() != trace.SpanKindServer || !downstream.IsValid() {
		t.Fatalf("span kind = %v, downstream valid = %v", span.SpanKind(), downstream.IsValid())
	}
	if got := span.Parent().SpanID().String(); got != "00f067aa0ba902b7" {
		t.Fatalf("parent span ID = %s", got)
	}
	if span.Status().Code != codes.Error {
		t.Fatalf("span status = %v", span.Status().Code)
	}
	attrs := make(map[string]any)
	for _, attr := range span.Attributes() {
		attrs[string(attr.Key)] = attr.Value.AsInterface()
	}
	if attrs["http.status_code"] != int64(http.StatusServiceUnavailable) || attrs["http.request_id"] != "req-span" {
		t.Fatalf("span attributes = %#v", attrs)
	}
}

func TestTelemetryRecordsPanicBeforeRecovery(t *testing.T) {
	spanRecorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	previousProvider := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		_ = provider.Shutdown(t.Context())
	})

	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("secret") })
	recorder := httptest.NewRecorder()
	Recover(Telemetry("HTTP Request")(next)).ServeHTTP(recorder, requestWithID("req-panic"))

	spans := spanRecorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Fatalf("span status = %v", spans[0].Status().Code)
	}
	for _, attr := range spans[0].Attributes() {
		if attr.Key == "http.status_code" && attr.Value.AsInt64() == http.StatusInternalServerError {
			return
		}
	}
	t.Fatal("panic span did not record HTTP 500")
}
