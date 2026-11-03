package httpclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type observedTransport struct {
	base       http.RoundTripper
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

func newObservedTransport(base http.RoundTripper, cfg Config) *observedTransport {
	t := &observedTransport{base: base}
	if cfg.EnableTelemetry {
		provider := cfg.TracerProvider
		if provider == nil {
			provider = otel.GetTracerProvider()
		}
		t.tracer = provider.Tracer("go-common/httpclient")
		t.propagator = cfg.Propagator
		if t.propagator == nil {
			t.propagator = otel.GetTextMapPropagator()
		}
	}
	return t
}

func (t *observedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	var span trace.Span
	if t.tracer != nil {
		ctx, span = t.tracer.Start(ctx, "HTTP "+req.Method, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("http.request.method", req.Method), attribute.String("server.address", req.URL.Hostname())))
		req = req.Clone(ctx)
		t.propagator.Inject(ctx, propagation.HeaderCarrier(req.Header))
	}
	resp, err := t.base.RoundTrip(req)
	if resp == nil && err == nil {
		err = errors.New("httpclient: transport returned no response")
	}
	if err != nil {
		if span != nil {
			span.SetStatus(codes.Error, "HTTP transport failed")
			span.End()
		}
		return resp, err
	}
	if span != nil {
		span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))
		if resp.StatusCode >= 500 {
			span.SetStatus(codes.Error, "HTTP server error")
		}
	}
	body := &observedBody{ReadCloser: resp.Body}
	var once sync.Once
	end := func(err error) {
		once.Do(func() {
			if span != nil {
				if err != nil && !errors.Is(err, io.EOF) {
					span.SetStatus(codes.Error, "HTTP response interrupted")
				}
				span.End()
			}
		})
	}
	stop := context.AfterFunc(ctx, func() { end(ctx.Err()) })
	body.onEnd = func(err error) { stop(); end(err) }
	resp.Body = body
	if body.ReadCloser == nil || body.ReadCloser == http.NoBody {
		body.finish(nil)
	}
	return resp, nil
}

func (t *observedTransport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

type observedBody struct {
	io.ReadCloser
	mu        sync.Mutex
	finished  bool
	callbacks []func()
	onEnd     func(error)
}

func (b *observedBody) isFinished() bool { b.mu.Lock(); defer b.mu.Unlock(); return b.finished }

func (b *observedBody) finish(err error) {
	b.mu.Lock()
	if b.finished {
		b.mu.Unlock()
		return
	}
	// End the span before any registered callback cancels the operation context.
	// afterFinish must not observe completion while onEnd is still pending.
	if b.onEnd != nil {
		b.onEnd(err)
	}
	b.finished = true
	callbacks := b.callbacks
	b.callbacks = nil
	b.mu.Unlock()
	for _, fn := range callbacks {
		fn()
	}
}
func (b *observedBody) afterFinish(fn func()) {
	b.mu.Lock()
	if !b.finished {
		b.callbacks = append(b.callbacks, fn)
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()
	fn()
}
func (b *observedBody) Read(p []byte) (int, error) {
	if b.ReadCloser == nil {
		b.finish(nil)
		return 0, io.EOF
	}
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.finish(err)
	}
	return n, err
}
func (b *observedBody) Close() error {
	var err error
	if b.ReadCloser != nil {
		err = b.ReadCloser.Close()
	}
	b.finish(err)
	return err
}
