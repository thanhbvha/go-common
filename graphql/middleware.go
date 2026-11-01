package graphql

import (
	"context"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type stateKey struct{}
type phaseKey struct{}
type requestState struct {
	telemetry bool
	operation string
	errors    int
	rejected  bool
}

// TelemetryInterceptor enriches the outer request span, including failures
// which have no operation context. It never records client text or error messages.
type TelemetryInterceptor struct{}

func (TelemetryInterceptor) ExtensionName() string                   { return "RequestTelemetry" }
func (TelemetryInterceptor) Validate(graphql.ExecutableSchema) error { return nil }
func (TelemetryInterceptor) InterceptOperation(ctx context.Context, next graphql.OperationHandler) graphql.ResponseHandler {
	if graphql.HasOperationContext(ctx) {
		op := graphql.GetOperationContext(ctx)
		if op.Operation != nil {
			kind := string(op.Operation.Operation)
			if state, ok := ctx.Value(stateKey{}).(*requestState); ok {
				state.operation = kind
				if state.telemetry {
					trace.SpanFromContext(ctx).SetAttributes(attribute.String("graphql.operation.type", kind))
				}
			}
		}
	}
	responses := next(context.WithValue(ctx, phaseKey{}, true))
	return func(ctx context.Context) *graphql.Response {
		res := responses(ctx)
		// gqlgen uses nil as an end-of-stream signal on cancellation. HTTP
		// query/mutation clients need an error envelope instead of JSON null.
		if res == nil && ctx.Err() != nil {
			if state, ok := ctx.Value(stateKey{}).(*requestState); ok {
				state.errors++
			}
			return &graphql.Response{Errors: gqlerror.List{ErrorPresenter(ctx, ctx.Err())}}
		}
		return res
	}
}
func (TelemetryInterceptor) InterceptResponse(ctx context.Context, next graphql.ResponseHandler) *graphql.Response {
	res := next(ctx)
	if res != nil && len(res.Errors) > 0 {
		if state, ok := ctx.Value(stateKey{}).(*requestState); ok {
			state.errors += len(res.Errors)
			if executing, _ := ctx.Value(phaseKey{}).(bool); !executing {
				state.rejected = true
			}
			if state.telemetry {
				trace.SpanFromContext(ctx).SetStatus(codes.Error, "GraphQL request failed")
			}
		}
	}
	return res
}

type instruments struct {
	tracer   trace.Tracer
	duration metric.Float64Histogram
	inflight metric.Int64UpDownCounter
	rejected metric.Int64Counter
	errors   metric.Int64Counter
}

func newInstruments() (*instruments, error) {
	m := otel.Meter("go-common/graphql")
	t := &instruments{tracer: otel.Tracer("go-common/graphql")}
	var err error
	if t.duration, err = m.Float64Histogram("graphql.request.duration", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if t.inflight, err = m.Int64UpDownCounter("graphql.request.inflight"); err != nil {
		return nil, err
	}
	if t.rejected, err = m.Int64Counter("graphql.request.rejected"); err != nil {
		return nil, err
	}
	if t.errors, err = m.Int64Counter("graphql.request.errors"); err != nil {
		return nil, err
	}
	return t, nil
}
func (t *instruments) start(ctx context.Context, state *requestState) (context.Context, func()) {
	ctx, span := t.tracer.Start(ctx, "GraphQL request")
	start := time.Now()
	t.inflight.Add(ctx, 1)
	return ctx, func() {
		defer span.End()
		t.inflight.Add(ctx, -1)
		kind := state.operation
		if kind == "" {
			kind = "unknown"
		}
		attrs := metric.WithAttributes(attribute.String("graphql.operation.type", kind))
		t.duration.Record(ctx, time.Since(start).Seconds(), attrs)
		if state.rejected {
			t.rejected.Add(ctx, 1, attrs)
		}
		if state.errors > 0 {
			t.errors.Add(ctx, int64(state.errors), attrs)
			span.SetStatus(codes.Error, "GraphQL request failed")
		}
		if ctx.Err() != nil {
			span.SetStatus(codes.Error, "GraphQL request cancelled or timed out")
		}
	}
}
