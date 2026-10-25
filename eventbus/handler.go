package eventbus

import (
	"context"
	"fmt"
	"runtime"
	"strings"
)

// TypedHandler is a generic handler function that takes a strongly-typed payload.
type TypedHandler[T any] func(ctx context.Context, job Job, payload T) error

// Handle wraps a TypedHandler to decode the payload and run middlewares.
func Handle[T any](handler TypedHandler[T], mws ...Middleware) Handler {
	if handler == nil {
		return nil
	}
	for _, mw := range mws {
		if mw == nil {
			return nil
		}
	}
	h := func(ctx context.Context, job Job) error {
		var payload T
		if err := job.Decode(&payload); err != nil {
			return fmt.Errorf("decode payload: %w", err)
		}
		return handler(ctx, job, payload)
	}

	// Apply middlewares in reverse order so they wrap each other correctly
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}

	return h
}

// Middleware is a function that wraps a Handler.
type Middleware func(Handler) Handler

// Enqueue is a generic helper to enqueue a payload without needing to cast it.
func Enqueue[T any](ctx context.Context, b Bus, jobType string, payload T, opts ...JobOption) error {
	return b.Enqueue(ctx, jobType, payload, opts...)
}

// Recoverer recovers handler panics when a Handler is called outside the engine.
// The engine already provides this protection by default. Recovery does not
// expose the panic value; stack logs contain only bounded source locations.
func Recoverer(logger Logger) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, job Job) error {
			return invokeHandler(ctx, job, next, logger)
		}
	}
}

func invokeHandler(ctx context.Context, job Job, handler Handler, logger Logger) (err error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		// Do not format or unwrap a panic value: it may contain secrets or have
		// an Error/String method that itself panics. The completion flag also
		// handles panic(nil) when GODEBUG=panicnil=1.
		_ = recover()
		err = ErrHandlerPanic
		if logger != nil {
			logger.ErrorAsync("panic recovered in job handler", "job_id", job.ID(), "type", job.Type(), "stack", panicStack())
		}
	}()
	err = handler(ctx, job)
	completed = true
	return err
}

// Unlike debug.Stack, CallersFrames does not include function argument values.
// Bound both frame collection and serialized output; never put this in Cause.
func panicStack() string {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var stack strings.Builder
	for {
		frame, more := frames.Next()
		line := fmt.Sprintf("%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if stack.Len()+len(line) > 8*1024 {
			break
		}
		stack.WriteString(line)
		if !more {
			break
		}
	}
	return stack.String()
}
