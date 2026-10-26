package eventbus

import (
	"fmt"
	"runtime"
	"strings"
)

const (
	maxPanicStackFrames = 32
	maxPanicStackBytes  = 8 * 1024
)

type panicScope string

const (
	panicScopeHandler      panicScope = "handler"
	panicScopeLogger       panicScope = "logger"
	panicScopeMetrics      panicScope = "metrics"
	panicScopeTracer       panicScope = "tracer"
	panicScopeConsumer     panicScope = "consumer"
	panicScopeWorker       panicScope = "worker"
	panicScopeHeartbeat    panicScope = "heartbeat"
	panicScopeSettlement   panicScope = "settlement"
	panicScopePublish      panicScope = "publish"
	panicScopeDedupe       panicScope = "dedupe"
	panicScopeProvisioning panicScope = "provisioning"
	panicScopeAdmin        panicScope = "admin"
	panicScopeShutdown     panicScope = "shutdown"
)

type panicRecord struct {
	Scope     panicScope
	Operation string
	Stack     string
}

type recoveredPanicError struct {
	scope     panicScope
	operation string
}

func (e *recoveredPanicError) Error() string {
	if e == nil || e.operation == "" {
		return ErrRecoveredPanic.Error()
	}
	return "eventbus: recovered panic in " + e.operation
}

func (e *recoveredPanicError) Unwrap() error { return ErrRecoveredPanic }

func newRecoveredPanicError(scope panicScope, operation string) error {
	return &recoveredPanicError{scope: scope, operation: operation}
}

// guardError catches panic(nil) as well as ordinary panic values by tracking
// normal completion instead of inspecting the recovered value.
func guardError(scope panicScope, operation string, onPanic func(panicRecord), fn func() error) (err error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = recover()
		record := panicRecord{Scope: scope, Operation: operation, Stack: capturePanicStack(4)}
		safePanicCallback(onPanic, record)
		err = newRecoveredPanicError(scope, operation)
	}()
	err = fn()
	completed = true
	return err
}

func guardValue[T any](scope panicScope, operation string, onPanic func(panicRecord), fn func() (T, error)) (value T, err error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = recover()
		record := panicRecord{Scope: scope, Operation: operation, Stack: capturePanicStack(4)}
		safePanicCallback(onPanic, record)
		var zero T
		value = zero
		err = newRecoveredPanicError(scope, operation)
	}()
	value, err = fn()
	completed = true
	return value, err
}

func guardCall(scope panicScope, operation string, onPanic func(panicRecord), fn func()) (panicked bool) {
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = recover()
		panicked = true
		record := panicRecord{Scope: scope, Operation: operation, Stack: capturePanicStack(4)}
		safePanicCallback(onPanic, record)
	}()
	fn()
	completed = true
	return false
}

func safePanicCallback(callback func(panicRecord), record panicRecord) {
	if callback == nil {
		return
	}
	defer func() { _ = recover() }()
	callback(record)
}

// capturePanicStack records source locations only. It never formats the panic
// value or function arguments.
func capturePanicStack(skip int) string {
	var pcs [maxPanicStackFrames]uintptr
	n := runtime.Callers(skip, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var stack strings.Builder
	for {
		frame, more := frames.Next()
		line := fmt.Sprintf("%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if stack.Len()+len(line) > maxPanicStackBytes {
			break
		}
		stack.WriteString(line)
		if !more {
			break
		}
	}
	return stack.String()
}
