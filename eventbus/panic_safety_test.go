package eventbus

import (
	"errors"
	"strings"
	"testing"
)

func TestGuardErrorSanitizesPanic(t *testing.T) {
	const secret = "sensitive-panic-value"
	var record panicRecord
	err := guardError(panicScopePublish, "transport publish", func(got panicRecord) {
		record = got
	}, func() error {
		panic(secret)
	})
	if !errors.Is(err, ErrRecoveredPanic) {
		t.Fatalf("error = %v, want ErrRecoveredPanic", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(record.Stack, secret) {
		t.Fatal("panic value leaked through recovered diagnostics")
	}
	if record.Scope != panicScopePublish || record.Operation != "transport publish" {
		t.Fatalf("record = %+v", record)
	}
	if record.Stack == "" || len(record.Stack) > maxPanicStackBytes {
		t.Fatalf("stack length = %d", len(record.Stack))
	}
}

func TestGuardsDetectPanicNilAndCallbackPanic(t *testing.T) {
	callbackCalls := 0
	err := guardError(panicScopeWorker, "worker", func(panicRecord) {
		callbackCalls++
		panic("callback failure")
	}, func() error {
		panic(nil)
	})
	if !errors.Is(err, ErrRecoveredPanic) || callbackCalls != 1 {
		t.Fatalf("error = %v, callback calls = %d", err, callbackCalls)
	}

	value, err := guardValue(panicScopeAdmin, "stats", nil, func() (int, error) {
		panic(nil)
	})
	if value != 0 || !errors.Is(err, ErrRecoveredPanic) {
		t.Fatalf("value = %d, error = %v", value, err)
	}

	if !guardCall(panicScopeTracer, "end span", nil, func() { panic(nil) }) {
		t.Fatal("guardCall did not report panic(nil)")
	}
}

func TestGuardsPreserveNormalResults(t *testing.T) {
	wantErr := errors.New("ordinary")
	if err := guardError(panicScopePublish, "publish", nil, func() error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v", err)
	}
	value, err := guardValue(panicScopeAdmin, "stats", nil, func() (int, error) { return 7, nil })
	if err != nil || value != 7 {
		t.Fatalf("value = %d, error = %v", value, err)
	}
	if guardCall(panicScopeMetrics, "counter", nil, func() {}) {
		t.Fatal("normal call reported panic")
	}
}
