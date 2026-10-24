package recovery

import (
	"errors"
	"strings"
	"testing"
)

type panickingStringer struct{}

func (panickingStringer) String() string { panic("format panic") }

func panicFromKnownFunction(value any) {
	panic(value)
}

func TestCatchCapturesStructuredReportAndOrigin(t *testing.T) {
	wantErr := errors.New("synthetic panic")
	var report Report

	func() {
		defer Catch(func(got Report) { report = got })
		panicFromKnownFunction(wantErr)
	}()

	if report.Message != wantErr.Error() {
		t.Fatalf("message = %q, want %q", report.Message, wantErr.Error())
	}
	if report.Type != "*errors.errorString" {
		t.Fatalf("type = %q", report.Type)
	}
	if !errors.Is(report.Cause, wantErr) {
		t.Fatalf("cause = %v, want original error", report.Cause)
	}
	if !strings.HasSuffix(report.Origin.Function, ".panicFromKnownFunction") {
		t.Fatalf("origin = %+v, want panicFromKnownFunction; stack = %+v", report.Origin, report.Stack)
	}
	if report.Origin.File == "" || report.Origin.Line <= 0 {
		t.Fatalf("origin is incomplete: %+v", report.Origin)
	}
	if len(report.Stack) == 0 || report.Stack[0] != report.Origin {
		t.Fatalf("stack does not start at origin: %+v", report.Stack)
	}
}

func TestCatchBoundsAndSafelyFormatsMessage(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "formatter panics", value: panickingStringer{}, want: formatFailure},
		{name: "long value", value: strings.Repeat("x", maxMessageSize+100), want: strings.Repeat("x", maxMessageSize) + "...[truncated]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var report Report
			func() {
				defer Catch(func(got Report) { report = got })
				panic(tt.value)
			}()
			if report.Message != tt.want {
				t.Fatalf("message = %q, want %q", report.Message, tt.want)
			}
		})
	}
}

func TestCatchWithNilHandlerRepanics(t *testing.T) {
	const value = "do not swallow"
	defer func() {
		if got := recover(); got != value {
			t.Fatalf("recovered = %v, want %q", got, value)
		}
	}()

	func() {
		defer Catch(nil)
		panic(value)
	}()
}
