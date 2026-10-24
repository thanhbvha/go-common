// Package recovery captures panics as structured, framework-independent reports.
//
// Catch must be deferred in the goroutine it protects. A recover boundary cannot
// catch a panic raised by another goroutine.
package recovery

import (
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

const (
	maxStackDepth  = 32
	maxStackBytes  = 32 * 1024
	maxMessageSize = 4 * 1024
	formatFailure  = "<panic value could not be formatted>"
)

// Frame identifies one call frame in a recovered panic stack.
type Frame struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}

// Report is the bounded diagnostic information captured from a panic.
// Cause is populated only when the panic value implements error. HTTP adapters
// must not expose Message or Cause to clients.
type Report struct {
	Message string  `json:"message"`
	Type    string  `json:"type"`
	Cause   error   `json:"-"`
	Origin  Frame   `json:"origin"`
	Stack   []Frame `json:"stack"`
}

// Handler receives a recovered panic report.
type Handler func(Report)

// Catch recovers a panic in the current goroutine and passes a structured report
// to handler. If handler is nil, the original panic is propagated instead of
// being silently swallowed.
//
// Typical use:
//
//	defer recovery.Catch(func(report recovery.Report) { ... })
func Catch(handler Handler) {
	value := recover()
	if value == nil {
		return
	}
	if handler == nil {
		panic(value)
	}
	handler(Capture(value))
}

// Capture converts a recovered panic value into a bounded structured report.
// Callers normally use Catch so that the panic stack is still available.
func Capture(value any) Report {
	message, cause := panicMessage(value)
	stack := captureStack()
	report := Report{
		Message: message,
		Type:    panicType(value),
		Cause:   cause,
		Stack:   stack,
	}
	if len(stack) > 0 {
		report.Origin = stack[0]
	}
	return report
}

func panicType(value any) string {
	if value == nil {
		return "<nil>"
	}
	return reflect.TypeOf(value).String()
}

func panicMessage(value any) (message string, cause error) {
	switch typed := value.(type) {
	case error:
		cause = typed
		message = safeFormat(func() string { return typed.Error() })
	case string:
		message = typed
	case fmt.Stringer:
		message = safeFormat(typed.String)
	default:
		message = safeFormat(func() string { return fmt.Sprint(typed) })
	}
	return truncateMessage(message), cause
}

func safeFormat(format func() string) (message string) {
	message = formatFailure
	defer func() {
		if recover() != nil {
			message = formatFailure
		}
	}()
	return format()
}

func truncateMessage(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	if len(message) <= maxMessageSize {
		return message
	}
	message = strings.ToValidUTF8(message[:maxMessageSize], "\uFFFD")
	return message + "...[truncated]"
}

func captureStack() []Frame {
	buf := make([]byte, maxStackBytes)
	n := runtime.Stack(buf, false)
	lines := strings.Split(string(buf[:n]), "\n")

	all := make([]Frame, 0, maxStackDepth+8)
	panicBoundary := -1
	for i := 1; i+1 < len(lines); i += 2 {
		functionLine := strings.TrimSpace(lines[i])
		if functionLine == "" || strings.HasPrefix(functionLine, "created by ") {
			break
		}
		frame, ok := parseFrame(functionLine, lines[i+1])
		if !ok {
			continue
		}
		all = append(all, frame)
		if panicBoundary < 0 && isRuntimePanic(frame.Function) {
			panicBoundary = len(all) - 1
		}
		if len(all) == maxStackDepth+8 {
			break
		}
	}

	start := 0
	if panicBoundary >= 0 {
		start = panicBoundary + 1
	}

	result := make([]Frame, 0, maxStackDepth)
	for _, frame := range all[start:] {
		if isInternalFrame(frame.Function) {
			continue
		}
		result = append(result, frame)
		if len(result) == maxStackDepth {
			break
		}
	}
	return result
}

func parseFrame(functionLine, sourceLine string) (Frame, bool) {
	functionEnd := strings.LastIndex(functionLine, "(")
	if functionEnd < 0 {
		return Frame{}, false
	}
	function := functionLine[:functionEnd]

	source := strings.TrimSpace(sourceLine)
	if offset := strings.Index(source, " +0x"); offset >= 0 {
		source = source[:offset]
	}
	lineSeparator := strings.LastIndex(source, ":")
	if lineSeparator < 0 {
		return Frame{}, false
	}
	line, err := strconv.Atoi(source[lineSeparator+1:])
	if err != nil {
		return Frame{}, false
	}
	return Frame{
		File:     source[:lineSeparator],
		Line:     line,
		Function: function,
	}, true
}

func isRuntimePanic(function string) bool {
	if function == "panic" {
		return true
	}
	if !strings.HasPrefix(function, "runtime.") {
		return false
	}
	name := strings.ToLower(strings.TrimPrefix(function, "runtime."))
	return strings.Contains(name, "panic")
}

func isInternalFrame(function string) bool {
	if strings.HasPrefix(function, "runtime.") {
		return true
	}
	const packagePrefix = "github.com/thanhbvha/go-common/recovery."
	if !strings.HasPrefix(function, packagePrefix) {
		return false
	}
	name := strings.TrimPrefix(function, packagePrefix)
	return name == "Catch" || name == "Capture" || name == "captureStack"
}
