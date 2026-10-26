package transport

import "errors"

// ErrRecoveredPanic identifies a panic recovered inside a transport-owned
// goroutine. The original panic value is deliberately excluded.
var ErrRecoveredPanic = errors.New("eventbus: recovered panic in transport")

// Guard runs fn with a panic boundary suitable for transport-owned goroutines.
// onPanic must update local health or cancel local ownership; it is itself
// protected from panic. Guard detects panic(nil) through completion tracking.
func Guard(onPanic func(), fn func()) (panicked bool) {
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = recover()
		panicked = true
		if onPanic != nil {
			defer func() { _ = recover() }()
			onPanic()
		}
	}()
	fn()
	completed = true
	return false
}
