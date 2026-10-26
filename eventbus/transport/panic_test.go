package transport

import "testing"

func TestGuardRecoversPanicNilAndCallbackPanic(t *testing.T) {
	called := false
	if !Guard(func() { called = true; panic("callback") }, func() { panic(nil) }) {
		t.Fatal("panic(nil) was not recovered")
	}
	if !called {
		t.Fatal("panic callback was not called")
	}
	if Guard(nil, func() {}) {
		t.Fatal("normal completion reported panic")
	}
}
