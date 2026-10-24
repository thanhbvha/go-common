package nats

import "testing"

func TestLookupDefault(t *testing.T) {
	previous, hadPrevious := LookupDefault()
	t.Cleanup(func() {
		if hadPrevious {
			SetDefault(previous)
		} else {
			SetDefault(nil)
		}
	})

	SetDefault(nil)
	if client, ok := LookupDefault(); ok || client != nil {
		t.Fatalf("LookupDefault() = (%v, %v), want (nil, false)", client, ok)
	}

	want := New(DefaultConfig())
	SetDefault(want)
	if got, ok := LookupDefault(); !ok || got != want {
		t.Fatalf("LookupDefault() = (%v, %v), want configured client", got, ok)
	}
}
