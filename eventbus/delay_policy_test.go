package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDelayDisabledRejectsFuturePublicationBeforeTransport(t *testing.T) {
	backend := &resultRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task", WithDelayDisabled()); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.EnqueueWithResult(context.Background(), "task", nil, WithDelay(time.Minute)); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("future publication = %v", err)
	}
	if len(backend.messages) != 0 {
		t.Fatalf("published = %d", len(backend.messages))
	}
	if _, err := bus.EnqueueWithResult(context.Background(), "task", nil); err != nil {
		t.Fatalf("immediate publication = %v", err)
	}
}
