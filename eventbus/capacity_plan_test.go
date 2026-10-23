package eventbus

import (
	"context"
	"errors"
	"testing"
)

func TestCapacityPlanFailsBeforeProvisioning(t *testing.T) {
	backend := &lifecycleTransport{}
	cfg := DefaultConfig()
	cfg.MaxTopologies = 1
	bus, err := Open(cfg, WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("one"); err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("two"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Start(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Start = %v", err)
	}
	if backend.provisions.Load() != 0 {
		t.Fatalf("provisions = %d", backend.provisions.Load())
	}
	if err := bus.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
