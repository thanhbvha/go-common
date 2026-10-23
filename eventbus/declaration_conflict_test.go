package eventbus

import (
	"context"
	"errors"
	"testing"
)

func TestDeclarationsAreIdempotentEqualAndRejectConflicts(t *testing.T) {
	bus, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task", WithConcurrency(2)); err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task", WithConcurrency(2)); err != nil {
		t.Fatalf("equal redeclaration = %v", err)
	}
	if err := bus.DeclareTask("task", WithConcurrency(3)); !errors.Is(err, ErrConflictingDeclaration) {
		t.Fatalf("conflicting task = %v", err)
	}
	if err := bus.DeclareEvent("event", WithPrefetch(2)); err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareEvent("event", WithPrefetch(2)); err != nil {
		t.Fatalf("equal event redeclaration = %v", err)
	}
	if err := bus.DeclareEvent("event", WithPrefetch(3)); !errors.Is(err, ErrConflictingDeclaration) {
		t.Fatalf("conflicting event = %v", err)
	}
}

func TestDuplicateHandlersAndSubscribersAreRejected(t *testing.T) {
	bus, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
	if err != nil {
		t.Fatal(err)
	}
	handler := func(context.Context, Job) error { return nil }
	if err := bus.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("task", handler); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("task", handler); !errors.Is(err, ErrDuplicateHandler) {
		t.Fatalf("duplicate handler = %v", err)
	}
	if err := bus.DeclareEvent("event"); err != nil {
		t.Fatal(err)
	}
	if err := bus.Subscribe("event", "subscriber", handler); err != nil {
		t.Fatal(err)
	}
	if err := bus.Subscribe("event", "subscriber", handler); !errors.Is(err, ErrDuplicateSubscriber) {
		t.Fatalf("duplicate subscriber = %v", err)
	}
}

func TestSubscriberRejectsProvisioningOptions(t *testing.T) {
	bus, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareEvent("event"); err != nil {
		t.Fatal(err)
	}
	handler := func(context.Context, Job) error { return nil }
	if err := bus.Subscribe("event", "bad", handler, WithOption("kafka.partitions", int32(3))); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("provisioning subscriber option = %v", err)
	}
	if err := bus.Subscribe("event", "good", handler, WithConcurrency(2)); err != nil {
		t.Fatalf("runtime subscriber option = %v", err)
	}
}

func TestImplicitEventDeclarationDoesNotPromoteSubscriberOptions(t *testing.T) {
	backend := &benchmarkTransport{}
	instance, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.Subscribe("event", "subscriber", func(context.Context, Job) error { return nil }, WithConcurrency(2)); err != nil {
		t.Fatal(err)
	}
	concrete := instance.(*bus)
	if _, exists := concrete.engine.eventTopologies["event"].Options["eventbus.concurrency"]; exists {
		t.Fatal("subscriber runtime option was promoted into the event declaration")
	}
	if got := concrete.engine.subTopologies["event"]["subscriber"].Options["eventbus.concurrency"]; got != 2 {
		t.Fatalf("subscriber concurrency = %#v", got)
	}
}
