package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestProducerOnlyTopologyDeclarations(t *testing.T) {
	recorder := &topologyRecordingTransport{
		published: make(chan transport.Topology, 2),
		ensured:   make(chan transport.Topology, 2),
	}
	bus, err := Open(DefaultConfig(), WithTransport(recorder))
	if err != nil {
		t.Fatal(err)
	}

	if err := bus.DeclareTask("email.send"); err != nil {
		t.Fatalf("DeclareTask: %v", err)
	}
	if err := bus.DeclareEvent("user.created"); err != nil {
		t.Fatalf("DeclareEvent: %v", err)
	}

	ctx := context.Background()
	if err := bus.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ensured := map[TopologyKind]bool{}
	for range 2 {
		topo := <-recorder.ensured
		ensured[topo.Kind] = true
	}
	if !ensured[TaskTopology] || !ensured[EventTopology] {
		t.Fatalf("provisioned kinds = %v, want task and event", ensured)
	}

	if err := bus.Enqueue(ctx, "email.send", "payload"); err != nil {
		t.Fatalf("producer-only Enqueue: %v", err)
	}
	if err := bus.Publish(ctx, "user.created", "payload"); err != nil {
		t.Fatalf("producer-only Publish: %v", err)
	}

	published := map[TopologyKind]bool{}
	for range 2 {
		topo := <-recorder.published
		published[topo.Kind] = true
	}
	if !published[TaskTopology] || !published[EventTopology] {
		t.Fatalf("published kinds = %v, want task and event", published)
	}

	if err := bus.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestHandleTaskRequiresDeclaration(t *testing.T) {
	bus, err := Open(DefaultConfig(), WithTransport(&topologyRecordingTransport{}))
	if err != nil {
		t.Fatal(err)
	}

	err = bus.HandleTask("undeclared", func(context.Context, Job) error { return nil })
	if !errors.Is(err, ErrUndeclaredTopology) {
		t.Fatalf("HandleTask error = %v, want ErrUndeclaredTopology", err)
	}
}

func TestDeclaredTopologyRequiredForPublish(t *testing.T) {
	bus, err := Open(DefaultConfig(), WithTransport(&topologyRecordingTransport{}))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := bus.Enqueue(ctx, "undeclared", "payload"); !errors.Is(err, ErrUnknownJobType) {
		t.Fatalf("Enqueue error = %v, want ErrUnknownJobType", err)
	}
	if err := bus.Publish(ctx, "undeclared", "payload"); !errors.Is(err, ErrUnknownJobType) {
		t.Fatalf("Publish error = %v, want ErrUnknownJobType", err)
	}
}
