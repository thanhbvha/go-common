package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestLegacyRegisterAdapterPreservesErrorIdentity(t *testing.T) {
	bus, err := Open(DefaultConfig(), WithTransport(&topologyRecordingTransport{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareEvent("shared"); err != nil {
		t.Fatal(err)
	}

	err = bus.Register("shared", func(context.Context, Job) error { return nil })
	if !errors.Is(err, ErrTopologyKindConflict) {
		t.Fatalf("Register error = %v, want ErrTopologyKindConflict", err)
	}
}

func TestLegacyPublishEventAdapter(t *testing.T) {
	recorder := &topologyRecordingTransport{published: make(chan transport.Topology, 1)}
	bus, err := Open(DefaultConfig(), WithTransport(recorder))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := bus.PublishEvent(ctx, "undeclared", "payload"); !errors.Is(err, ErrUnknownJobType) {
		t.Fatalf("PublishEvent undeclared error = %v, want ErrUnknownJobType", err)
	}
	if err := bus.DeclareEvent("declared"); err != nil {
		t.Fatal(err)
	}
	if err := bus.PublishEvent(ctx, "declared", "payload"); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	topology := <-recorder.published
	if topology.Kind != EventTopology || topology.JobType != "declared" {
		t.Fatalf("PublishEvent topology = %#v, want declared event", topology)
	}
}
