//go:build integration

package nats

import (
	"context"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestEncodedTopologyNamesPreventNATSCrossDelivery(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	first := transport.Topology{Namespace: "acme", JobType: "event.foo", Kind: transport.TaskTopology}
	second := targetedEventTopology("acme", "foo")
	if backend.streamName(first) == backend.streamName(second) {
		t.Fatal("hostile task/event tuples share a NATS stream")
	}
	for _, topo := range []transport.Topology{first, second} {
		if err := backend.EnsureTopology(ctx, topo); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Publish(ctx, first, transport.Message{ID: "task-id", Payload: []byte("task")}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, second, transport.Message{ID: "event-id", Payload: []byte("event")}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		topo    transport.Topology
		payload string
	}{{first, "task"}, {second, "event"}} {
		info, err := js.StreamInfo(backend.streamName(want.topo))
		if err != nil || info.State.Msgs != 1 {
			t.Fatalf("stream state = %+v, %v", info, err)
		}
		msg, err := js.GetMsg(backend.streamName(want.topo), 1)
		if err != nil || string(msg.Data) != want.payload {
			t.Fatalf("stream payload = %q, %v; want %q", msg.Data, err, want.payload)
		}
	}
}
