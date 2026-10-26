package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

var _ transport.Transport = (*benchmarkTransport)(nil)

func TestUnsupportedOptionalTransportOperations(t *testing.T) {
	instance, err := Open(DefaultConfig(), WithTransport(&benchmarkTransport{}))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := instance.DeclareTask("optional"); err != nil {
		t.Fatalf("DeclareTask: %v", err)
	}

	if _, err := instance.TopologyStats(context.Background(), TaskRef("optional")); !errors.Is(err, ErrNotSupported) || !errors.Is(err, transport.ErrNotSupported) {
		t.Fatalf("TopologyStats error = %v, want stable ErrNotSupported", err)
	}

	err = instance.Enqueue(context.Background(), "optional", "payload", WithDedupeKey("key"))
	if !errors.Is(err, ErrDedupeUnavailable) || !errors.Is(err, ErrNotSupported) {
		t.Fatalf("dedupe error = %v, want ErrDedupeUnavailable wrapping ErrNotSupported", err)
	}
}

func TestMemoryAdvertisesOnlyImplementedOptionalInterfaces(t *testing.T) {
	backend := memory.New()
	if _, ok := backend.(transport.Deduper); !ok {
		t.Fatal("memory transport does not advertise Deduper")
	}
	if _, ok := backend.(transport.PublicationDeduper); !ok {
		t.Fatal("memory transport does not advertise PublicationDeduper")
	}
	if _, ok := backend.(transport.DLQAdmin); !ok {
		t.Fatal("memory transport does not advertise DLQAdmin")
	}
	if _, ok := backend.(transport.StatsReader); !ok {
		t.Fatal("memory transport does not advertise StatsReader")
	}
	if _, ok := backend.(transport.Pauser); ok {
		t.Fatal("memory transport advertises unsupported Pauser")
	}
}
