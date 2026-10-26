package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

type panickingAdminTransport struct{ benchmarkTransport }

func (*panickingAdminTransport) Stats(context.Context, transport.Topology) (transport.TopologyStats, error) {
	panic("stats")
}
func (*panickingAdminTransport) List(context.Context, transport.Topology, int, int) ([]transport.DeadLetter, error) {
	panic("list")
}
func (*panickingAdminTransport) Requeue(context.Context, transport.Topology, []string) error {
	panic("requeue")
}
func (*panickingAdminTransport) Purge(context.Context, transport.Topology) error { panic("purge") }
func (*panickingAdminTransport) Count(context.Context, transport.Topology) (int64, error) {
	panic("count")
}
func (*panickingAdminTransport) Ping(context.Context) error { panic("ping") }

func TestBusAdminUsesExplicitTopologyKinds(t *testing.T) {
	backend := memory.New()
	instance, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	if err := instance.DeclareTask("shared"); err != nil {
		t.Fatal(err)
	}
	// A separate bus can declare the same logical name as an event against the
	// same transport, proving administration does not force task routing.
	eventBus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventBus.DeclareEvent("shared"); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.TopologyStats(context.Background(), TaskRef("shared")); err != nil {
		t.Fatal(err)
	}
	if _, err := eventBus.TopologyStats(context.Background(), EventRef("shared")); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.TopologyStats(context.Background(), EventRef("shared")); !errors.Is(err, ErrUndeclaredTopology) {
		t.Fatalf("event stats error = %v", err)
	}
}

func TestBusDLQAdminDoesNotRequireConcreteTransport(t *testing.T) {
	backend := memory.New()
	instance, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	if err := instance.DeclareTask("admin"); err != nil {
		t.Fatal(err)
	}
	ref := TaskRef("admin")
	if count, err := instance.CountDeadLetters(context.Background(), ref); err != nil || count != 0 {
		t.Fatalf("count = %d, %v", count, err)
	}
	page, err := instance.ListDeadLetters(context.Background(), ref, "", 10)
	if err != nil || len(page.Records) != 0 {
		t.Fatalf("page = %#v, %v", page, err)
	}
	if err := instance.RequeueDeadLetters(context.Background(), ref, nil); err != nil {
		t.Fatal(err)
	}
	if err := instance.PurgeDeadLetters(context.Background(), ref); err != nil {
		t.Fatal(err)
	}
}

func TestBusSubscriberStatsUsesExplicitGroup(t *testing.T) {
	backend := memory.New()
	instance, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(context.Background())
	if err := instance.DeclareEvent("event"); err != nil {
		t.Fatal(err)
	}
	if err := instance.Subscribe("event", "alpha", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := backend.(transport.SubscriptionProvisioner).EnsureSubscription(context.Background(), transport.Subscription{
		Topology: transport.Topology{Namespace: DefaultNamespace, JobType: "event", Kind: EventTopology}, Group: "alpha",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.SubscriberStats(context.Background(), "event", "alpha"); err != nil {
		t.Fatal(err)
	}
	if _, err := instance.SubscriberStats(context.Background(), "event", "missing"); !errors.Is(err, ErrUndeclaredTopology) {
		t.Fatalf("missing subscriber error = %v", err)
	}
}

func TestAdminPanicsReturnRecoveredPanic(t *testing.T) {
	instance, err := Open(DefaultConfig(), WithTransport(&panickingAdminTransport{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := instance.DeclareTask("admin"); err != nil {
		t.Fatal(err)
	}
	ref := TaskRef("admin")
	checks := []func() error{
		func() error { return instance.Ping(context.Background()) },
		func() error { _, err := instance.TopologyStats(context.Background(), ref); return err },
		func() error { _, err := instance.ListDeadLetters(context.Background(), ref, "", 1); return err },
		func() error { _, err := instance.CountDeadLetters(context.Background(), ref); return err },
		func() error { return instance.RequeueDeadLetters(context.Background(), ref, []string{"id"}) },
		func() error { return instance.PurgeDeadLetters(context.Background(), ref) },
	}
	for i, check := range checks {
		if err := check(); !errors.Is(err, ErrRecoveredPanic) {
			t.Fatalf("check %d error = %v", i, err)
		}
	}
}
