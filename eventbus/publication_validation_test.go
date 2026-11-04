package eventbus_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

type validationTransport struct {
	transport.Transport
	publications atomic.Int32
}

func (t *validationTransport) Publish(ctx context.Context, topology transport.Topology, message transport.Message) error {
	t.publications.Add(1)
	return t.Transport.Publish(ctx, topology, message)
}

type invalidHeaderInjector struct{}

func (invalidHeaderInjector) Start(ctx context.Context, _ string, _ eventbus.Headers) (context.Context, func()) {
	return ctx, func() {}
}
func (invalidHeaderInjector) Inject(_ context.Context, carrier eventbus.Headers) {
	carrier["injected"] = "invalid\nvalue"
}

func TestPublicationRejectsUndeliverableMetadataBeforeAcceptance(t *testing.T) {
	for _, scenario := range []string{"oversized header", "control character", "too many headers", "trace injection"} {
		t.Run(scenario, func(t *testing.T) {
			tr := &validationTransport{Transport: memory.New()}
			cfg := eventbus.DefaultConfig()
			var options []eventbus.JobOption
			switch scenario {
			case "oversized header":
				options = append(options, eventbus.WithHeader("application", strings.Repeat("x", 1025)))
			case "control character":
				options = append(options, eventbus.WithHeader("application", "secret\nvalue"))
			case "too many headers":
				for i := 0; i < 65; i++ {
					options = append(options, eventbus.WithHeader(string(rune('A'+i)), "value"))
				}
			case "trace injection":
				cfg.Tracer = invalidHeaderInjector{}
			}
			b, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close(context.Background())
			if err := b.DeclareTask("task"); err != nil {
				t.Fatal(err)
			}
			if _, err := b.EnqueueWithResult(t.Context(), "task", struct{}{}, options...); !errors.Is(err, eventbus.ErrInvalidOption) {
				t.Fatal("invalid envelope accepted", err)
			}
			if tr.publications.Load() != 0 {
				t.Fatal("broker received undeliverable publication")
			}
		})
	}
}
