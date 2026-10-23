package eventbus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type unorderedRecordingTransport struct {
	resultRecordingTransport
}

func (*unorderedRecordingTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{DelayPrecision: time.Millisecond}
}

func TestConcurrencyKeyDoesNotRequireBrokerOrdering(t *testing.T) {
	backend := &unorderedRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task", WithConcurrencyKeyLimit(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.EnqueueWithResult(context.Background(), "task", nil, WithConcurrencyKey("account-7")); err != nil {
		t.Fatal(err)
	}
	if len(backend.messages) != 1 || backend.messages[0].OrderingKey != "" || backend.messages[0].ConcurrencyKey != "account-7" {
		t.Fatalf("message = %#v", backend.messages)
	}
	if _, err := bus.EnqueueWithResult(context.Background(), "task", nil, WithBrokerOrderingKey("account-7")); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("broker ordering error = %v", err)
	}
}

func TestLegacyOrderingKeySetsBothKeys(t *testing.T) {
	codec := NewCodec(nil)
	_, env, err := codec.EncodeTopology(TaskTopology, "task", nil, 1, WithOrderingKey("account-7"))
	if err != nil {
		t.Fatal(err)
	}
	if env.OrderingKey != "account-7" || env.ConcurrencyKey != "account-7" {
		t.Fatalf("envelope = %#v", env)
	}
}

func TestPublicationRejectsOversizedKeysBeforeBrokerIO(t *testing.T) {
	backend := &unorderedRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("x", maxEnvelopeKeyBytes+1)
	for name, option := range map[string]JobOption{
		"ordering":    WithBrokerOrderingKey(key),
		"concurrency": WithConcurrencyKey(key),
		"dedupe":      WithDedupeKey(key),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := bus.EnqueueWithResult(context.Background(), "task", nil, option); !errors.Is(err, ErrInvalidOption) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if len(backend.messages) != 0 {
		t.Fatalf("published = %d", len(backend.messages))
	}
}
