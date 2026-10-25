package eventbus

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type resultRecordingTransport struct {
	benchmarkTransport
	messages []transport.Message
}

func (t *resultRecordingTransport) Publish(_ context.Context, _ transport.Topology, message transport.Message) error {
	t.messages = append(t.messages, message)
	return nil
}

func TestPublicationResultPreservesStableMessageID(t *testing.T) {
	backend := &resultRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	result, err := bus.EnqueueWithResult(context.Background(), "task", "payload", WithMessageID("stable-id"))
	if err != nil {
		t.Fatal(err)
	}
	if result.ID != "stable-id" || result.Duplicate {
		t.Fatalf("result = %#v", result)
	}
	if len(backend.messages) != 1 || backend.messages[0].ID != "stable-id" {
		t.Fatalf("messages = %#v", backend.messages)
	}
	env, err := NewCodec(nil).Decode(backend.messages[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if env.ID != result.ID {
		t.Fatalf("envelope ID = %q, result ID = %q", env.ID, result.ID)
	}
}

func TestPublicationResultGeneratesMessageID(t *testing.T) {
	backend := &resultRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareEvent("event"); err != nil {
		t.Fatal(err)
	}
	result, err := bus.PublishWithResult(context.Background(), "event", "payload")
	if err != nil {
		t.Fatal(err)
	}
	if result.ID == "" || result.Duplicate {
		t.Fatalf("result = %#v", result)
	}
}

func TestWithMessageIDValidation(t *testing.T) {
	backend := &resultRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	for name, id := range map[string]string{
		"empty":     "",
		"padded":    " id ",
		"control":   "id\n",
		"oversized": strings.Repeat("x", maxEnvelopeIDBytes+1),
		"invalid":   string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			result, err := bus.EnqueueWithResult(context.Background(), "task", nil, WithMessageID(id))
			if !errors.Is(err, ErrInvalidOption) || result.ID != "" {
				t.Fatalf("result=%#v error=%v", result, err)
			}
		})
	}
	if len(backend.messages) != 0 {
		t.Fatalf("invalid IDs published %d messages", len(backend.messages))
	}
}
