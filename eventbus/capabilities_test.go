package eventbus

import (
	"context"
	"errors"
	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"testing"
)

type panickingCapabilities struct{ transport.Transport }

func (panickingCapabilities) Capabilities() transport.Capabilities {
	panic("synthetic capabilities panic")
}
func TestOptionalCapabilityProvider(t *testing.T) {
	for _, panics := range []bool{false, true} {
		backend := memory.New()
		var tr transport.Transport = backend
		if panics {
			tr = panickingCapabilities{backend}
		}
		b, err := Open(DefaultConfig(), WithTransport(tr))
		if err != nil {
			t.Fatal(err)
		}
		provider, ok := b.(CapabilityProvider)
		if !ok {
			t.Fatal("capability provider unavailable")
		}
		caps, err := provider.Capabilities()
		if panics {
			if !errors.Is(err, ErrRecoveredPanic) {
				t.Fatalf("panic not isolated: %v", err)
			}
		} else if err != nil || caps.Dedupe != transport.DedupeProcessConfirmed {
			t.Fatalf("capabilities=%+v error=%v", caps, err)
		}
		if err = b.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
