//go:build integration

package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	buscontract "github.com/thanhbvha/go-common/eventbus/contracttest"
	"github.com/thanhbvha/go-common/eventbus/transport"
	transportcontract "github.com/thanhbvha/go-common/eventbus/transport/contracttest"
)

func TestNATSTransportContract(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()

	transportcontract.Run(t, transportcontract.Harness{
		New: func(t *testing.T) transport.Transport {
			backend := New(js).(*natsTransport)
			t.Cleanup(func() {
				if err := backend.Close(context.Background()); err != nil {
					t.Errorf("close NATS contract transport: %v", err)
				}
			})
			return &natsContractTransport{natsTransport: backend}
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			name = strings.NewReplacer("/", "_", " ", "_").Replace(name)
			options := map[string]any{"nats.ack_wait": time.Minute}
			if kind == transport.EventTopology {
				options[natsEventLayoutOption] = natsTargetedLayoutV1
			}
			return transport.Topology{
				Namespace: "nats_contract", JobType: name, Kind: kind,
				Options: options,
			}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			backend := tr.(*natsContractTransport).natsTransport
			kv, err := backend.getKV(backend.kvBucketName(topo))
			if err != nil {
				return 0, err
			}
			moved, err := backend.dispatchDue(ctx, topo, kv, now)
			return int64(moved), err
		},
		ExpireDedupe: func(t *testing.T, ttl time.Duration) {
			t.Helper()
			time.Sleep(ttl + 25*time.Millisecond)
		},
		SubscriberRetryIsolation: true,
		SubscriberDLQIsolation:   true,
		ReceiveWithin:            5 * time.Second,
	})
}

func TestNATSBusContract(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	buscontract.Run(t, buscontract.Harness{
		New: func(t *testing.T) transport.Transport {
			backend := New(js).(*natsTransport)
			t.Cleanup(func() { _ = backend.Close(context.Background()) })
			return &natsContractTransport{natsTransport: backend}
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			name = strings.NewReplacer("/", "_", " ", "_").Replace(name)
			return transport.Topology{Namespace: "nats_bus_contract", JobType: name, Kind: kind, Options: map[string]any{
				"nats.ack_wait": time.Minute, "nats.delay_batch_size": 1,
			}}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			backend := tr.(*natsContractTransport).natsTransport
			kv, err := backend.getKV(backend.kvBucketName(topo))
			if err != nil {
				return 0, err
			}
			moved, err := backend.dispatchDue(ctx, topo, kv, now)
			return int64(moved), err
		},
		ReceiveWithin: 5 * time.Second,
	})
}

// natsContractTransport stops the autonomous scheduler after provisioning so
// the shared suite can move a fake deadline explicitly without timing races.
type natsContractTransport struct {
	*natsTransport
}

func (t *natsContractTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if topo.Kind == transport.EventTopology {
		options := make(map[string]any, len(topo.Options)+1)
		for key, value := range topo.Options {
			options[key] = value
		}
		options[natsEventLayoutOption] = natsTargetedLayoutV1
		topo.Options = options
	}
	if err := t.natsTransport.EnsureTopology(ctx, topo); err != nil {
		return err
	}
	t.mu.Lock()
	cancel := t.sweepers[topo.Name()]
	delete(t.sweepers, topo.Name())
	t.mu.Unlock()
	if cancel != nil {
		cancel()
		t.sweeperWG.Wait()
	}
	return nil
}
