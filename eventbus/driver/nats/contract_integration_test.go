//go:build integration

package nats

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
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
			options := map[string]any{"nats.ack_wait": 100 * time.Millisecond}
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
		PrepareUnsettledRedelivery: func(*testing.T, transport.Transport, transport.Topology) {
			time.Sleep(150 * time.Millisecond)
		},
		PostCommitAmbiguity: func(t *testing.T, tr transport.Transport, topo transport.Topology, kind transport.DecisionKind, phase transportcontract.AmbiguityPhase) transportcontract.AmbiguityState {
			backend := tr.(*natsContractTransport).natsTransport
			switch phase {
			case transportcontract.AmbiguityArm:
				backend.afterTransfer = func(got transport.DecisionKind) error {
					if got != kind {
						t.Fatalf("ambiguity transfer kind = %s, want %s", got, kind)
					}
					return errors.New("synthetic response loss after durable NATS transfer")
				}
				return transportcontract.AmbiguityState{}
			case transportcontract.AmbiguityObserve:
				backend.afterTransfer = nil
				stats, err := backend.Stats(context.Background(), topo)
				if err != nil {
					t.Fatal(err)
				}
				count := stats.DLQ
				if kind == transport.DecisionRetry {
					count = stats.Delayed
				}
				return transportcontract.AmbiguityState{SourceOutstanding: stats.InFlight > 0, RetrySameDelivery: true, DestinationCount: count, ExpectedFinalDestinations: 1}
			case transportcontract.AmbiguityFinal:
				stats, err := backend.Stats(context.Background(), topo)
				if err != nil {
					t.Fatal(err)
				}
				count := stats.DLQ
				if kind == transport.DecisionRetry {
					count = stats.Delayed
				}
				return transportcontract.AmbiguityState{SourceOutstanding: stats.InFlight > 0, DestinationCount: count}
			default:
				t.Fatalf("unknown ambiguity phase %d", phase)
				return transportcontract.AmbiguityState{}
			}
		},
		SchedulerOwnershipRecovery: func(t *testing.T, first, second transport.Transport, topo transport.Topology, now time.Time) int {
			firstBackend := first.(*natsContractTransport).natsTransport
			secondBackend := second.(*natsContractTransport).natsTransport
			ctx := context.Background()
			kv, err := firstBackend.getKV(firstBackend.kvBucketName(topo))
			if err != nil {
				t.Fatal(err)
			}
			keys := scheduledKeys(t, kv)
			if len(keys) != 1 {
				t.Fatalf("scheduled ownership records = %d, want 1", len(keys))
			}
			entry, err := kv.Get(keys[0])
			if err != nil {
				t.Fatal(err)
			}
			var record delayRecord
			if err := json.Unmarshal(entry.Value(), &record); err != nil {
				t.Fatal(err)
			}
			record.Owner = firstBackend.instanceID
			record.LeaseUntil = now.Add(time.Second).UnixMilli()
			claimed, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kv.Update(keys[0], claimed, entry.Revision()); err != nil {
				t.Fatal(err)
			}
			if moved, err := secondBackend.dispatchDue(ctx, topo, kv, now); err != nil || moved != 0 {
				t.Fatalf("successor dispatched live NATS lease = %d, %v", moved, err)
			}
			if moved, err := secondBackend.dispatchDue(ctx, topo, kv, now.Add(2*time.Second)); err != nil || moved != 1 {
				t.Fatalf("successor recovered NATS lease = %d, %v", moved, err)
			}
			return 1
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
				"nats.ack_wait": 100 * time.Millisecond, "nats.delay_batch_size": 1,
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
		PrepareUnsettledRedelivery: func(*testing.T, transport.Topology) {
			time.Sleep(150 * time.Millisecond)
		},
		DurableRedelivery:        true,
		RepeatedReconnect:        true,
		ReconnectSessionLifetime: 25 * time.Millisecond,
		ReceiveWithin:            5 * time.Second,
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
