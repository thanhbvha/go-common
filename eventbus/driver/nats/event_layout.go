package nats

import (
	"context"
	"errors"
	"fmt"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *natsTransport) ensureEventLayoutMarker(ctx context.Context, topology transport.Topology, kv gonats.KeyValue) error {
	if topology.Kind != transport.EventTopology {
		return nil
	}
	if _, err := kv.Create(eventLayoutMarkerKey, []byte(natsTargetedLayoutV1)); err != nil && !errors.Is(err, gonats.ErrKeyExists) {
		return fmt.Errorf("persist targeted event layout marker: %w", err)
	}
	return t.validateEventLayoutKV(topology, kv)
}

func (t *natsTransport) validateEventLayout(ctx context.Context, topology transport.Topology) error {
	if topology.Kind != transport.EventTopology {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	kv, err := t.getKV(t.kvBucketName(topology))
	if err != nil {
		return fmt.Errorf("nats: targeted event layout is not provisioned: %w", err)
	}
	return t.validateEventLayoutKV(topology, kv)
}

func (t *natsTransport) validateEventLayoutKV(topology transport.Topology, kv gonats.KeyValue) error {
	if topology.Kind != transport.EventTopology {
		return nil
	}
	entry, err := kv.Get(eventLayoutMarkerKey)
	if err != nil {
		return fmt.Errorf("nats: targeted event layout is not provisioned: %w", err)
	}
	if string(entry.Value()) != natsTargetedLayoutV1 {
		return fmt.Errorf("nats: targeted event layout marker is %q, want %q", entry.Value(), natsTargetedLayoutV1)
	}
	return nil
}
