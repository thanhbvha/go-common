package nats

import (
	"context"
	"errors"
	"fmt"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func (t *natsTransport) ensureDelayLayoutMarker(ctx context.Context, topology transport.Topology, kv gonats.KeyValue) error {
	if !delayLayoutIndexed(topology) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := kv.Create(delayLayoutMarkerKey, []byte(natsDelayIndexedV3)); err != nil && !errors.Is(err, gonats.ErrKeyExists) {
		return fmt.Errorf("persist indexed delay layout marker: %w", err)
	}
	return t.validateDelayLayoutKV(topology, kv)
}

func (t *natsTransport) validateDelayLayoutKV(topology transport.Topology, kv gonats.KeyValue) error {
	if !delayLayoutIndexed(topology) {
		return nil
	}
	entry, err := kv.Get(delayLayoutMarkerKey)
	if err != nil {
		return fmt.Errorf("nats: indexed delay layout is not provisioned: %w", err)
	}
	if string(entry.Value()) != natsDelayIndexedV3 {
		return fmt.Errorf("nats: delay layout marker is %q, want %q", entry.Value(), natsDelayIndexedV3)
	}
	return nil
}
