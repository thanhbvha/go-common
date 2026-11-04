// Package eventbus wraps EventBus handlers with a typed transactional inbox.
package eventbus

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/internal/eventbuswire"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
)

const (
	HeaderNamespace     = eventbuswire.HeaderNamespace
	HeaderTenant        = eventbuswire.HeaderTenant
	HeaderSchemaVersion = eventbuswire.HeaderSchemaVersion
	HeaderMessageID     = eventbuswire.HeaderMessageID
)

// TenantResolver must validate tenant identity against trusted application
// context. The untrusted message header alone is not an authorization boundary.
type TenantResolver func(context.Context, bus.Job) (string, error)
type Config struct {
	Namespace, Consumer string
	Tenant              TenantResolver
}

func WrapInbox[T any](processor inbox.Processor[T], cfg Config, handler func(context.Context, T, bus.Job) error) (bus.Handler, error) {
	if processor == nil || handler == nil || cfg.Tenant == nil || !messaging.ValidIdentity(cfg.Namespace, false) || !messaging.ValidIdentity(cfg.Consumer, false) {
		return nil, messaging.ErrInvalidConfig
	}
	return func(ctx context.Context, job bus.Job) error {
		tenant, err := cfg.Tenant(ctx, job)
		if err != nil {
			return err
		}
		if job.Header(HeaderNamespace) != cfg.Namespace || job.Header(HeaderTenant) != tenant {
			return messaging.ErrInvalidMessage
		}
		version, err := strconv.Atoi(job.Header(HeaderSchemaVersion))
		if err != nil {
			return messaging.ErrInvalidMessage
		}
		var payload json.RawMessage
		if err := job.Decode(&payload); err != nil {
			return messaging.ErrInvalidMessage
		}
		kind := messaging.Task
		if job.Kind() == bus.EventTopology {
			kind = messaging.Event
		}
		m := messaging.Message{ID: job.Header(HeaderMessageID), Namespace: cfg.Namespace, TenantID: tenant, Kind: kind, Type: job.Type(), SchemaVersion: version, Payload: payload}
		if job.ID() != eventbuswire.WireID(m) {
			return messaging.ErrInvalidMessage
		}
		fingerprint, err := m.Fingerprint()
		if err != nil {
			return err
		}
		_, err = processor.Consume(ctx, inbox.Receipt{Identity: m.Identity(), Consumer: cfg.Consumer, Fingerprint: fingerprint}, func(tx T) error { return handler(ctx, tx, job) })
		return err
	}, nil
}
