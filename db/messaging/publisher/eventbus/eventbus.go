// Package eventbus publishes transactional outbox messages through EventBus.
package eventbus

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/internal/eventbuswire"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

const (
	HeaderNamespace     = eventbuswire.HeaderNamespace
	HeaderTenant        = eventbuswire.HeaderTenant
	HeaderSchemaVersion = eventbuswire.HeaderSchemaVersion
	HeaderMessageID     = eventbuswire.HeaderMessageID
)

type Config struct {
	// Namespace must match both the store and the configured EventBus namespace.
	Namespace string
	// DedupeTTL opts in to broker-confirmed producer dedupe. Zero disables it.
	DedupeTTL time.Duration
}
type Publisher struct {
	bus          bus.Bus
	cfg          Config
	capabilities transport.Capabilities
}

func NewPublisher(b bus.Bus, cfg Config) (*Publisher, error) {
	if b == nil || !messaging.ValidIdentity(cfg.Namespace, false) || cfg.DedupeTTL < 0 {
		return nil, messaging.ErrInvalidConfig
	}
	cp, ok := b.(bus.CapabilityProvider)
	if !ok {
		return nil, bus.ErrNotSupported
	}
	c, err := cp.Capabilities()
	if err != nil {
		return nil, err
	}
	if c.Durability != transport.DurabilityBroker || (cfg.DedupeTTL > 0 && c.Dedupe != transport.DedupeBrokerConfirmed) {
		return nil, bus.ErrNotSupported
	}
	return &Publisher{bus: b, cfg: cfg, capabilities: transport.NormalizeCapabilities(c)}, nil
}
func (p *Publisher) Publish(ctx context.Context, m messaging.Message) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if m.Namespace != p.cfg.Namespace {
		return messaging.ErrInvalidMessage
	}
	if m.OrderingKey != "" && !p.capabilities.SupportsBrokerOrdering() {
		return bus.ErrNotSupported
	}
	// Some brokers (notably JetStream) deduplicate IDs even without an explicit
	// dedupe option. Scope the wire identity so equal business IDs in separate
	// tenants cannot suppress each other. The original ID travels unchanged.
	wireID := WireID(m)
	options := []bus.JobOption{bus.WithMessageID(wireID), bus.WithConcurrencyKey(m.ConcurrencyKey)}
	if m.OrderingKey != "" {
		options = append(options, bus.WithBrokerOrderingKey(m.OrderingKey))
	}
	for k, v := range m.Headers {
		if strings.HasPrefix(k, "transactional.") {
			return messaging.ErrInvalidMessage
		}
		options = append(options, bus.WithHeader(k, v))
	}
	options = append(options, bus.WithHeader(HeaderNamespace, m.Namespace), bus.WithHeader(HeaderTenant, m.TenantID), bus.WithHeader(HeaderSchemaVersion, strconv.Itoa(m.SchemaVersion)), bus.WithHeader(HeaderMessageID, m.ID))
	if p.cfg.DedupeTTL > 0 {
		// Namespace, tenant, topology and ID all participate in reservation identity.
		options = append(options, bus.WithDedupeKey(wireID), bus.WithDedupeTTL(p.cfg.DedupeTTL))
	}
	var result bus.PublicationResult
	var err error
	if m.Kind == messaging.Task {
		result, err = p.bus.EnqueueWithResult(ctx, m.Type, m.Payload, options...)
	} else {
		result, err = p.bus.PublishWithResult(ctx, m.Type, m.Payload, options...)
	}
	if err != nil {
		return err
	}
	if result.ID != wireID {
		return messaging.ErrIdentityConflict
	}
	return nil
}

// WireID returns the stable broker identity; HeaderMessageID retains the business ID.
func WireID(m messaging.Message) string { return eventbuswire.WireID(m) }

var _ outbox.Publisher = (*Publisher)(nil)
