// Package eventbus bridges the existing EventBus and realtime routing. The bus
// and router are borrowed: declare this bridge before starting the shared bus.
package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	bus "github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/thanhbvha/go-common/websocket/routing"
)

type Notification struct {
	MessageID string `json:"message_id"`
	TenantID  string `json:"tenant_id"`
	UserID    string `json:"user_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	RoomID    string `json:"room_id,omitempty"`
	Payload   []byte `json:"payload"`
}
type Mapping struct {
	Event   string
	Options []bus.EventOption
	Map     func(context.Context, bus.Job) (Notification, error)
}

// PublicationDedupePolicy controls producer-side confirmed deduplication.
// It does not disable gateway deduplication or provide business idempotency.
type PublicationDedupePolicy int

const (
	// DedupeRequired preserves confirmed deduplication for existing callers.
	DedupeRequired PublicationDedupePolicy = iota
	// DedupeDisabled keeps stable IDs but permits duplicate publications. Use
	// application idempotency/inbox handling with drivers such as Kafka.
	DedupeDisabled
)

type Config struct {
	Bus                 bus.Bus
	PublicationDedupe   PublicationDedupePolicy
	Router              *routing.Router
	CommandTask         string
	CommandOptions      []bus.TaskOption
	NotificationEvent   string
	NotificationOptions []bus.EventOption
	Mappings            []Mapping
}
type Bridge struct{ cfg Config }

// New declares topologies and registers the shared realtime-router subscriber.
// Explicit driver event-layout options belong in NotificationOptions/Mappings.
func New(c Config) (*Bridge, error) {
	if c.Bus == nil {
		return nil, routing.ErrInvalid
	}
	if c.PublicationDedupe != DedupeRequired && c.PublicationDedupe != DedupeDisabled {
		return nil, fmt.Errorf("%w: unknown publication dedupe policy", routing.ErrInvalid)
	}
	if c.PublicationDedupe == DedupeRequired {
		if provider, ok := c.Bus.(bus.CapabilityProvider); ok {
			capabilities, err := provider.Capabilities()
			if err != nil {
				return nil, err
			}
			if capabilities.Dedupe == transport.DedupeNone {
				return nil, fmt.Errorf("bridge requires confirmed publication dedupe: %w", bus.ErrDedupeUnavailable)
			}
		}
	}
	if c.CommandTask == "" {
		c.CommandTask = "ws.command"
	}
	if c.NotificationEvent == "" {
		c.NotificationEvent = "ws.notification"
	}
	b := &Bridge{cfg: c}
	if err := c.Bus.DeclareTask(c.CommandTask, c.CommandOptions...); err != nil {
		return nil, err
	}
	if err := c.Bus.DeclareEvent(c.NotificationEvent, c.NotificationOptions...); err != nil {
		return nil, err
	}
	if c.Router != nil {
		mappings := append([]Mapping{{Event: c.NotificationEvent, Map: func(_ context.Context, j bus.Job) (Notification, error) {
			var n Notification
			err := j.Decode(&n)
			return n, err
		}}}, c.Mappings...)
		for _, m := range mappings {
			if m.Event == "" || m.Map == nil {
				return nil, routing.ErrInvalid
			}
			if m.Event != c.NotificationEvent {
				if err := c.Bus.DeclareEvent(m.Event, m.Options...); err != nil {
					return nil, err
				}
			}
			mapper := m.Map
			if err := c.Bus.Subscribe(m.Event, "realtime-router", func(ctx context.Context, j bus.Job) error {
				n, err := mapper(ctx, j)
				if err != nil {
					return bus.MarkPermanent(err)
				}
				if n.MessageID == "" {
					n.MessageID = j.ID()
				}
				return b.route(ctx, n)
			}); err != nil {
				return nil, err
			}
		}
	}
	return b, nil
}
func (b *Bridge) PublishCommand(ctx context.Context, c routing.BusinessCommand) error {
	if c.MessageID == "" {
		c.MessageID = routing.NewID()
	}
	if c.Type == "" || c.UserID == "" || c.TenantID == "" || c.SessionID == "" || !json.Valid(c.Payload) {
		return routing.ErrInvalid
	}
	options := append(b.publicationOptions(c.MessageID), bus.WithHeader("request_id", c.RequestID))
	return b.cfg.Bus.Enqueue(ctx, b.cfg.CommandTask, c, options...)
}
func (b *Bridge) PublishNotification(ctx context.Context, n Notification) error {
	if err := validate(n); err != nil {
		return err
	}
	if n.MessageID == "" {
		n.MessageID = routing.NewID()
	}
	return b.cfg.Bus.Publish(ctx, b.cfg.NotificationEvent, n, b.publicationOptions(n.MessageID)...)
}

func (b *Bridge) publicationOptions(id string) []bus.JobOption {
	options := []bus.JobOption{bus.WithMessageID(id)}
	if b.cfg.PublicationDedupe == DedupeRequired {
		options = append(options, bus.WithDedupeKey(id))
	}
	return options
}
func validate(n Notification) error {
	if n.TenantID == "" || (n.RoomID == "" && n.UserID == "") || (n.RoomID != "" && (n.UserID != "" || n.SessionID != "")) {
		return routing.ErrInvalid
	}
	return nil
}
func (b *Bridge) route(ctx context.Context, n Notification) error {
	if err := validate(n); err != nil {
		return bus.MarkPermanent(err)
	}
	var res routing.DeliveryResult
	var err error
	switch {
	case n.RoomID != "":
		res, err = b.cfg.Router.SendToRoom(ctx, n.TenantID, n.RoomID, n.Payload, n.MessageID)
	case n.SessionID != "":
		res, err = b.cfg.Router.SendToSession(ctx, n.TenantID, n.UserID, n.SessionID, n.Payload, n.MessageID)
	default:
		res, err = b.cfg.Router.SendToUser(ctx, n.TenantID, n.UserID, n.Payload, n.MessageID)
	}
	if errors.Is(err, routing.ErrInvalid) {
		return bus.MarkPermanent(err)
	}
	if err != nil {
		return err
	}
	if res.Retryable() {
		return fmt.Errorf("websocket delivery incomplete: %w", routing.ErrUnavailable)
	}
	return nil
}
