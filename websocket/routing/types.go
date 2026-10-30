// Package routing provides opt-in, tenant-scoped realtime routing. Success means
// accepted by a gateway's outbound queue, never acknowledged by the client.
package routing

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrCapacity    = errors.New("websocket routing: registry capacity reached")
	ErrInvalid     = errors.New("websocket routing: invalid argument")
	ErrUnavailable = errors.New("websocket routing: unavailable")
	ErrStale       = errors.New("websocket routing: stale generation")
	ErrQueueFull   = errors.New("websocket routing: queue full")
	ErrClosed      = errors.New("websocket routing: session closed")
)

func NewID() string { return uuid.NewString() }

type Location struct {
	TenantID   string    `json:"tenant_id"`
	UserID     string    `json:"user_id"`
	SessionID  string    `json:"session_id"`
	NodeID     string    `json:"node_id"`
	ShardID    string    `json:"shard_id"`
	Generation string    `json:"generation"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func (l Location) Valid() bool {
	return l.TenantID != "" && l.UserID != "" && l.SessionID != "" && l.NodeID != "" && l.Generation != ""
}
func tuple(values ...string) string    { b, _ := json.Marshal(values); return string(b) }
func (l Location) Key() string         { return tuple(l.TenantID, l.UserID, l.SessionID) }
func (l Location) DeliveryKey() string { return tuple(l.TenantID, l.UserID, l.SessionID, l.Generation) }

type SessionRegistry interface {
	Register(context.Context, Location, time.Duration) (Location, error)
	Renew(context.Context, []Location, time.Duration) error
	Remove(context.Context, Location) error
	Resolve(context.Context, string, string, string) ([]Location, error)
	JoinRoom(context.Context, Location, string, time.Duration) error
	LeaveRoom(context.Context, Location, string) error
	ResolveRoom(context.Context, string, string) ([]string, error)
}

// RenewResult identifies the input generation and its individual renewal error.
type RenewResult struct {
	Location Location
	Err      error
}

// SessionLeaseRenewer reports one result per input, in order. A stale generation
// must not prevent other sessions from renewing. Implementations must not mutate ls.
type SessionLeaseRenewer interface {
	RenewBatch(context.Context, []Location, time.Duration) []RenewResult
}

// SessionRemover atomically removes a generation and its known room memberships.
// Removing an older generation must preserve a replacement session and its rooms.
type SessionRemover interface {
	RemoveSession(context.Context, Location, []string) error
}

type Status string

const (
	Enqueued    Status = "enqueued"
	NotFound    Status = "not_found"
	Stale       Status = "stale"
	QueueFull   Status = "queue_full"
	Unavailable Status = "unavailable"
	Unknown     Status = "unknown"
)

type Outcome struct {
	Location Location `json:"location"`
	Status   Status   `json:"status"`
}
type DeliveryResult struct {
	// Counts summarizes room recipients without an unbounded wire response.
	Counts   map[Status]int `json:"counts,omitempty"`
	Outcomes []Outcome      `json:"outcomes"`
	Offline  bool           `json:"offline,omitempty"`
}

func (r DeliveryResult) Retryable() bool {
	for _, o := range r.Outcomes {
		if o.Status != Enqueued {
			return true
		}
	}
	return false
}

type DeliveryCommand struct {
	Version       int        `json:"version"`
	MessageID     string     `json:"message_id"`
	CorrelationID string     `json:"correlation_id"`
	TenantID      string     `json:"tenant_id"`
	TargetNode    string     `json:"target_node"`
	Sessions      []Location `json:"sessions,omitempty"`
	RoomID        string     `json:"room_id,omitempty"`
	Payload       []byte     `json:"payload"`
	ExpiresAt     time.Time  `json:"expires_at"`
}
type DeliveryHandler func(context.Context, DeliveryCommand) DeliveryResult
type DeliveryTransport interface {
	Start(context.Context, string, DeliveryHandler) error
	Deliver(context.Context, string, DeliveryCommand) (DeliveryResult, error)
	Ready() bool
	Close(context.Context) error
}

// Publisher is implemented by the EventBus bridge without coupling core to it.
type BusinessCommand struct {
	MessageID string `json:"message_id"`
	Type      string `json:"type"`
	TenantID  string `json:"tenant_id"`
	UserID    string `json:"user_id"`
	AccountID string `json:"account_id,omitempty"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	Payload   []byte `json:"payload"`
}
type CommandPublisher interface {
	PublishCommand(context.Context, BusinessCommand) error
}

// RoomLeaseRenewer batches room lease refresh when the registry supports it.
type RoomMembership struct {
	Location Location
	RoomID   string
}
type RoomLeaseRenewer interface {
	RenewRooms(context.Context, []RoomMembership, time.Duration) error
}

func (r DeliveryResult) Count(status Status) int {
	if r.Counts != nil {
		return r.Counts[status]
	}
	n := 0
	for _, o := range r.Outcomes {
		if o.Status == status {
			n++
		}
	}
	return n
}
