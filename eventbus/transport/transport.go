package transport

import (
	"context"
	"encoding/base32"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// ErrNotSupported is returned when a transport does not implement an optional
// capability. Callers should use errors.Is so wrapped errors remain stable.
var ErrNotSupported = errors.New("eventbus: operation not supported by transport")

// ErrLeaseLost means a delivery no longer owns its pending broker entry.
var ErrLeaseLost = errors.New("eventbus: delivery lease lost")

// ErrCapacityExceeded means a bounded transport resource cannot accept more
// durable work without violating its no-loss contract.
var ErrCapacityExceeded = errors.New("eventbus: transport capacity exceeded")

// CapacityError identifies the bounded broker resource that rejected
// admission. It intentionally excludes message payloads and credentials.
type CapacityError struct {
	Transport string
	Topology  string
	Resource  string
	Current   int64
	Limit     int64
}

func (e *CapacityError) Error() string {
	if e == nil {
		return ErrCapacityExceeded.Error()
	}
	if e.Topology == "" {
		return fmt.Sprintf("%s: %s capacity %d/%d: %v", e.Transport, e.Resource, e.Current, e.Limit, ErrCapacityExceeded)
	}
	return fmt.Sprintf("%s: topology %s %s capacity %d/%d: %v", e.Transport, e.Topology, e.Resource, e.Current, e.Limit, ErrCapacityExceeded)
}

func (e *CapacityError) Unwrap() error { return ErrCapacityExceeded }

// LeaseDelivery lets a driver request renewal before handler execution and a
// lease-specific heartbeat cadence. Renewal failure leaves the source unsettled.
type LeaseDelivery interface {
	HeartbeatInterval() time.Duration
}

var (
	// ErrDeliverySettled is returned when a terminal decision is attempted after
	// settlement has already succeeded.
	ErrDeliverySettled = errors.New("eventbus: delivery already settled")

	// ErrSettlementInProgress is returned when another goroutine is currently
	// settling the same delivery.
	ErrSettlementInProgress = errors.New("eventbus: delivery settlement in progress")

	// ErrInvalidDecision is returned for an unknown settlement decision kind.
	ErrInvalidDecision = errors.New("eventbus: invalid settlement decision")

	// ErrInvalidTopology is returned when a transport topology is malformed.
	ErrInvalidTopology = errors.New("eventbus: invalid transport topology")

	// ErrDedupeAmbiguous means a deduplication reservation cannot be proven to
	// represent a durably published message. Callers must not treat it as a
	// successful duplicate response.
	ErrDedupeAmbiguous = errors.New("eventbus: deduplication state is ambiguous")
)

const (
	// ResourceNameVersionOption selects the broker resource naming layout.
	ResourceNameVersionOption = "eventbus.resource_name_version"
	ResourceNameVersionV2     = "v2"
	ResourceNameVersionLegacy = "legacy"
)

// TopologyKind separates competing-consumer tasks from fan-out events.
type TopologyKind uint8

const (
	TaskTopology TopologyKind = iota
	EventTopology
)

func (k TopologyKind) String() string {
	if k == EventTopology {
		return "event"
	}
	return "task"
}

// Topology defines the naming structure for a specific job type.
// Different transports will use this to derive Stream/Topic and Group names.
type Topology struct {
	Namespace string
	JobType   string
	Kind      TopologyKind
	Options   map[string]any // Driver-specific options for this topology
}

// Validate checks transport-level topology invariants.
func (t Topology) Validate() error {
	if strings.TrimSpace(t.JobType) == "" || t.JobType != strings.TrimSpace(t.JobType) {
		return fmt.Errorf("%w: job type must not be blank or padded", ErrInvalidTopology)
	}
	if t.Kind != TaskTopology && t.Kind != EventTopology {
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidTopology, t.Kind)
	}
	return nil
}

// Name returns the canonical broker-facing topology name. The v2 default is
// an injective encoding of namespace, topology kind, and logical name.
func (t Topology) Name() string {
	return t.NameWithSeparator(".")
}

// NameWithSeparator returns the canonical topology name using sep between the
// version marker and an encoded tuple. The encoded tuple is a single component,
// so separators occurring in caller-controlled values cannot create aliases.
func (t Topology) NameWithSeparator(sep string) string {
	if t.resourceNameVersion() == ResourceNameVersionLegacy {
		return t.LegacyNameWithSeparator(sep)
	}
	return "eb2" + sep + encodeResourceTuple(t.Namespace, t.Kind.String(), t.JobType)
}

// ConsumerName returns an injective name for a topology consumer group.
func (t Topology) ConsumerName(group string) string {
	return t.ConsumerNameWithSeparator(group, ".")
}

// ConsumerNameWithSeparator is ConsumerName for brokers that reserve dots.
func (t Topology) ConsumerNameWithSeparator(group, sep string) string {
	if t.resourceNameVersion() == ResourceNameVersionLegacy {
		name := t.LegacyNameWithSeparator(sep) + sep + "group"
		if group != "" {
			name = t.LegacyNameWithSeparator(sep) + sep + group
		}
		return name
	}
	return t.NameWithSeparator(sep) + sep + "c" + sep + encodeResourceTuple(group)
}

// LegacyNameWithSeparator returns the pre-v2 separator-joined name. It exists
// only for explicit migrations and must not be used as a collision-safe name.
func (t Topology) LegacyNameWithSeparator(sep string) string {
	parts := make([]string, 0, 3)
	if t.Namespace != "" {
		parts = append(parts, t.Namespace)
	}
	if t.Kind == EventTopology {
		parts = append(parts, t.Kind.String())
	}
	parts = append(parts, t.JobType)
	return strings.Join(parts, sep)
}

func (t Topology) resourceNameVersion() string {
	if version, ok := t.Options[ResourceNameVersionOption].(string); ok && version == ResourceNameVersionLegacy {
		return version
	}
	return ResourceNameVersionV2
}

func encodeResourceTuple(components ...string) string {
	encoded := make([]byte, 0, 64)
	encoded = binary.AppendUvarint(encoded, uint64(len(components)))
	for _, component := range components {
		encoded = binary.AppendUvarint(encoded, uint64(len(component)))
		encoded = append(encoded, component...)
	}
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(encoded)
}

// Capabilities describes the features supported by a specific transport driver.
type Capabilities struct {
	DelayPrecision time.Duration // e.g., 1ms for Redis, 1s for Kafka delay tier
	Ordering       bool          // true if it supports partition/ordering keys (e.g., Kafka)
}

// Message is the data structure passed to the transport for publishing.
type Message struct {
	ID          string
	Type        string
	Payload     []byte // The serialized JSON Envelope
	OrderingKey string
	DedupeKey   string
	Headers     map[string]string
}

// DeadLetter contains information about a job that has permanently failed.
type DeadLetter struct {
	// ID is the transport-assigned administrative record identifier. It is
	// stable for the lifetime of the DLQ record and is used for requeue.
	ID             string
	Message        Message
	Reason         string
	LastError      string
	Attempts       int
	FailedAt       time.Time
	Source         string
	SourceGroup    string
	SourcePosition string
}

// DLQPage is one stable cursor page of dead-letter records. An empty cursor
// starts at the oldest record. NextCursor is empty when there is no next page.
type DLQPage struct {
	Records    []DeadLetter
	NextCursor string
}

// Subscription is passed to Consume to indicate what job type to listen for.
type Subscription struct {
	Topology Topology
	Group    string // The consumer group name. Empty for default work-queue.
	// Concurrency and Prefetch are the effective core intake bounds for this
	// subscription. Drivers use their sum to configure broker-side in-flight
	// limits. Zero means the direct transport caller did not provide a bound.
	Concurrency int
	Prefetch    int
	// WaitForHandlers is an optional session cleanup barrier supplied by the
	// engine. After stopping all callbacks, a consumer that owns settlement
	// resources must call it before releasing those resources. It is safe to
	// call repeatedly; no further deliver calls are allowed after calling it.
	WaitForHandlers func()
}

// SubscriptionProvisioner durably creates the broker-side consumer identity
// before a consumer session starts. The engine invokes it during Start after
// topology provisioning and before committing to the running state.
type SubscriptionProvisioner interface {
	EnsureSubscription(ctx context.Context, subscription Subscription) error
}

// TopologyStats holds metrics for a specific topology (job type).
type TopologyStats struct {
	Pending  int64 // ready delivery obligations not yet handed to a consumer
	Delayed  int64 // scheduled messages not yet ready
	InFlight int64 // delivered but unsettled delivery obligations
	DLQ      int64 // dead-letter records
	Lag      int64 // Pending + InFlight; excludes Delayed and DLQ
	// LagKnown and OldestAgeKnown distinguish unsupported measurements from 0.
	LagKnown       bool
	OldestAge      time.Duration
	OldestAgeKnown bool
}

// DLQAdmin is the optional Dead Letter Queue administrative interface.
// A transport must implement the complete contract before advertising it.
type DLQAdmin interface {
	// List provides legacy positional pagination. New code should prefer
	// CursorDLQAdmin when the transport implements it.
	List(ctx context.Context, topology Topology, offset, limit int) ([]DeadLetter, error)
	// messageIDs are DeadLetter.ID values, not application payload IDs.
	Requeue(ctx context.Context, topology Topology, messageIDs []string) error
	Purge(ctx context.Context, topology Topology) error
	Count(ctx context.Context, topology Topology) (int64, error)
}

// CursorDLQAdmin provides deletion-stable DLQ pagination. The opaque cursor
// returned by one page must be passed unchanged to the next call.
type CursorDLQAdmin interface {
	ListPage(ctx context.Context, topology Topology, cursor string, limit int) (DLQPage, error)
}

// DLQStore is retained as a source-compatible alias.
//
// Deprecated: use DLQAdmin.
type DLQStore = DLQAdmin

// Deduper provides an interface for deduplication if the transport natively supports it.
type Deduper interface {
	// Reserve attempts to acquire a lock for the given dedupe key.
	// Returns true if acquired, false if it already exists.
	Reserve(ctx context.Context, dedupeKey string, ttl time.Duration) (bool, error)
	// Release removes the lock, allowing the key to be used again (e.g., if publish fails).
	Release(ctx context.Context, dedupeKey string) error
}

// TopologyDeduper scopes reservations to namespace, topology kind and name.
// The engine prefers this interface over the legacy Deduper when available.
type TopologyDeduper interface {
	ReserveTopology(ctx context.Context, topology Topology, key string, ttl time.Duration) (bool, error)
	ReleaseTopology(ctx context.Context, topology Topology, key string) error
}

// DedupeReservationState is the durable state observed when beginning a
// publication protected by a dedupe key.
type DedupeReservationState uint8

const (
	DedupeReservationAcquired DedupeReservationState = iota + 1
	DedupeReservationPublished
	DedupeReservationPending
)

// PublicationDeduper distinguishes an unconfirmed reservation from proof that
// a prior publication was durably accepted. reservationID must uniquely own an
// acquired reservation; confirm/abort must not mutate another owner's state.
type PublicationDeduper interface {
	BeginPublication(ctx context.Context, topology Topology, key, reservationID string, ttl time.Duration) (DedupeReservationState, error)
	ConfirmPublication(ctx context.Context, topology Topology, key, reservationID string) error
	AbortPublication(ctx context.Context, topology Topology, key, reservationID string) error
}

// StatsReader is implemented by transports that can return real topology
// statistics. Unsupported transports must not return successful zero values.
type StatsReader interface {
	Stats(ctx context.Context, topology Topology) (TopologyStats, error)
}

// SubscriptionStatsReader returns one event subscriber group's obligations.
// It is separate from topology-wide event statistics, which aggregate groups.
type SubscriptionStatsReader interface {
	SubscriptionStats(ctx context.Context, subscription Subscription) (TopologyStats, error)
}

// Pauser is implemented by transports that support topology-level intake
// pause and resume operations.
type Pauser interface {
	Pause(ctx context.Context, topology Topology) error
	Resume(ctx context.Context, topology Topology) error
}

// TopologyValidator is implemented by transports with driver-specific
// topology options. EventBus invokes it before registering or provisioning a
// topology.
type TopologyValidator interface {
	ValidateTopology(topology Topology) error
}

// DecisionKind is the terminal outcome requested for a delivery.
type DecisionKind uint8

const (
	DecisionAck DecisionKind = iota + 1
	DecisionRetry
	DecisionDeadLetter
)

func (k DecisionKind) String() string {
	switch k {
	case DecisionAck:
		return "ack"
	case DecisionRetry:
		return "retry"
	case DecisionDeadLetter:
		return "dlq"
	default:
		return "unknown"
	}
}

// Decision describes the one terminal settlement requested for a delivery.
// Next is required by retry to carry the incremented envelope. RetryAt is an
// absolute time so drivers do not independently calculate retry deadlines.
type Decision struct {
	// Source carries envelope-normalized source metadata when decoding succeeded.
	// Drivers use it for DLQ records instead of their raw broker message. Nil
	// preserves raw metadata (for example, after a decode failure).
	Source  *Message
	Kind    DecisionKind
	Next    Message
	RetryAt time.Time
	Reason  string
	Cause   error
	// ErrorCode is a fixed, sanitized classification stored in logs and DLQ
	// metadata instead of rendering Cause. Empty values normalize to
	// "delivery_error" for direct transport callers.
	ErrorCode string
	Attempts  int
	FailedAt  time.Time
}

// SettlementGuard provides the shared single-use state machine for drivers.
// Failed operations reopen the guard so settlement can be retried; successful
// operations permanently close it.
type SettlementGuard struct {
	state atomic.Uint32
}

const (
	settlementOpen uint32 = iota
	settlementInProgress
	settlementDone
)

// Settle executes operation at most once successfully for this guard.
func (g *SettlementGuard) Settle(decision Decision, operation func() error) (err error) {
	if decision.Kind < DecisionAck || decision.Kind > DecisionDeadLetter {
		return fmt.Errorf("%w: kind %d", ErrInvalidDecision, decision.Kind)
	}
	if !g.state.CompareAndSwap(settlementOpen, settlementInProgress) {
		if g.state.Load() == settlementDone {
			return ErrDeliverySettled
		}
		return ErrSettlementInProgress
	}

	completed := false
	defer func() {
		if !completed {
			g.state.Store(settlementOpen)
		}
	}()

	if operation == nil {
		return fmt.Errorf("%w: nil operation", ErrInvalidDecision)
	}
	if err := operation(); err != nil {
		return err
	}

	g.state.Store(settlementDone)
	completed = true
	return nil
}

// Delivery represents a single message delivered to a consumer.
type Delivery interface {
	// Message returns the payload and keys.
	Message() Message

	// DeliveryAttempt returns the number of times this message has been delivered
	// by the transport, including current. Useful for poison message detection.
	DeliveryAttempt() int

	// Heartbeat extends the delivery lease where the transport supports one.
	Heartbeat(ctx context.Context) error

	// Settle applies exactly one terminal ACK, retry, or DLQ decision. A failed
	// settlement remains retryable and must leave the source eligible for redelivery.
	Settle(ctx context.Context, decision Decision) error
}

// Producer contains the required transport publishing operations.
type Producer interface {
	Publish(ctx context.Context, t Topology, m Message) error
	PublishAt(ctx context.Context, t Topology, m Message, runAt time.Time) error
}

// Consumer contains the required transport consumption operation.
type Consumer interface {
	// Consume invokes deliver synchronously (possibly applying backpressure).
	// Before returning, it must stop and join all session-owned goroutines and
	// callbacks. Topology-wide schedulers may instead live until Close.
	Consume(ctx context.Context, sub Subscription, deliver func(Delivery)) error
}

// Provisioner creates or updates broker resources for a topology.
type Provisioner interface {
	EnsureTopology(ctx context.Context, t Topology) error
}

// HealthChecker verifies that the transport can reach its backing service.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// Closer releases transport resources.
type Closer interface {
	Close(ctx context.Context) error
}

// Transport is the minimal interface every backend driver must implement.
// Administrative and deduplication features are exposed through the optional
// DLQAdmin, StatsReader, Deduper, and Pauser interfaces.
type Transport interface {
	Producer
	Consumer
	Provisioner
	HealthChecker
	Closer

	// Name returns the name of the transport driver.
	Name() string

	// Capabilities returns the features supported by this transport.
	Capabilities() Capabilities
}
