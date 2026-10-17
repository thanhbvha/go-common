package eventbus

import (
	"errors"
	"fmt"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

var (
	// ErrHandlerPanic identifies a recovered handler or middleware panic. Its
	// original value is deliberately excluded from errors, logs, and DLQ records.
	ErrHandlerPanic = errors.New("eventbus: panic in handler")

	// ErrUnknownJobType is returned when trying to enqueue a job type that has not been registered.
	ErrUnknownJobType = errors.New("eventbus: unknown job type")

	// ErrAlreadyStarted is returned when trying to register a handler after the bus has started.
	ErrAlreadyStarted = errors.New("eventbus: bus is already started")

	// ErrNotSupported is returned when a transport does not support an operation.
	// It aliases the transport-level sentinel so errors.Is works across layers.
	ErrNotSupported = transport.ErrNotSupported

	// ErrDedupeUnavailable is returned when deduplication is requested but no Deduper is configured.
	ErrDedupeUnavailable = fmt.Errorf("eventbus: deduplication not available: %w", ErrNotSupported)

	// ErrBusClosed is returned when an operation is performed on a stopped bus.
	ErrBusClosed = errors.New("eventbus: bus is closed")

	// ErrTopologyKindConflict is returned when one logical name is registered
	// as both a competing-consumer task and a fan-out event.
	ErrTopologyKindConflict = errors.New("eventbus: topology kind conflict")

	// ErrUndeclaredTopology is returned when a handler is attached to a task
	// that has not been explicitly declared.
	ErrUndeclaredTopology = errors.New("eventbus: topology is not declared")

	// ErrInvalidConfig is returned when bus-level configuration is invalid.
	ErrInvalidConfig = errors.New("eventbus: invalid config")

	// ErrInvalidName is returned when a topology or subscriber name is empty or malformed.
	ErrInvalidName = errors.New("eventbus: invalid name")

	// ErrNilHandler is returned when a nil handler is registered.
	ErrNilHandler = errors.New("eventbus: handler is nil")

	// ErrInvalidOption is returned when a job, topology, or driver option is invalid.
	ErrInvalidOption = errors.New("eventbus: invalid option")
)
