package eventbus

import (
	"context"
	"fmt"

	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Bus represents the public interface for the event bus.
type Bus interface {
	// DeclareTask declares a competing-consumer task topology. Producer-only
	// applications can enqueue after declaration without registering a handler.
	DeclareTask(name string, opts ...TopologyOption) error

	// HandleTask attaches a handler to an explicitly declared task topology.
	HandleTask(name string, handler Handler) error

	// Register registers a handler for a specific job type.
	// Deprecated: use DeclareTask followed by HandleTask.
	Register(jobType string, handler Handler, opts ...TopologyOption) error

	// DeclareEvent declares a fan-out event topology. Events can be published
	// without any local subscribers.
	DeclareEvent(name string, opts ...TopologyOption) error

	// Start provisions all declared topologies before starting consumers. A
	// provisioning failure can be retried. The context bounds startup only.
	// Concurrent starts return ErrAlreadyStarted; shutdown makes Start return
	// ErrBusClosed.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the bus.
	Stop(ctx context.Context) error

	// Drain stops intake and lets accepted deliveries finish handling and
	// settlement until ctx expires, then cancels workers. It closes the transport
	// after completion or forced cancellation. The first shutdown caller sets
	// the grace deadline; later callers only bound their own wait.
	Drain(ctx context.Context) error

	// Close shuts down consumers and closes the transport, including a bus
	// that has never started. Concurrent shutdown calls share one cleanup;
	// each caller can bound its own wait with ctx.
	Close(ctx context.Context) error

	// Enqueue adds a job to the queue.
	Enqueue(ctx context.Context, jobType string, payload any, opts ...JobOption) error

	// EnqueueBatch publishes tasks in input order. It is bounded and non-atomic:
	// on failure, earlier items may already be durable and later items are not
	// attempted. BatchError identifies the failed index.
	EnqueueBatch(ctx context.Context, jobType string, jobs []BatchJob) error

	// PublishEvent publishes an event to multiple subscribers.
	//
	// Deprecated: use Publish.
	PublishEvent(ctx context.Context, eventName string, payload any, opts ...JobOption) error

	// Publish publishes an event to its independent subscriber groups.
	Publish(ctx context.Context, eventName string, payload any, opts ...JobOption) error

	// Subscribe registers a handler for an event in a fan-out setup. For
	// compatibility it implicitly declares an undeclared event; new code should
	// call DeclareEvent first.
	Subscribe(eventName, handlerName string, handler Handler, opts ...TopologyOption) error

	// Stats returns the statistics for a job type.
	Stats(ctx context.Context, jobType string) (transport.TopologyStats, error)

	// Ping checks the health of the underlying transport.
	Ping(ctx context.Context) error

	// Pause stops new local intake for every consumer of a declared topology.
	// Accepted deliveries continue handling and settlement.
	Pause(ctx context.Context, name string) error

	// Resume restarts local intake for a paused topology.
	Resume(ctx context.Context, name string) error

	// DrainTopology pauses local intake and waits for all accepted deliveries
	// for that topology to finish. It does not close the bus or transport.
	DrainTopology(ctx context.Context, name string) error
}

type bus struct {
	cfg       Config
	transport transport.Transport
	codec     *Codec
	engine    *engine
}

// Option allows customizing the bus during creation.
type Option func(*bus)

// WithTransport explicitly sets the transport driver, bypassing the registry.
func WithTransport(t transport.Transport) Option {
	return func(b *bus) {
		b.transport = t
	}
}

// Open creates and initializes a new Bus using the provided configuration.
// It resolves the driver using the registry unless overridden by WithTransport.
func Open(cfg Config, opts ...Option) (Bus, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	b := &bus{
		cfg:   cfg,
		codec: NewCodec(nil),
	}

	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("%w: nil bus option", ErrInvalidOption)
		}
		opt(b)
	}

	if b.transport == nil {
		if cfg.Driver == "" {
			return nil, fmt.Errorf("eventbus: config Driver is empty and WithTransport was not provided")
		}
		t, err := registry.Get(cfg.Driver)
		if err != nil {
			return nil, err
		}
		b.transport = t
	}

	b.engine = newEngine(b.cfg, b.transport, b.codec)

	return b, nil
}

func (b *bus) Register(jobType string, handler Handler, opts ...TopologyOption) error {
	return b.engine.Register(jobType, handler, opts...)
}

func (b *bus) DeclareTask(name string, opts ...TopologyOption) error {
	return b.engine.DeclareTask(name, opts...)
}

func (b *bus) HandleTask(name string, handler Handler) error {
	return b.engine.HandleTask(name, handler)
}

func (b *bus) DeclareEvent(name string, opts ...TopologyOption) error {
	return b.engine.DeclareEvent(name, opts...)
}

func (b *bus) Start(ctx context.Context) error {
	return b.engine.Start(ctx)
}

func (b *bus) Stop(ctx context.Context) error {
	return b.engine.Stop(ctx)
}

func (b *bus) Close(ctx context.Context) error {
	return b.engine.Stop(ctx)
}

func (b *bus) Drain(ctx context.Context) error {
	return b.engine.Drain(ctx)
}

func (b *bus) Enqueue(ctx context.Context, jobType string, payload any, opts ...JobOption) error {
	return b.engine.Enqueue(ctx, jobType, payload, opts...)
}

func (b *bus) EnqueueBatch(ctx context.Context, jobType string, jobs []BatchJob) error {
	return b.engine.EnqueueBatch(ctx, jobType, jobs)
}

// PublishEvent publishes an event to multiple subscribers.
//
// Deprecated: use Publish.
func (b *bus) PublishEvent(ctx context.Context, eventName string, payload any, opts ...JobOption) error {
	return b.Publish(ctx, eventName, payload, opts...)
}

func (b *bus) Publish(ctx context.Context, eventName string, payload any, opts ...JobOption) error {
	return b.engine.PublishEvent(ctx, eventName, payload, opts...)
}

func (b *bus) Subscribe(eventName, handlerName string, handler Handler, opts ...TopologyOption) error {
	return b.engine.Subscribe(eventName, handlerName, handler, opts...)
}

func (b *bus) Stats(ctx context.Context, jobType string) (transport.TopologyStats, error) {
	if err := validateName("task name", jobType); err != nil {
		return transport.TopologyStats{}, err
	}
	topo := Topology{Namespace: b.cfg.Namespace, JobType: jobType, Kind: TaskTopology}
	reader, ok := b.transport.(transport.StatsReader)
	if !ok {
		return transport.TopologyStats{}, fmt.Errorf("eventbus: stats: %w", ErrNotSupported)
	}
	stats, err := reader.Stats(ctx, topo)
	if err == nil {
		if metrics, ok := b.cfg.Metrics.(RuntimeMetrics); ok {
			metrics.ObserveStats(jobType, stats)
		}
	}
	return stats, err
}

func (b *bus) Ping(ctx context.Context) error {
	if b.transport == nil {
		return ErrBusClosed
	}
	return b.transport.Ping(ctx)
}

func (b *bus) Pause(ctx context.Context, name string) error {
	return b.engine.Pause(ctx, name)
}

func (b *bus) Resume(ctx context.Context, name string) error {
	return b.engine.Resume(ctx, name)
}

func (b *bus) DrainTopology(ctx context.Context, name string) error {
	return b.engine.DrainTopology(ctx, name)
}
