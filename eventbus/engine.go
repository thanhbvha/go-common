package eventbus

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	stateNew      int32 = 0
	stateRunning  int32 = 1
	stateStopping int32 = 2
	stateStopped  int32 = 3
	stateStarting int32 = 4

	maxConsecutiveConsumerPanics = 5
)

type engine struct {
	cfg             Config
	transport       transport.Transport
	codec           *Codec
	handlers        map[string]Handler
	subscribers     map[string]map[string]Handler
	taskTopologies  map[string]Topology
	eventTopologies map[string]Topology
	subTopologies   map[string]map[string]Topology
	controls        map[consumerKey]*consumerControl
	policies        map[consumerKey]*processingPolicy

	state        int32
	ctx          context.Context // intake context: polling, reconnect, provisioning
	cancel       context.CancelFunc
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.RWMutex
	operationMu  sync.RWMutex
	startDone    chan struct{}
	stopDone     chan struct{}
	stopErr      error
	panicCount   atomic.Uint64
	degraded     atomic.Bool
}

func (e *engine) recordInfrastructurePanic() {
	e.panicCount.Add(1)
	e.degraded.Store(true)
}

func (e *engine) recoveredPanicCount() uint64 { return e.panicCount.Load() }

func (e *engine) isDegraded() bool { return e.degraded.Load() }

func (e *engine) beginOperation() (func(), error) {
	e.operationMu.RLock()
	switch atomic.LoadInt32(&e.state) {
	case stateStopping, stateStopped:
		e.operationMu.RUnlock()
		return nil, ErrBusClosed
	default:
		return e.operationMu.RUnlock, nil
	}
}

func (e *engine) beginDeclaration() (func(), error) {
	release, err := e.beginOperation()
	if err != nil {
		return nil, err
	}
	if atomic.LoadInt32(&e.state) != stateNew {
		release()
		return nil, ErrAlreadyStarted
	}
	return release, nil
}

func lifecycleDeclarationError(state int32) error {
	if state == stateStopping || state == stateStopped {
		return ErrBusClosed
	}
	return ErrAlreadyStarted
}

func newEngine(cfg Config, t transport.Transport, codec *Codec) *engine {
	ctx, cancel := context.WithCancel(context.Background())
	workerCtx, workerCancel := context.WithCancel(context.Background())
	return &engine{
		cfg:             cfg,
		transport:       t,
		codec:           codec,
		handlers:        make(map[string]Handler),
		subscribers:     make(map[string]map[string]Handler),
		taskTopologies:  make(map[string]Topology),
		eventTopologies: make(map[string]Topology),
		subTopologies:   make(map[string]map[string]Topology),
		controls:        make(map[consumerKey]*consumerControl),
		policies:        make(map[consumerKey]*processingPolicy),
		state:           stateNew,
		ctx:             ctx,
		cancel:          cancel,
		workerCtx:       workerCtx,
		workerCancel:    workerCancel,
	}
}

func (e *engine) DeclareTask(jobType string, opts ...TaskOption) error {
	release, err := e.beginDeclaration()
	if err != nil {
		return err
	}
	defer release()

	topo, err := e.buildTopology(TaskTopology, jobType, opts)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return lifecycleDeclarationError(atomic.LoadInt32(&e.state))
	}
	if _, exists := e.eventTopologies[jobType]; exists {
		return fmt.Errorf("%w: %q is already an event", ErrTopologyKindConflict, jobType)
	}
	if existing, exists := e.taskTopologies[jobType]; exists {
		if reflect.DeepEqual(existing, topo) {
			return nil
		}
		return fmt.Errorf("%w: task %q", ErrConflictingDeclaration, jobType)
	}
	e.taskTopologies[jobType] = cloneTopology(topo)
	return nil
}

func (e *engine) HandleTask(jobType string, handler Handler) error {
	release, err := e.beginDeclaration()
	if err != nil {
		return err
	}
	defer release()

	if err := validateName("task name", jobType); err != nil {
		return err
	}
	if handler == nil {
		return ErrNilHandler
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return lifecycleDeclarationError(atomic.LoadInt32(&e.state))
	}
	if _, exists := e.eventTopologies[jobType]; exists {
		return fmt.Errorf("%w: %q is already an event", ErrTopologyKindConflict, jobType)
	}
	if _, declared := e.taskTopologies[jobType]; !declared {
		return fmt.Errorf("%w: task %q", ErrUndeclaredTopology, jobType)
	}
	if _, exists := e.handlers[jobType]; exists {
		return fmt.Errorf("%w: task %q", ErrDuplicateHandler, jobType)
	}
	e.handlers[jobType] = handler
	return nil
}

func (e *engine) DeclareEvent(eventName string, opts ...EventOption) error {
	release, err := e.beginDeclaration()
	if err != nil {
		return err
	}
	defer release()

	topo, err := e.buildTopology(EventTopology, eventName, opts)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return lifecycleDeclarationError(atomic.LoadInt32(&e.state))
	}
	if _, exists := e.taskTopologies[eventName]; exists {
		return fmt.Errorf("%w: %q is already a task", ErrTopologyKindConflict, eventName)
	}
	if existing, exists := e.eventTopologies[eventName]; exists {
		if reflect.DeepEqual(existing, topo) {
			return nil
		}
		return fmt.Errorf("%w: event %q", ErrConflictingDeclaration, eventName)
	}
	e.eventTopologies[eventName] = cloneTopology(topo)
	return nil
}

func (e *engine) Subscribe(eventName, handlerName string, handler Handler, opts ...SubscriberOption) error {
	release, err := e.beginDeclaration()
	if err != nil {
		return err
	}
	defer release()

	if err := validateName("event name", eventName); err != nil {
		return err
	}
	if err := validateName("subscriber name", handlerName); err != nil {
		return err
	}
	if handler == nil {
		return ErrNilHandler
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return lifecycleDeclarationError(atomic.LoadInt32(&e.state))
	}
	if _, exists := e.taskTopologies[eventName]; exists {
		return fmt.Errorf("%w: %q is already a task", ErrTopologyKindConflict, eventName)
	}

	if _, exists := e.subscribers[eventName][handlerName]; exists {
		return fmt.Errorf("%w: event %q subscriber %q", ErrDuplicateSubscriber, eventName, handlerName)
	}
	topo, declared := e.eventTopologies[eventName]
	if !declared {
		// Compatibility path: Subscribe historically declared the event. New
		// code should call DeclareEvent explicitly before adding subscribers.
		topo = Topology{Namespace: e.cfg.Namespace, JobType: eventName, Kind: EventTopology}
	}
	materialized, err := applySubscriberOptions(topo, opts)
	if err != nil {
		return err
	}
	if err := validateTopology(materialized, e.transport); err != nil {
		return err
	}
	if !declared {
		e.eventTopologies[eventName] = cloneTopology(topo)
	}

	if e.subscribers[eventName] == nil {
		e.subscribers[eventName] = make(map[string]Handler)
		e.subTopologies[eventName] = make(map[string]Topology)
	}
	e.subscribers[eventName][handlerName] = handler
	e.subTopologies[eventName][handlerName] = materialized

	return nil
}

func (e *engine) buildTopology(kind TopologyKind, name string, opts []TopologyOption) (Topology, error) {
	topo := Topology{Namespace: e.cfg.Namespace, JobType: name, Kind: kind}
	if err := applyTopologyOptions(&topo, opts); err != nil {
		return Topology{}, err
	}
	if err := validateTopology(topo, e.transport); err != nil {
		return Topology{}, err
	}
	return topo, nil
}

func (e *engine) Start(ctx context.Context) (err error) {
	e.operationMu.Lock()
	e.mu.Lock()
	switch atomic.LoadInt32(&e.state) {
	case stateStopping, stateStopped:
		e.mu.Unlock()
		e.operationMu.Unlock()
		return ErrBusClosed
	case stateStarting, stateRunning:
		e.mu.Unlock()
		e.operationMu.Unlock()
		return ErrAlreadyStarted
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		e.operationMu.Unlock()
		return err
	}
	if err := e.validateCapacityPlan(); err != nil {
		e.mu.Unlock()
		e.operationMu.Unlock()
		return err
	}
	atomic.StoreInt32(&e.state, stateStarting)
	e.startDone = make(chan struct{})
	e.mu.Unlock()
	e.operationMu.Unlock()

	// Stop can interrupt provisioning without waiting for the registry lock.
	startCtx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(e.ctx, cancel)
	defer func() {
		stopCancel()
		cancel()
		e.mu.Lock()
		if atomic.LoadInt32(&e.state) == stateStarting {
			atomic.StoreInt32(&e.state, stateNew)
		}
		close(e.startDone)
		e.mu.Unlock()
	}()

	for jobType, topo := range e.taskTopologies {
		if err := guardError(panicScopeProvisioning, "ensure task topology", nil, func() error { return e.transport.EnsureTopology(startCtx, topo) }); err != nil {
			return fmt.Errorf("ensure task topology for %s failed: %w", jobType, err)
		}
	}
	for eventName, topo := range e.eventTopologies {
		if err := guardError(panicScopeProvisioning, "ensure event topology", nil, func() error { return e.transport.EnsureTopology(startCtx, topo) }); err != nil {
			return fmt.Errorf("ensure event topology for %s failed: %w", eventName, err)
		}
	}
	if provisioner, ok := e.transport.(transport.SubscriptionProvisioner); ok {
		for jobType, topo := range e.taskTopologies {
			if err := guardError(panicScopeProvisioning, "ensure task subscription", nil, func() error { return provisioner.EnsureSubscription(startCtx, transport.Subscription{Topology: topo}) }); err != nil {
				return fmt.Errorf("ensure task subscription for %s failed: %w", jobType, err)
			}
		}
		for eventName, subscribers := range e.subTopologies {
			for group, topo := range subscribers {
				if err := guardError(panicScopeProvisioning, "ensure event subscription", nil, func() error {
					return provisioner.EnsureSubscription(startCtx, transport.Subscription{Topology: topo, Group: group})
				}); err != nil {
					return fmt.Errorf("ensure event subscription for %s/%s failed: %w", eventName, group, err)
				}
			}
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateStarting {
		return ErrBusClosed
	}
	if err := startCtx.Err(); err != nil {
		return err
	}
	atomic.StoreInt32(&e.state, stateRunning)

	// Start consumers for Work-Queue
	for jobType, handler := range e.handlers {
		key := consumerKey{kind: TaskTopology, name: jobType}
		e.controls[key] = newConsumerControl()
		e.policies[key] = newProcessingPolicy(e.taskTopologies[jobType])
		e.wg.Add(1)
		go e.runConsumerLoop(TaskTopology, jobType, "", handler)
	}

	// Start consumers for Pub-Sub
	for eventName, handlers := range e.subscribers {
		for handlerName, handler := range handlers {
			key := consumerKey{kind: EventTopology, name: eventName, group: handlerName}
			e.controls[key] = newConsumerControl()
			e.policies[key] = newProcessingPolicy(e.subTopologies[eventName][handlerName])
			e.wg.Add(1)
			go e.runConsumerLoop(EventTopology, eventName, handlerName, handler)
		}
	}

	return nil
}

func (e *engine) runConsumerLoop(kind TopologyKind, jobType, handlerName string, handler Handler) {
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = recover()
		e.recordInfrastructurePanic()
		e.cfg.Logger.ErrorAsync("eventbus: consumer supervisor panic recovered", "job_type", jobType, "handler_name", handlerName, "stack", capturePanicStack(4))
	}()
	e.consumeLoop(kind, jobType, handlerName, handler)
	completed = true
}

func (e *engine) validateCapacityPlan() error {
	topologies := len(e.taskTopologies) + len(e.eventTopologies)
	subscribers := 0
	for _, groups := range e.subscribers {
		subscribers += len(groups)
	}
	if e.cfg.MaxTopologies > 0 && topologies > e.cfg.MaxTopologies {
		return fmt.Errorf("%w: topology plan %d exceeds limit %d", ErrInvalidConfig, topologies, e.cfg.MaxTopologies)
	}
	if e.cfg.MaxSubscribers > 0 && subscribers > e.cfg.MaxSubscribers {
		return fmt.Errorf("%w: subscriber plan %d exceeds limit %d", ErrInvalidConfig, subscribers, e.cfg.MaxSubscribers)
	}
	return nil
}

func (e *engine) consumeLoop(kind TopologyKind, jobType string, handlerName string, handler Handler) {
	defer e.wg.Done()
	control := e.controlFor(consumerKey{kind: kind, name: jobType, group: handlerName})

	e.mu.RLock()
	var topo Topology
	if kind == TaskTopology {
		topo = e.taskTopologies[jobType]
	} else {
		topo = e.subTopologies[jobType][handlerName]
	}
	e.mu.RUnlock()

	sub := transport.Subscription{Topology: topo, Group: handlerName}
	backoff := ExponentialBackoff{
		Initial: 100 * time.Millisecond,
		Max:     30 * time.Second,
		Factor:  2,
		Jitter:  true,
	}
	consecutivePanics := 0
	for attempt := 1; e.ctx.Err() == nil; {
		if !control.waitReady(e.ctx) {
			return
		}
		started := time.Now()
		err := e.consumeSession(sub, handler)
		if e.ctx.Err() != nil || atomic.LoadInt32(&e.state) != stateRunning {
			return
		}
		if control.isPaused() {
			continue
		}
		if errors.Is(err, ErrRecoveredPanic) {
			consecutivePanics++
			e.recordInfrastructurePanic()
			if consecutivePanics >= maxConsecutiveConsumerPanics {
				e.cfg.Logger.ErrorAsync("eventbus: consumer stopped after panic storm", "job_type", jobType, "handler_name", handlerName)
				return
			}
		} else if time.Since(started) >= time.Minute {
			consecutivePanics = 0
		}
		// A session-local cancellation is reconnectable while the engine is
		// alive. Even a nil return needs backoff to avoid a hot reconnect loop.
		e.cfg.Logger.ErrorAsync("transport consume session ended", "job_type", jobType, "handler_name", handlerName, "error_type", safeErrorType(err))
		if time.Since(started) >= time.Minute {
			attempt = 1
		}
		timer := time.NewTimer(backoff.NextBackoff(attempt))
		select {
		case <-e.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if e.ctx.Err() != nil {
			return
		}
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			safeRuntimeMetric(func() { metrics.IncReconnect(metricsName(jobType, handlerName)) })
		}
		// Bound exponent calculation as well as the resulting delay.
		if attempt < 10 {
			attempt++
		}
	}
}

// consumeSession owns its workers and intake queue. No new session starts
// until all deliveries accepted by the previous session have finished.
func (e *engine) consumeSession(sub transport.Subscription, handler Handler) error {
	topo := sub.Topology
	kind, jobType, handlerName := topo.Kind, topo.JobType, sub.Group
	control := e.controlFor(consumerKey{kind: kind, name: jobType, group: handlerName})
	ctx, cancel, finish, ok := control.begin(e.ctx)
	if !ok {
		return context.Canceled
	}
	defer finish()
	defer cancel()

	// Each task/subscriber owns a fixed worker pool and a bounded intake queue.
	// A full queue blocks the synchronous transport callback, applying
	// backpressure without creating a goroutine for every delivery.
	concurrency := e.cfg.Concurrency
	if val, ok := topo.Options["eventbus.concurrency"].(int); ok && val > 0 {
		concurrency = val
	}
	prefetch := e.cfg.Prefetch
	if val, ok := topo.Options["eventbus.prefetch"].(int); ok && val > 0 {
		prefetch = val
	}

	maxAttempts := 0
	if val, ok := topo.Options["eventbus.max_attempts"].(int); ok && val > 0 {
		maxAttempts = val
	}

	if maxAttempts > 0 {
		e.cfg.Logger.InfoAsync("MaxAttempts override", "job_type", jobType, "max", maxAttempts)
	}
	sub.Concurrency = concurrency
	sub.Prefetch = prefetch

	intake := make(chan transport.Delivery, prefetch)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-e.workerCtx.Done():
					return
				case delivery, ok := <-intake:
					if !ok {
						return
					}
					if e.workerCtx.Err() != nil {
						return
					}
					delivery = withSessionCancel(delivery, cancel)
					panicked := guardCall(panicScopeWorker, "process delivery", func(record panicRecord) {
						e.cfg.Logger.ErrorAsync("eventbus: worker panic recovered",
							"job_type", jobType,
							"handler_name", handlerName,
							"stack", record.Stack,
						)
					}, func() {
						e.processDelivery(kind, jobType, handlerName, handler, delivery, maxAttempts)
					})
					if panicked {
						e.recordInfrastructurePanic()
						cancel()
						return
					}
				}
			}
		}()
	}
	var finishOnce sync.Once
	finishWorkers := func() {
		finishOnce.Do(func() {
			close(intake)
			workers.Wait()
		})
	}
	defer finishWorkers()
	sub.WaitForHandlers = finishWorkers

	deliverFn := func(delivery transport.Delivery) {
		if ctx.Err() != nil {
			return
		}
		select {
		case intake <- delivery:
			return
		default:
		}
		started := time.Now()
		defer func() {
			if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
				safeRuntimeMetric(func() { metrics.ObserveSaturation(metricsName(jobType, handlerName), time.Since(started).Seconds()) })
			}
		}()
		select {
		case intake <- delivery:
		case <-ctx.Done():
			return
		}
	}

	err := guardError(panicScopeConsumer, "transport consume", func(record panicRecord) {
		e.cfg.Logger.ErrorAsync("eventbus: consumer panic recovered",
			"job_type", jobType,
			"handler_name", handlerName,
			"stack", record.Stack,
		)
	}, func() error {
		return e.transport.Consume(ctx, sub, deliverFn)
	})
	cancel()
	return err
}

func (e *engine) processDelivery(kind TopologyKind, jobType string, handlerName string, handler Handler, delivery transport.Delivery, maxAttempts int) {
	if e.workerCtx.Err() != nil {
		return
	}
	msg := delivery.Message()
	metricsJobType := metricsName(jobType, handlerName)
	safeMetric(func() { e.cfg.Metrics.IncDequeue(metricsJobType) })
	maxPayloadBytes := e.maxPayloadBytesFor(consumerKey{kind: kind, name: jobType, group: handlerName})
	if len(msg.Payload) > maxPayloadBytes {
		err := payloadTooLargeError(len(msg.Payload), maxPayloadBytes)
		e.cfg.Logger.ErrorAsync("eventbus: oversized envelope, sending to DLQ", "id", msg.ID, "size", len(msg.Payload), "max", maxPayloadBytes)
		e.settleDelivery(delivery, transport.Decision{
			Kind:      transport.DecisionDeadLetter,
			Reason:    "payload_too_large",
			Cause:     err,
			ErrorCode: "payload_too_large",
			Attempts:  delivery.DeliveryAttempt(),
			FailedAt:  time.Now(),
		}, metricsJobType, msg.ID)
		return
	}
	leaseInterval := time.Duration(0)
	_, hasLease := delivery.(transport.LeaseDelivery)
	if lease, ok := delivery.(transport.LeaseDelivery); ok {
		leaseInterval = lease.HeartbeatInterval()
		opCtx, cancel := context.WithTimeout(e.workerCtx, min(5*time.Second, leaseInterval))
		err := guardError(panicScopeHeartbeat, "initial delivery heartbeat", func(record panicRecord) {
			e.recordInfrastructurePanic()
			if canceler, ok := delivery.(sessionCanceler); ok {
				canceler.cancelSession()
			}
			e.cfg.Logger.ErrorAsync("eventbus: heartbeat panic recovered", "job_type", jobType, "stack", record.Stack)
		}, func() error {
			return delivery.Heartbeat(opCtx)
		})
		cancel()
		if err != nil {
			e.cfg.Logger.WarnAsync("delivery lease unavailable before processing", "error_type", safeErrorType(err))
			return
		}
	}

	// Decode envelope
	env, err := e.codec.Decode(msg.Payload)
	if err != nil {
		e.cfg.Logger.ErrorAsync("failed to decode envelope, sending to DLQ", "id", msg.ID, "error_code", "decode_error")
		e.settleDelivery(delivery, transport.Decision{
			Kind:      transport.DecisionDeadLetter,
			Reason:    "decode_error",
			Cause:     err,
			ErrorCode: "decode_error",
			Attempts:  delivery.DeliveryAttempt(),
			FailedAt:  time.Now(),
		}, metricsJobType, msg.ID)
		return
	}
	if reason, validationErr := validateEnvelopeRoute(env, kind, jobType); validationErr != nil {
		e.cfg.Logger.ErrorAsync("invalid envelope, sending to DLQ", "id", msg.ID, "reason", reason)
		e.settleDelivery(delivery, transport.Decision{
			Kind: transport.DecisionDeadLetter, Reason: reason, ErrorCode: reason,
			Attempts: delivery.DeliveryAttempt(), FailedAt: time.Now(),
		}, metricsJobType, msg.ID)
		return
	}

	job := Job{env: env}
	// The envelope is authoritative: raw broker metadata may be missing or
	// stale. Preserve the original wire payload while restoring all keys.
	msg.ID = env.ID
	msg.Type = env.Type
	msg.OrderingKey = env.OrderingKey
	msg.ConcurrencyKey = env.ConcurrencyKey
	msg.DedupeKey = env.DedupeKey
	msg.Headers = cloneHeaders(env.Headers)
	e.observeQueueAge(metricsJobType, env)
	traceCtx, endSpan := safeTraceStart(e.cfg.Tracer, e.workerCtx, "eventbus.process", cloneHeaders(env.Headers))
	defer endSpan()
	delivery = &observedDelivery{Delivery: delivery, message: msg, ctx: traceCtx}
	policy := e.policyFor(consumerKey{kind: kind, name: jobType, group: handlerName})

	// Poison message guard (Bug C14)
	currentAttempt := job.Attempt()
	if delivery.DeliveryAttempt() > currentAttempt {
		currentAttempt = delivery.DeliveryAttempt()
		env.Attempt = currentAttempt
	}

	limit := job.MaxAttempts()
	if maxAttempts > 0 {
		limit = maxAttempts
	}
	env.MaxAttempts = limit

	if currentAttempt > limit {
		e.cfg.Logger.WarnAsync("poison message detected, max attempts exceeded", "id", job.ID(), "count", currentAttempt)
		e.settleDelivery(delivery, transport.Decision{
			Kind:      transport.DecisionDeadLetter,
			Reason:    "poison_message",
			Cause:     errors.New("max attempts exceeded"),
			ErrorCode: "poison_message",
			Attempts:  currentAttempt,
			FailedAt:  time.Now(),
		}, metricsJobType, job.ID())
		return
	}

	// Rate/key waits keep the lease alive but do not consume handler timeout.
	executionCtx := traceCtx
	executionCancel := context.CancelFunc(noopRelease)
	var heartbeatDone chan struct{}
	var heartbeatFailed atomic.Bool
	if hasLease {
		executionCtx, executionCancel = context.WithCancel(traceCtx)
		heartbeatDone = make(chan struct{})
		go func() {
			defer close(heartbeatDone)
			completed := false
			defer func() {
				if completed {
					return
				}
				_ = recover()
				heartbeatFailed.Store(true)
				e.recordInfrastructurePanic()
				executionCancel()
				if canceler, ok := delivery.(sessionCanceler); ok {
					canceler.cancelSession()
				}
				e.cfg.Logger.ErrorAsync("eventbus: heartbeat panic recovered", "job_type", jobType, "id", job.ID(), "stack", capturePanicStack(4))
			}()
			e.heartbeatLoop(executionCtx, delivery, jobType, job.ID(), leaseInterval, func() { heartbeatFailed.Store(true); executionCancel() })
			completed = true
		}()
		defer func() {
			executionCancel()
			<-heartbeatDone
		}()
	}
	releaseKey, err := policy.wait(executionCtx, env.ConcurrencyKey)
	if err != nil {
		return
	}
	defer releaseKey()

	// Execution context with timeout begins only when the handler can start.
	procCtx, procCancel := context.WithTimeout(executionCtx, e.cfg.ProcessTimeout)
	defer procCancel()

	// Execute handler
	start := time.Now()
	err = invokeHandler(procCtx, job, handler, e.cfg.Logger)
	duration := time.Since(start).Seconds()
	if safeIs(err, ErrHandlerPanic) {
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			safeRuntimeMetric(func() { metrics.IncHandlerFailure(metricsJobType, "panic") })
		}
		e.traceOutcome(procCtx, "handler.panic", "panic")
	}
	if errors.Is(procCtx.Err(), context.DeadlineExceeded) {
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			safeRuntimeMetric(func() { metrics.IncHandlerFailure(metricsJobType, "timeout") })
		}
		e.traceOutcome(procCtx, "handler.timeout", "timeout")
		err = context.DeadlineExceeded
	}
	releaseKey()
	if hasLease {
		executionCancel()
		<-heartbeatDone
	}

	safeMetric(func() { e.cfg.Metrics.ObserveDuration(metricsJobType, duration) })
	if heartbeatFailed.Load() {
		return
	}
	// Forced shutdown leaves the source unsettled for broker redelivery, even
	// when a handler ignores cancellation and later returns success.
	if e.workerCtx.Err() != nil {
		return
	}

	if err == nil {
		e.settleDelivery(delivery, transport.Decision{Kind: transport.DecisionAck}, metricsJobType, job.ID())
		return
	}

	// Check if outcome is Skip
	if _, ok := safeAs[*SkipError](err); ok {
		e.settleDelivery(delivery, transport.Decision{Kind: transport.DecisionAck}, metricsJobType, job.ID())
		return
	}

	// Log the handler error for visibility
	e.cfg.Logger.ErrorAsync("eventbus: job handler returned error",
		"job_id", job.ID(),
		"type", jobType,
		"attempt", currentAttempt,
		"error_code", safeErrorCode(err),
	)

	// Check if outcome is Permanent
	if _, ok := safeAs[*PermanentError](err); ok {
		e.settleDelivery(delivery, transport.Decision{
			Kind:      transport.DecisionDeadLetter,
			Reason:    "permanent_error",
			ErrorCode: "permanent_error",
			Attempts:  currentAttempt,
			FailedAt:  time.Now(),
		}, metricsJobType, job.ID())
		return
	}

	if currentAttempt >= limit {
		e.settleDelivery(delivery, transport.Decision{
			Kind:      transport.DecisionDeadLetter,
			Reason:    "max_attempts_exceeded",
			Cause:     err,
			ErrorCode: safeErrorCode(err),
			Attempts:  currentAttempt,
			FailedAt:  time.Now(),
		}, metricsJobType, job.ID())
		return
	}

	retryCause := err
	backoff := time.Duration(0)
	var retryPolicyErr error
	if retryAfterErr, ok := safeAs[*RetryAfterError](err); ok {
		backoff = normalizeRetryDelay(retryAfterErr.After)
	} else {
		backoff, retryPolicyErr = safeRetryBackoff(e.cfg.RetryPolicy, currentAttempt)
	}
	if retryPolicyErr != nil {
		e.cfg.Logger.ErrorAsync("eventbus: retry policy failed", "job_id", job.ID(), "error_code", safeErrorCode(retryPolicyErr))
		backoff = 0
		retryCause = retryPolicyErr
	}

	// Update envelope for the next attempt
	retryAt := time.Now().Add(backoff)
	env.Attempt = currentAttempt + 1
	env.RunAt = retryAt.UnixMilli()

	nextPayload, err := json.Marshal(env)
	if err != nil {
		e.cfg.Logger.ErrorAsync("failed to encode envelope for retry", "id", job.ID(), "err", err)
		return // leave un-acked, let transport redeliver
	}

	nextMsg := msg
	nextMsg.Payload = nextPayload
	nextMsg.Headers = cloneHeaders(env.Headers)

	// Transport must durably publish the retry before acknowledging the source.
	e.settleDelivery(delivery, transport.Decision{
		Kind:      transport.DecisionRetry,
		Next:      nextMsg,
		RetryAt:   retryAt,
		Cause:     retryCause,
		ErrorCode: safeErrorCode(retryCause),
		Attempts:  currentAttempt,
	}, metricsJobType, job.ID())
}

func (e *engine) settleDelivery(delivery transport.Delivery, decision transport.Decision, metricsJobType, jobID string) {
	if e.workerCtx.Err() != nil {
		return
	}
	// Intake cancellation does not interrupt settlement; forced worker
	// cancellation does. Drivers must never ACK after a failed durable transfer.
	parent := e.workerCtx
	var carrier Headers
	if observed, ok := delivery.(*observedDelivery); ok {
		parent = observed.ctx
		source := observed.Message()
		decision.Source = &source
		carrier = cloneHeaders(source.Headers)
	} else {
		carrier = cloneHeaders(delivery.Message().Headers)
	}
	spanCtx, endSpan := safeTraceStart(e.cfg.Tracer, parent, settlementSpanName(decision.Kind), carrier)
	defer endSpan()
	opCtx, cancel := context.WithTimeout(spanCtx, 5*time.Second)
	defer cancel()

	started := time.Now()
	decision.Reason, decision.ErrorCode = transport.NormalizeDecisionMetadata(decision)
	err := guardError(panicScopeSettlement, "delivery settlement", func(record panicRecord) {
		e.recordInfrastructurePanic()
		if canceler, ok := delivery.(sessionCanceler); ok {
			canceler.cancelSession()
		}
		e.cfg.Logger.ErrorAsync("eventbus: settlement panic recovered",
			"job_id", jobID,
			"decision", decision.Kind.String(),
			"stack", record.Stack,
		)
	}, func() error {
		return delivery.Settle(opCtx, decision)
	})
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
		safeRuntimeMetric(func() {
			metrics.ObserveSettlement(metricsJobType, decision.Kind.String(), outcome, time.Since(started).Seconds())
		})
	}
	e.traceSettlement(opCtx, decision.Kind.String(), outcome)
	if err != nil {
		e.cfg.Logger.ErrorAsync("eventbus: delivery settlement failed",
			"job_id", jobID,
			"decision", decision.Kind.String(),
			"reason", decision.Reason,
			"error_type", safeErrorType(err),
		)
		return
	}
	safeMetric(func() { e.cfg.Metrics.IncComplete(metricsJobType, decision.Kind.String()) })
}

func settlementSpanName(kind transport.DecisionKind) string {
	switch kind {
	case transport.DecisionAck:
		return "eventbus.settle.ack"
	case transport.DecisionRetry:
		return "eventbus.settle.retry"
	case transport.DecisionDeadLetter:
		return "eventbus.settle.dlq"
	default:
		return "eventbus.settle.unknown"
	}
}

func (e *engine) heartbeatLoop(ctx context.Context, delivery transport.Delivery, jobType, jobID string, leaseInterval time.Duration, leaseFailed func()) {
	// Heartbeat every ProcessTimeout / 3
	interval := e.cfg.ProcessTimeout / 3
	if interval < 1*time.Second {
		interval = 1 * time.Second
	}
	if leaseInterval > 0 {
		interval = min(interval, leaseInterval)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Extend visibility timeout
			opCtx, cancel := context.WithTimeout(ctx, min(5*time.Second, interval))
			if err := delivery.Heartbeat(opCtx); err != nil {
				// Normal handler completion cancels procCtx and any heartbeat request
				// already in flight. That cancellation is not a lease failure and must
				// not suppress settlement of the completed handler result.
				if ctx.Err() != nil {
					cancel()
					return
				}
				e.cfg.Logger.WarnAsync("failed to extend delivery heartbeat", "job_type", jobType, "id", jobID, "error_type", safeErrorType(err))
				if leaseInterval > 0 {
					cancel()
					leaseFailed()
					return
				}
			}
			cancel()
		}
	}
}

func (e *engine) Stop(ctx context.Context) error {
	return e.Drain(ctx)
}

func (e *engine) Drain(ctx context.Context) error {
	e.operationMu.Lock()
	e.mu.Lock()
	if e.stopDone == nil {
		e.stopDone = make(chan struct{})
		atomic.StoreInt32(&e.state, stateStopping)
		e.cancel()
		go e.shutdown(ctx, e.startDone)
	}
	done := e.stopDone
	e.mu.Unlock()
	e.operationMu.Unlock()
	// Once shutdown is complete, its recorded result wins even if this caller
	// supplies an already-canceled waiting context.
	select {
	case <-done:
		return e.stopErr
	default:
	}
	select {
	case <-done:
		return e.stopErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// The first shutdown caller owns cleanup; all later callers join it with their
// own waiting deadline. Provisioning and WaitGroup.Add must finish before Wait.
func (e *engine) shutdown(ctx context.Context, startDone <-chan struct{}) {
	var shutdownErr error
	completed := false
	defer func() {
		if !completed {
			_ = recover()
			shutdownErr = errors.Join(shutdownErr, newRecoveredPanicError(panicScopeShutdown, "shutdown"))
		}
		e.finishShutdown(shutdownErr)
	}()
	shutdownErr = e.performShutdown(ctx, startDone)
	completed = true
}

func (e *engine) performShutdown(ctx context.Context, startDone <-chan struct{}) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		completed := false
		defer func() {
			if completed {
				return
			}
			_ = recover()
			e.recordInfrastructurePanic()
		}()
		if startDone != nil {
			<-startDone
		}
		e.wg.Wait()
		completed = true
	}()
	select {
	case <-done:
		e.workerCancel()
		return guardError(panicScopeShutdown, "transport close", nil, func() error { return e.transport.Close(ctx) })
	case <-ctx.Done():
		e.workerCancel()
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		closeErr := guardError(panicScopeShutdown, "transport close", nil, func() error { return e.transport.Close(closeCtx) })
		return errors.Join(ctx.Err(), closeErr)
	}
}

func (e *engine) finishShutdown(err error) {
	e.mu.Lock()
	e.stopErr = err
	atomic.StoreInt32(&e.state, stateStopped)
	if e.stopDone != nil {
		close(e.stopDone)
	}
	e.mu.Unlock()
}

func (e *engine) Enqueue(ctx context.Context, jobType string, payload any, opts ...JobOption) error {
	release, err := e.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	_, err = e.publish(ctx, TaskTopology, jobType, payload, false, opts...)
	return err
}

func (e *engine) EnqueueWithResult(ctx context.Context, jobType string, payload any, opts ...JobOption) (PublicationResult, error) {
	release, err := e.beginOperation()
	if err != nil {
		return PublicationResult{}, err
	}
	defer release()
	return e.publish(ctx, TaskTopology, jobType, payload, true, opts...)
}

func (e *engine) EnqueueBatch(ctx context.Context, jobType string, jobs []BatchJob) error {
	release, err := e.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	if len(jobs) == 0 {
		return fmt.Errorf("%w: batch must contain at least one job", ErrInvalidOption)
	}
	if len(jobs) > MaxBatchJobs {
		return fmt.Errorf("%w: batch has %d jobs, maximum is %d", ErrInvalidOption, len(jobs), MaxBatchJobs)
	}
	for index, job := range jobs {
		if err := ctx.Err(); err != nil {
			return &BatchError{Index: index, Err: err}
		}
		if _, err := e.publish(ctx, TaskTopology, jobType, job.Payload, false, job.Options...); err != nil {
			return &BatchError{Index: index, Err: err}
		}
	}
	return nil
}

func (e *engine) EnqueueBatchWithResult(ctx context.Context, jobType string, jobs []BatchJob) (BatchPublicationResult, error) {
	release, err := e.beginOperation()
	if err != nil {
		return BatchPublicationResult{}, err
	}
	defer release()
	if len(jobs) == 0 {
		return BatchPublicationResult{}, fmt.Errorf("%w: batch must contain at least one job", ErrInvalidOption)
	}
	if len(jobs) > MaxBatchJobs {
		return BatchPublicationResult{}, fmt.Errorf("%w: batch has %d jobs, maximum is %d", ErrInvalidOption, len(jobs), MaxBatchJobs)
	}
	result := BatchPublicationResult{Results: make([]PublicationResult, 0, len(jobs))}
	for index, job := range jobs {
		if err := ctx.Err(); err != nil {
			return result, &BatchError{Index: index, Err: err}
		}
		published, err := e.publish(ctx, TaskTopology, jobType, job.Payload, true, job.Options...)
		if err != nil {
			return result, &BatchError{Index: index, Err: err}
		}
		result.Results = append(result.Results, published)
	}
	return result, nil
}

func (e *engine) PublishEvent(ctx context.Context, eventName string, payload any, opts ...JobOption) error {
	release, err := e.beginOperation()
	if err != nil {
		return err
	}
	defer release()
	_, err = e.publish(ctx, EventTopology, eventName, payload, false, opts...)
	return err
}

func (e *engine) PublishEventWithResult(ctx context.Context, eventName string, payload any, opts ...JobOption) (PublicationResult, error) {
	release, err := e.beginOperation()
	if err != nil {
		return PublicationResult{}, err
	}
	defer release()
	return e.publish(ctx, EventTopology, eventName, payload, true, opts...)
}

func (e *engine) publish(ctx context.Context, kind TopologyKind, name string, payload any, requireStableDuplicateID bool, opts ...JobOption) (publication PublicationResult, result error) {
	if err := validateName("topology name", name); err != nil {
		return PublicationResult{}, err
	}
	ctx, endSpan := safeTraceStart(e.cfg.Tracer, ctx, "eventbus.publish", nil)
	defer endSpan()
	status := "success"
	defer func() {
		if result != nil {
			status = "error"
		}
		safeMetric(func() { e.cfg.Metrics.IncEnqueue(name, status) })
		e.traceOutcome(ctx, "publish", status)
	}()
	e.mu.RLock()
	var topo Topology
	var hasTopo bool
	if kind == TaskTopology {
		topo, hasTopo = e.taskTopologies[name]
	} else {
		topo, hasTopo = e.eventTopologies[name]
	}
	e.mu.RUnlock()

	if !hasTopo {
		return PublicationResult{}, ErrUnknownJobType
	}

	maxAttempts := e.cfg.MaxAttempts
	if val, ok := topo.Options["eventbus.max_attempts"].(int); ok && val > 0 {
		maxAttempts = val
	}
	env, err := e.codec.buildEnvelope(kind, name, payload, maxAttempts, opts...)
	if err != nil {
		return PublicationResult{}, err
	}
	if env.RunAt > time.Now().UnixMilli() && !delayEnabled(topo) {
		return PublicationResult{}, ErrNotSupported
	}
	env.Headers = cloneHeaders(env.Headers)
	if injector, ok := e.cfg.Tracer.(TraceInjector); ok {
		if env.Headers == nil {
			env.Headers = make(Headers)
		}
		safeTraceInject(injector, ctx, env.Headers)
	}
	data, err := json.Marshal(env)
	if err != nil {
		return PublicationResult{}, err
	}
	maxPayloadBytes := e.cfg.MaxPayloadBytes
	if val, ok := topo.Options["eventbus.max_payload_bytes"].(int); ok && val > 0 {
		maxPayloadBytes = val
	}
	if len(data) > maxPayloadBytes {
		return PublicationResult{}, payloadTooLargeError(len(data), maxPayloadBytes)
	}

	msg := transport.Message{
		ID:             env.ID,
		Type:           env.Type,
		Payload:        data,
		OrderingKey:    env.OrderingKey,
		ConcurrencyKey: env.ConcurrencyKey,
		DedupeKey:      env.DedupeKey,
		Headers:        cloneHeaders(env.Headers),
	}

	if msg.OrderingKey != "" {
		capabilities, err := guardValue(panicScopePublish, "transport capabilities", nil, func() (transport.Capabilities, error) {
			return e.transport.Capabilities(), nil
		})
		if err != nil {
			return PublicationResult{}, err
		}
		if !capabilities.SupportsBrokerOrdering() {
			return PublicationResult{}, ErrNotSupported
		}
	}

	// Handle Deduplication (Bug C11)
	if msg.DedupeKey != "" {
		dedupeTTL := time.Duration(env.DedupeTTL)
		if dedupeTTL == 0 {
			dedupeTTL = 24 * time.Hour
		}
		if deduper, ok := e.transport.(transport.PublicationDeduperWithResult); ok {
			reservation, err := guardValue(panicScopeDedupe, "dedupe begin publication", nil, func() (transport.PublicationReservation, error) {
				return deduper.BeginPublicationWithResult(ctx, topo, msg.DedupeKey, msg.ID, dedupeTTL)
			})
			if err != nil {
				return PublicationResult{}, fmt.Errorf("dedupe begin: %w", err)
			}
			switch reservation.State {
			case transport.DedupeReservationPublished:
				if reservation.MessageID == "" {
					return PublicationResult{}, fmt.Errorf("%w: published reservation has no message ID", ErrDedupeAmbiguous)
				}
				status = "duplicate"
				return PublicationResult{ID: reservation.MessageID, Duplicate: true}, nil
			case transport.DedupeReservationPending:
				return PublicationResult{}, fmt.Errorf("%w: unconfirmed reservation", ErrDedupeAmbiguous)
			case transport.DedupeReservationAcquired:
			default:
				return PublicationResult{}, fmt.Errorf("%w: unknown reservation state %d", ErrDedupeAmbiguous, reservation.State)
			}
			pubErr := e.publishMessage(ctx, topo, msg, env.RunAt)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if pubErr != nil {
				if abortErr := guardError(panicScopeDedupe, "dedupe abort publication", nil, func() error { return deduper.AbortPublication(cleanupCtx, topo, msg.DedupeKey, msg.ID) }); abortErr != nil {
					return PublicationResult{}, errors.Join(pubErr, ErrDedupeAmbiguous, fmt.Errorf("dedupe cleanup: %w", abortErr))
				}
				return PublicationResult{}, pubErr
			}
			if err := guardError(panicScopeDedupe, "dedupe confirm publication", nil, func() error { return deduper.ConfirmPublication(cleanupCtx, topo, msg.DedupeKey, msg.ID) }); err != nil {
				return PublicationResult{}, errors.Join(ErrDedupeAmbiguous, fmt.Errorf("durable publish confirmation: %w", err))
			}
			return PublicationResult{ID: msg.ID}, nil
		}
		if deduper, ok := e.transport.(transport.PublicationDeduper); ok {
			state, err := guardValue(panicScopeDedupe, "dedupe begin publication", nil, func() (transport.DedupeReservationState, error) {
				return deduper.BeginPublication(ctx, topo, msg.DedupeKey, msg.ID, dedupeTTL)
			})
			if err != nil {
				return PublicationResult{}, fmt.Errorf("dedupe begin: %w", err)
			}
			switch state {
			case transport.DedupeReservationPublished:
				status = "duplicate"
				if requireStableDuplicateID {
					return PublicationResult{}, fmt.Errorf("%w: legacy deduper cannot return the original message ID", ErrDedupeAmbiguous)
				}
				return PublicationResult{Duplicate: true}, nil
			case transport.DedupeReservationPending:
				return PublicationResult{}, fmt.Errorf("%w: unconfirmed reservation", ErrDedupeAmbiguous)
			case transport.DedupeReservationAcquired:
			default:
				return PublicationResult{}, fmt.Errorf("%w: unknown reservation state %d", ErrDedupeAmbiguous, state)
			}

			pubErr := e.publishMessage(ctx, topo, msg, env.RunAt)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if pubErr != nil {
				if abortErr := guardError(panicScopeDedupe, "dedupe abort publication", nil, func() error { return deduper.AbortPublication(cleanupCtx, topo, msg.DedupeKey, msg.ID) }); abortErr != nil {
					return PublicationResult{}, errors.Join(pubErr, ErrDedupeAmbiguous, fmt.Errorf("dedupe cleanup: %w", abortErr))
				}
				return PublicationResult{}, pubErr
			}
			if err := guardError(panicScopeDedupe, "dedupe confirm publication", nil, func() error { return deduper.ConfirmPublication(cleanupCtx, topo, msg.DedupeKey, msg.ID) }); err != nil {
				return PublicationResult{}, errors.Join(ErrDedupeAmbiguous, fmt.Errorf("durable publish confirmation: %w", err))
			}
			return PublicationResult{ID: msg.ID}, nil
		}

		var reserve func(context.Context, string, time.Duration) (bool, error)
		var release func(context.Context, string) error
		if scoped, ok := e.transport.(transport.TopologyDeduper); ok {
			reserve = func(ctx context.Context, key string, ttl time.Duration) (bool, error) {
				return scoped.ReserveTopology(ctx, topo, key, ttl)
			}
			release = func(ctx context.Context, key string) error { return scoped.ReleaseTopology(ctx, topo, key) }
		} else if deduper, ok := e.transport.(transport.Deduper); ok {
			reserve, release = deduper.Reserve, deduper.Release
		} else {
			return PublicationResult{}, ErrDedupeUnavailable
		}

		acquired, err := guardValue(panicScopeDedupe, "dedupe reserve", nil, func() (bool, error) { return reserve(ctx, msg.DedupeKey, dedupeTTL) })
		if err != nil {
			return PublicationResult{}, fmt.Errorf("dedupe reserve: %w", err)
		}
		if !acquired {
			return PublicationResult{}, fmt.Errorf("%w: legacy deduper cannot prove durable publication", ErrDedupeAmbiguous)
		}

		// Publish
		pubErr := e.publishMessage(ctx, topo, msg, env.RunAt)

		if pubErr != nil {
			// Release lock if publish fails so it can be retried
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if cleanupErr := guardError(panicScopeDedupe, "dedupe release", nil, func() error { return release(cleanupCtx, msg.DedupeKey) }); cleanupErr != nil {
				return PublicationResult{}, errors.Join(pubErr, ErrDedupeAmbiguous, fmt.Errorf("dedupe cleanup: %w", cleanupErr))
			}
			return PublicationResult{}, pubErr
		}

		return PublicationResult{ID: msg.ID}, nil
	}

	pubErr := e.publishMessage(ctx, topo, msg, env.RunAt)

	if pubErr != nil {
		return PublicationResult{}, pubErr
	}

	return PublicationResult{ID: msg.ID}, nil
}

func (e *engine) publishMessage(ctx context.Context, topo Topology, msg transport.Message, runAt int64) error {
	if runAt > 0 {
		return guardError(panicScopePublish, "transport publish at", nil, func() error {
			return e.transport.PublishAt(ctx, topo, msg, time.UnixMilli(runAt))
		})
	}
	return guardError(panicScopePublish, "transport publish", nil, func() error {
		return e.transport.Publish(ctx, topo, msg)
	})
}

func payloadTooLargeError(size, limit int) error {
	return fmt.Errorf("%w: serialized envelope is %d bytes, maximum is %d", ErrPayloadTooLarge, size, limit)
}

func (e *engine) maxPayloadBytesFor(key consumerKey) int {
	limit := e.cfg.MaxPayloadBytes
	e.mu.RLock()
	var topo Topology
	if key.kind == TaskTopology {
		topo = e.taskTopologies[key.name]
	} else if subscribers := e.subTopologies[key.name]; subscribers != nil {
		topo = subscribers[key.group]
	}
	e.mu.RUnlock()
	if val, ok := topo.Options["eventbus.max_payload_bytes"].(int); ok && val > 0 {
		return val
	}
	return limit
}
