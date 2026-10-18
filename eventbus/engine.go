package eventbus

import (
	"context"
	"errors"
	"fmt"
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

	state        int32
	ctx          context.Context // intake context: polling, reconnect, provisioning
	cancel       context.CancelFunc
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.RWMutex
	startDone    chan struct{}
	stopDone     chan struct{}
	stopErr      error
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
		state:           stateNew,
		ctx:             ctx,
		cancel:          cancel,
		workerCtx:       workerCtx,
		workerCancel:    workerCancel,
	}
}

func (e *engine) Register(jobType string, handler Handler, opts ...TopologyOption) error {
	if err := validateName("task name", jobType); err != nil {
		return err
	}
	if handler == nil {
		return ErrNilHandler
	}
	topo, err := e.buildTopology(TaskTopology, jobType, opts)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}
	if _, exists := e.eventTopologies[jobType]; exists {
		return fmt.Errorf("%w: %q is already an event", ErrTopologyKindConflict, jobType)
	}
	e.taskTopologies[jobType] = topo
	e.handlers[jobType] = handler
	return nil
}

func (e *engine) DeclareTask(jobType string, opts ...TopologyOption) error {
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}

	topo, err := e.buildTopology(TaskTopology, jobType, opts)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}
	if _, exists := e.eventTopologies[jobType]; exists {
		return fmt.Errorf("%w: %q is already an event", ErrTopologyKindConflict, jobType)
	}
	e.taskTopologies[jobType] = topo
	return nil
}

func (e *engine) HandleTask(jobType string, handler Handler) error {
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}

	if err := validateName("task name", jobType); err != nil {
		return err
	}
	if handler == nil {
		return ErrNilHandler
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}
	if _, exists := e.eventTopologies[jobType]; exists {
		return fmt.Errorf("%w: %q is already an event", ErrTopologyKindConflict, jobType)
	}
	if _, declared := e.taskTopologies[jobType]; !declared {
		return fmt.Errorf("%w: task %q", ErrUndeclaredTopology, jobType)
	}
	e.handlers[jobType] = handler
	return nil
}

func (e *engine) DeclareEvent(eventName string, opts ...TopologyOption) error {
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}

	topo, err := e.buildTopology(EventTopology, eventName, opts)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}
	if _, exists := e.taskTopologies[eventName]; exists {
		return fmt.Errorf("%w: %q is already a task", ErrTopologyKindConflict, eventName)
	}
	e.eventTopologies[eventName] = topo
	return nil
}

func (e *engine) Subscribe(eventName, handlerName string, handler Handler, opts ...TopologyOption) error {
	if atomic.LoadInt32(&e.state) != stateNew {
		return ErrAlreadyStarted
	}

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
		return ErrAlreadyStarted
	}
	if _, exists := e.taskTopologies[eventName]; exists {
		return fmt.Errorf("%w: %q is already a task", ErrTopologyKindConflict, eventName)
	}

	topo, declared := e.eventTopologies[eventName]
	if !declared {
		// Compatibility path: Subscribe historically declared the event. New
		// code should call DeclareEvent explicitly before adding subscribers.
		topo = Topology{Namespace: e.cfg.Namespace, JobType: eventName, Kind: EventTopology}
	} else if topo.Options != nil {
		cloned := make(map[string]any, len(topo.Options))
		for key, value := range topo.Options {
			cloned[key] = value
		}
		topo.Options = cloned
	}
	if err := applyTopologyOptions(&topo, opts); err != nil {
		return err
	}
	if err := validateTopology(topo, e.transport); err != nil {
		return err
	}
	if !declared {
		e.eventTopologies[eventName] = topo
	}

	if e.subscribers[eventName] == nil {
		e.subscribers[eventName] = make(map[string]Handler)
		e.subTopologies[eventName] = make(map[string]Topology)
	}
	e.subscribers[eventName][handlerName] = handler
	e.subTopologies[eventName][handlerName] = topo

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
	e.mu.Lock()
	switch atomic.LoadInt32(&e.state) {
	case stateStopping, stateStopped:
		e.mu.Unlock()
		return ErrBusClosed
	case stateStarting, stateRunning:
		e.mu.Unlock()
		return ErrAlreadyStarted
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	atomic.StoreInt32(&e.state, stateStarting)
	e.startDone = make(chan struct{})
	e.mu.Unlock()

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
		if err := e.transport.EnsureTopology(startCtx, topo); err != nil {
			return fmt.Errorf("ensure task topology for %s failed: %w", jobType, err)
		}
	}
	for eventName, topo := range e.eventTopologies {
		if err := e.transport.EnsureTopology(startCtx, topo); err != nil {
			return fmt.Errorf("ensure event topology for %s failed: %w", eventName, err)
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
		e.controls[consumerKey{kind: TaskTopology, name: jobType}] = newConsumerControl()
		e.wg.Add(1)
		go e.consumeLoop(TaskTopology, jobType, "", handler)
	}

	// Start consumers for Pub-Sub
	for eventName, handlers := range e.subscribers {
		for handlerName, handler := range handlers {
			e.controls[consumerKey{kind: EventTopology, name: eventName, group: handlerName}] = newConsumerControl()
			e.wg.Add(1)
			go e.consumeLoop(EventTopology, eventName, handlerName, handler)
		}
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
		// A session-local cancellation is reconnectable while the engine is
		// alive. Even a nil return needs backoff to avoid a hot reconnect loop.
		e.cfg.Logger.ErrorAsync("transport consume session ended", "job_type", jobType, "handler_name", handlerName, "err", err)
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
			metrics.IncReconnect(metricsName(jobType, handlerName))
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
					e.processDelivery(kind, jobType, handlerName, handler, delivery, maxAttempts)
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
				metrics.ObserveSaturation(metricsName(jobType, handlerName), time.Since(started).Seconds())
			}
		}()
		select {
		case intake <- delivery:
		case <-ctx.Done():
			return
		}
	}

	err := e.transport.Consume(ctx, sub, deliverFn)
	cancel()
	return err
}

func (e *engine) processDelivery(kind TopologyKind, jobType string, handlerName string, handler Handler, delivery transport.Delivery, maxAttempts int) {
	if e.workerCtx.Err() != nil {
		return
	}
	msg := delivery.Message()
	leaseInterval := time.Duration(0)
	if lease, ok := delivery.(transport.LeaseDelivery); ok {
		leaseInterval = lease.HeartbeatInterval()
		opCtx, cancel := context.WithTimeout(e.workerCtx, min(5*time.Second, leaseInterval))
		err := delivery.Heartbeat(opCtx)
		cancel()
		if err != nil {
			e.cfg.Logger.WarnAsync("delivery lease unavailable before processing", "err", err)
			return
		}
	}

	metricsJobType := metricsName(jobType, handlerName)
	e.cfg.Metrics.IncDequeue(metricsJobType)

	// Decode envelope
	env, err := e.codec.Decode(msg.Payload)
	if err != nil {
		e.cfg.Logger.ErrorAsync("failed to decode envelope, sending to DLQ", "id", msg.ID, "err", err)
		e.settleDelivery(delivery, transport.Decision{
			Kind:     transport.DecisionDeadLetter,
			Reason:   "decode_error",
			Cause:    err,
			Attempts: delivery.DeliveryAttempt(),
			FailedAt: time.Now(),
		}, metricsJobType, msg.ID)
		return
	}

	job := Job{env: env}
	env.Kind = kind
	// The envelope is authoritative: raw broker metadata may be missing or
	// stale. Preserve the original wire payload while restoring all keys.
	msg.ID = env.ID
	msg.Type = env.Type
	msg.OrderingKey = env.OrderingKey
	msg.DedupeKey = env.DedupeKey
	msg.Headers = cloneHeaders(env.Headers)
	e.observeQueueAge(metricsJobType, env)
	traceCtx, endSpan := e.cfg.Tracer.Start(e.workerCtx, "eventbus.process", cloneHeaders(env.Headers))
	defer endSpan()
	delivery = &observedDelivery{Delivery: delivery, message: msg, ctx: traceCtx}

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
			Kind:     transport.DecisionDeadLetter,
			Reason:   "poison_message",
			Cause:    errors.New("max attempts exceeded"),
			Attempts: currentAttempt,
			FailedAt: time.Now(),
		}, metricsJobType, job.ID())
		return
	}

	// Execution context with timeout
	procCtx, procCancel := context.WithTimeout(traceCtx, e.cfg.ProcessTimeout)

	// Setup Heartbeat (Bug C9)
	heartbeatDone := make(chan struct{})
	var heartbeatFailed atomic.Bool
	go func() {
		defer close(heartbeatDone)
		e.heartbeatLoop(procCtx, delivery, jobType, job.ID(), leaseInterval, func() { heartbeatFailed.Store(true); procCancel() })
	}()
	defer func() {
		procCancel()
		<-heartbeatDone
	}()

	// Execute handler
	start := time.Now()
	err = invokeHandler(procCtx, job, handler, e.cfg.Logger)
	duration := time.Since(start).Seconds()
	if errors.Is(err, ErrHandlerPanic) {
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			metrics.IncHandlerFailure(metricsJobType, "panic")
		}
		e.traceEvent(procCtx, "handler.panic", map[string]string{"outcome": "panic"})
	}
	if errors.Is(procCtx.Err(), context.DeadlineExceeded) {
		if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
			metrics.IncHandlerFailure(metricsJobType, "timeout")
		}
		e.traceEvent(procCtx, "handler.timeout", map[string]string{"outcome": "timeout"})
		err = context.DeadlineExceeded
	}
	procCancel()
	<-heartbeatDone

	e.cfg.Metrics.ObserveDuration(metricsJobType, duration)
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
	var skipErr *SkipError
	if errors.As(err, &skipErr) {
		e.settleDelivery(delivery, transport.Decision{Kind: transport.DecisionAck}, metricsJobType, job.ID())
		return
	}

	// Log the handler error for visibility
	e.cfg.Logger.ErrorAsync("eventbus: job handler returned error",
		"job_id", job.ID(),
		"type", jobType,
		"attempt", currentAttempt,
		"err", err.Error(),
	)

	// Check if outcome is Permanent
	var permErr *PermanentError
	if errors.As(err, &permErr) {
		e.settleDelivery(delivery, transport.Decision{
			Kind:     transport.DecisionDeadLetter,
			Reason:   "permanent_error",
			Cause:    permErr.Unwrap(),
			Attempts: currentAttempt,
			FailedAt: time.Now(),
		}, metricsJobType, job.ID())
		return
	}

	if currentAttempt >= limit {
		e.settleDelivery(delivery, transport.Decision{
			Kind:     transport.DecisionDeadLetter,
			Reason:   "max_attempts_exceeded",
			Cause:    err,
			Attempts: currentAttempt,
			FailedAt: time.Now(),
		}, metricsJobType, job.ID())
		return
	}

	backoff := e.cfg.RetryPolicy.NextBackoff(currentAttempt)
	retryCause := err
	var retryAfterErr *RetryAfterError
	if errors.As(err, &retryAfterErr) {
		backoff = retryAfterErr.After
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
		Kind:     transport.DecisionRetry,
		Next:     nextMsg,
		RetryAt:  retryAt,
		Cause:    retryCause,
		Attempts: currentAttempt,
	}, metricsJobType, job.ID())
}

func (e *engine) settleDelivery(delivery transport.Delivery, decision transport.Decision, metricsJobType, jobID string) {
	if e.workerCtx.Err() != nil {
		return
	}
	// Intake cancellation does not interrupt settlement; forced worker
	// cancellation does. Drivers must never ACK after a failed durable transfer.
	parent := e.workerCtx
	if observed, ok := delivery.(*observedDelivery); ok {
		parent = observed.ctx
		source := observed.Message()
		decision.Source = &source
	}
	spanCtx, endSpan := e.cfg.Tracer.Start(parent, "eventbus.settle."+decision.Kind.String(), cloneHeaders(delivery.Message().Headers))
	defer endSpan()
	opCtx, cancel := context.WithTimeout(spanCtx, 5*time.Second)
	defer cancel()

	started := time.Now()
	err := delivery.Settle(opCtx, decision)
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if metrics, ok := e.cfg.Metrics.(RuntimeMetrics); ok {
		metrics.ObserveSettlement(metricsJobType, decision.Kind.String(), outcome, time.Since(started).Seconds())
	}
	e.traceEvent(opCtx, "settlement", map[string]string{"decision": decision.Kind.String(), "outcome": outcome})
	if err != nil {
		e.cfg.Logger.ErrorAsync("eventbus: delivery settlement failed",
			"job_id", jobID,
			"decision", decision.Kind.String(),
			"reason", decision.Reason,
			"err", err,
		)
		return
	}
	e.cfg.Metrics.IncComplete(metricsJobType, decision.Kind.String())
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
				e.cfg.Logger.WarnAsync("failed to extend delivery heartbeat", "job_type", jobType, "id", jobID, "err", err)
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
	e.mu.Lock()
	if e.stopDone == nil {
		e.stopDone = make(chan struct{})
		atomic.StoreInt32(&e.state, stateStopping)
		e.cancel()
		go e.shutdown(ctx, e.startDone)
	}
	done := e.stopDone
	e.mu.Unlock()
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
	done := make(chan struct{})
	go func() {
		if startDone != nil {
			<-startDone
		}
		e.wg.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
		e.workerCancel()
		err = e.transport.Close(ctx)
	case <-ctx.Done():
		e.workerCancel()
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = errors.Join(ctx.Err(), e.transport.Close(closeCtx))
		cancel()
	}
	e.mu.Lock()
	e.stopErr = err
	atomic.StoreInt32(&e.state, stateStopped)
	close(e.stopDone)
	e.mu.Unlock()
}

func (e *engine) Enqueue(ctx context.Context, jobType string, payload any, opts ...JobOption) error {
	return e.publish(ctx, TaskTopology, jobType, payload, opts...)
}

func (e *engine) EnqueueBatch(ctx context.Context, jobType string, jobs []BatchJob) error {
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
		if err := e.publish(ctx, TaskTopology, jobType, job.Payload, job.Options...); err != nil {
			return &BatchError{Index: index, Err: err}
		}
	}
	return nil
}

func (e *engine) PublishEvent(ctx context.Context, eventName string, payload any, opts ...JobOption) error {
	return e.publish(ctx, EventTopology, eventName, payload, opts...)
}

func (e *engine) publish(ctx context.Context, kind TopologyKind, name string, payload any, opts ...JobOption) (result error) {
	if err := validateName("topology name", name); err != nil {
		return err
	}
	ctx, endSpan := e.cfg.Tracer.Start(ctx, "eventbus.publish", nil)
	defer endSpan()
	status := "success"
	defer func() {
		if result != nil {
			status = "error"
		}
		e.cfg.Metrics.IncEnqueue(name, status)
		e.traceEvent(ctx, "publish", map[string]string{"outcome": status})
	}()
	e.mu.RLock()
	var known bool
	var topo Topology
	var hasTopo bool
	if kind == TaskTopology {
		topo, hasTopo = e.taskTopologies[name]
		known = hasTopo
	} else {
		topo, hasTopo = e.eventTopologies[name]
		known = hasTopo
	}
	e.mu.RUnlock()

	if !hasTopo {
		topo = Topology{Namespace: e.cfg.Namespace, JobType: name, Kind: kind}
	}

	if !known {
		if !e.cfg.AutoRegister {
			return ErrUnknownJobType
		}
		// Lazy EnsureTopology for unknown types if AutoRegister is true (Bug C5)
		if err := e.transport.EnsureTopology(ctx, topo); err != nil {
			return fmt.Errorf("ensure topology: %w", err)
		}
	}

	maxAttempts := e.cfg.MaxAttempts
	if val, ok := topo.Options["eventbus.max_attempts"].(int); ok && val > 0 {
		maxAttempts = val
	}
	data, env, err := e.codec.EncodeTopology(kind, name, payload, maxAttempts, opts...)
	if err != nil {
		return err
	}
	env.Headers = cloneHeaders(env.Headers)
	if injector, ok := e.cfg.Tracer.(TraceInjector); ok {
		injector.Inject(ctx, env.Headers)
	}
	data, err = json.Marshal(env)
	if err != nil {
		return err
	}

	msg := transport.Message{
		ID:          env.ID,
		Type:        env.Type,
		Payload:     data,
		OrderingKey: env.OrderingKey,
		DedupeKey:   env.DedupeKey,
		Headers:     cloneHeaders(env.Headers),
	}

	if msg.OrderingKey != "" && !e.transport.Capabilities().Ordering {
		return ErrNotSupported
	}

	// Handle Deduplication (Bug C11)
	if msg.DedupeKey != "" {
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
			return ErrDedupeUnavailable
		}

		dedupeTTL := time.Duration(env.DedupeTTL)
		if dedupeTTL == 0 {
			dedupeTTL = 24 * time.Hour
		}
		acquired, err := reserve(ctx, msg.DedupeKey, dedupeTTL)
		if err != nil {
			return fmt.Errorf("dedupe reserve: %w", err)
		}
		if !acquired {
			status = "duplicate"
			return nil // Duplicate detected, skip silently or return specific err? Standard is skip silently for dedupe.
		}

		// Publish
		var pubErr error
		if env.RunAt > 0 {
			pubErr = e.transport.PublishAt(ctx, topo, msg, time.UnixMilli(env.RunAt))
		} else {
			pubErr = e.transport.Publish(ctx, topo, msg)
		}

		if pubErr != nil {
			// Release lock if publish fails so it can be retried
			_ = release(context.WithoutCancel(ctx), msg.DedupeKey)
			return pubErr
		}

		return nil
	}

	var pubErr error
	if env.RunAt > 0 {
		pubErr = e.transport.PublishAt(ctx, topo, msg, time.UnixMilli(env.RunAt))
	} else {
		pubErr = e.transport.Publish(ctx, topo, msg)
	}

	if pubErr != nil {
		return pubErr
	}

	return nil
}
