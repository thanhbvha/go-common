package eventbus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type consumerKey struct {
	kind  TopologyKind
	name  string
	group string
}

type consumerControl struct {
	mu         sync.Mutex
	paused     bool
	changed    chan struct{}
	cancel     context.CancelFunc
	idle       chan struct{}
	generation uint64
}

func newConsumerControl() *consumerControl {
	return &consumerControl{changed: make(chan struct{})}
}

func (c *consumerControl) waitReady(ctx context.Context) bool {
	for {
		c.mu.Lock()
		if !c.paused {
			c.mu.Unlock()
			return ctx.Err() == nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

func (c *consumerControl) begin(parent context.Context) (context.Context, context.CancelFunc, func(), bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.paused || parent.Err() != nil {
		return nil, func() {}, func() {}, false
	}
	ctx, cancel := context.WithCancel(parent)
	c.generation++
	generation := c.generation
	c.cancel = cancel
	c.idle = make(chan struct{})
	var once sync.Once
	finish := func() {
		once.Do(func() {
			c.mu.Lock()
			if c.generation == generation {
				c.cancel = nil
				close(c.idle)
				c.idle = nil
			}
			c.mu.Unlock()
		})
	}
	return ctx, cancel, finish, true
}

func (c *consumerControl) pause() <-chan struct{} {
	c.mu.Lock()
	if !c.paused {
		c.paused = true
		c.signalLocked()
	}
	cancel := c.cancel
	idle := c.idle
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if idle == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return idle
}

func (c *consumerControl) resume() {
	c.mu.Lock()
	if c.paused {
		c.paused = false
		c.signalLocked()
	}
	c.mu.Unlock()
}

func (c *consumerControl) isPaused() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

func (c *consumerControl) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

func (e *engine) controlFor(key consumerKey) *consumerControl {
	e.mu.Lock()
	defer e.mu.Unlock()
	control := e.controls[key]
	if control == nil {
		control = newConsumerControl()
		e.controls[key] = control
	}
	return control
}

func (e *engine) topologyControls(name string) ([]*consumerControl, error) {
	if err := validateName("topology name", name); err != nil {
		return nil, err
	}
	if atomic.LoadInt32(&e.state) != stateRunning {
		return nil, ErrBusNotRunning
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	controls := make([]*consumerControl, 0)
	if _, exists := e.taskTopologies[name]; exists {
		if control := e.controls[consumerKey{kind: TaskTopology, name: name}]; control != nil {
			controls = append(controls, control)
		}
	} else if _, exists := e.eventTopologies[name]; exists {
		for group := range e.subscribers[name] {
			if control := e.controls[consumerKey{kind: EventTopology, name: name, group: group}]; control != nil {
				controls = append(controls, control)
			}
		}
	} else {
		return nil, fmt.Errorf("%w: topology %q", ErrUnknownJobType, name)
	}
	if len(controls) == 0 {
		return nil, fmt.Errorf("eventbus: topology %q has no local consumers: %w", name, transport.ErrNotSupported)
	}
	return controls, nil
}

func (e *engine) Pause(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	controls, err := e.topologyControls(name)
	if err != nil {
		return err
	}
	for _, control := range controls {
		control.pause()
	}
	return nil
}

func (e *engine) Resume(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	controls, err := e.topologyControls(name)
	if err != nil {
		return err
	}
	for _, control := range controls {
		control.resume()
	}
	return nil
}

func (e *engine) DrainTopology(ctx context.Context, name string) error {
	controls, err := e.topologyControls(name)
	if err != nil {
		return err
	}
	idle := make([]<-chan struct{}, 0, len(controls))
	for _, control := range controls {
		idle = append(idle, control.pause())
	}
	for _, done := range idle {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
