package registry

import (
	"errors"
	"fmt"
	"sync"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

var ErrFactoryPanic = errors.New("eventbus: transport factory panic")

// Factory is a function that creates a new Transport driver instance.
type Factory func() (transport.Transport, error)

var (
	mu        sync.RWMutex
	factories = make(map[string]Factory)
)

// Register registers a new transport driver factory.
// This should be called from the init() function of the driver package.
func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	if factory == nil {
		panic("eventbus: Register factory is nil")
	}
	if _, dup := factories[name]; dup {
		panic("eventbus: Register called twice for factory " + name)
	}
	factories[name] = factory
}

// Get creates a new transport instance by its registered name.
func Get(name string) (instance transport.Transport, err error) {
	mu.RLock()
	factory, ok := factories[name]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("eventbus: unknown driver %q (forgot to import?)", name)
	}

	completed := false
	defer func() {
		if !completed {
			_ = recover()
			instance = nil
			err = ErrFactoryPanic
		}
	}()
	instance, err = factory()
	completed = true
	return instance, err
}
