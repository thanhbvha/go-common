// Package orm provides instance-scoped GORM clients, bounded repositories and
// explicit transaction scopes. PostgreSQL, MySQL and SQLite adapters expose their
// verified capabilities. Open never runs migrations or silently emulates features.
package orm

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"unicode"
)

// Manager's zero value is usable. Do not copy after use. Lifecycle changes are
// serialized; lookups never wait on network I/O. Close is terminal.
type Manager struct {
	once        sync.Once
	gate        chan struct{}
	mu          sync.RWMutex
	clients     map[string]*Client
	defaultName string
	initialized bool
	closed      bool
	open        func(context.Context, Config) (*Client, error) // test seam
}

func NewManager() *Manager { return &Manager{} }
func (m *Manager) lock(ctx context.Context) error {
	m.once.Do(func() { m.gate = make(chan struct{}, 1); m.gate <- struct{}{} })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.gate:
		if err := ctx.Err(); err != nil {
			m.unlock()
			return err
		}
		return nil
	}
}
func (m *Manager) unlock() { m.gate <- struct{}{} }
func validName(s string) bool {
	if s == "" || len(s) > 128 || strings.TrimSpace(s) != s {
		return false
	}
	for _, c := range s {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}
func (m *Manager) connect(ctx context.Context, cfg Config) (*Client, error) {
	if m.open != nil {
		return m.open(ctx, cfg)
	}
	return Open(ctx, cfg)
}

// Init validates before opening anything and publishes atomically. Failure
// closes newly opened pools. A successful Init cannot be repeated.
func (m *Manager) Init(ctx context.Context, configs map[string]Config, defaultName string) error {
	if err := m.lock(ctx); err != nil {
		return normalize("init", err)
	}
	defer m.unlock()
	m.mu.RLock()
	closed, initialized := m.closed, m.initialized
	m.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if initialized {
		return ErrAlreadyInitialized
	}
	if len(configs) == 0 {
		return invalid("connections")
	}
	names := make([]string, 0, len(configs))
	for name, cfg := range configs {
		if !validName(name) {
			return invalid("connection_name")
		}
		if err := cfg.Validate(); err != nil {
			return err
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if defaultName == "" && len(names) == 1 {
		defaultName = names[0]
	}
	if _, ok := configs[defaultName]; !ok {
		return invalid("default_connection")
	}
	staged := make(map[string]*Client, len(configs))
	for _, name := range names {
		c, err := m.connect(ctx, configs[name])
		if err != nil {
			errs := []error{err}
			for _, c := range staged {
				errs = append(errs, c.Close(context.Background()))
			}
			return errors.Join(errs...)
		}
		staged[name] = c
	}
	if err := ctx.Err(); err != nil {
		errs := []error{normalize("init", err)}
		for _, c := range staged {
			errs = append(errs, c.Close(context.Background()))
		}
		return errors.Join(errs...)
	}
	m.mu.Lock()
	m.clients = staged
	m.defaultName = defaultName
	m.initialized = true
	m.mu.Unlock()
	return nil
}

// Add requires successful Init. Existing aliases are never replaced.
func (m *Manager) Add(ctx context.Context, name string, cfg Config) error {
	if !validName(name) {
		return invalid("connection_name")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := m.lock(ctx); err != nil {
		return normalize("add", err)
	}
	defer m.unlock()
	m.mu.RLock()
	closed, initialized := m.closed, m.initialized
	_, exists := m.clients[name]
	m.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if !initialized {
		return invalid("manager_not_initialized")
	}
	if exists {
		return ErrDuplicateConnection
	}
	c, err := m.connect(ctx, cfg)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(normalize("add", err), c.Close(context.Background()))
	}
	m.mu.Lock()
	m.clients[name] = c
	m.mu.Unlock()
	return nil
}
func (m *Manager) Lookup(name string) (*Client, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}
	if name == "" {
		name = m.defaultName
	}
	c, ok := m.clients[name]
	if !ok {
		return nil, ErrNotFound
	}
	return c, nil
}

// Close starts every client drain, even when another client is slow. Retrying a
// timed-out wait is safe; new lookups and lifecycle changes stay rejected.
func (m *Manager) Close(ctx context.Context) error {
	if err := m.lock(ctx); err != nil {
		return normalize("close", err)
	}
	defer m.unlock()
	m.mu.Lock()
	m.closed = true
	clients := make([]*Client, 0, len(m.clients))
	for _, c := range m.clients {
		clients = append(clients, c)
	}
	m.mu.Unlock()
	var wg sync.WaitGroup
	errs := make([]error, len(clients))
	for i, c := range clients {
		wg.Add(1)
		go func() { defer wg.Done(); errs[i] = c.Close(ctx) }()
	}
	wg.Wait()
	return errors.Join(errs...)
}
