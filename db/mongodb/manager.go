// Package mongodb provides MongoDB clients, instance-scoped connection management,
// OpenTelemetry instrumentation and generic repositories. Prefer NewManager for
// isolated services and tests; package-level helpers use a shared manager.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Private clients and named groups have separate namespaces.
type clientKey struct {
	name   string
	shared bool
}
type managedClient struct {
	client *mongo.Client
	config Config
}

func keyFor(alias string, cfg Config) clientKey {
	if cfg.ClientGroup != "" {
		return clientKey{name: cfg.ClientGroup, shared: true}
	}
	return clientKey{name: alias}
}

func connectionConfig(cfg Config) Config {
	cfg.DBName, cfg.ClientGroup = "", ""
	return cfg
}

func groupConflict(a, b Config) error {
	a, b = connectionConfig(a), connectionConfig(b)
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	var fields []string
	for i := 0; i < x.NumField(); i++ {
		if x.Field(i).Interface() != y.Field(i).Interface() {
			fields = append(fields, x.Type().Field(i).Name)
		}
	}
	return &ClientGroupError{Fields: fields}
}

// Manager must not be copied after first use. Its zero value is usable.
// Lifecycle changes are serialized; lookups never wait for network I/O.
type Manager struct {
	opMu        contextGate
	mu          sync.RWMutex
	dbs         map[string]*mongo.Database
	clients     map[clientKey]managedClient
	bindings    map[string]clientKey
	defaultName string
	generation  *transactionGeneration
	stats       TransactionStats
	initialized bool                                                 // protected by opMu
	connect     func(context.Context, Config) (*mongo.Client, error) // test seam, set before use
}

func NewManager() *Manager { return &Manager{} }

var globalManager = NewManager()

// Init initializes once until DisconnectAll. Repeated successful calls are no-ops.
// Connections added earlier are preserved. Newly opened clients are rolled back
// on failure and are published together only after every connection succeeds.
func (m *Manager) Init(ctx context.Context, configs map[string]Config, defaultDBName ...string) error {
	if err := m.opMu.Lock(ctx); err != nil {
		return err
	}
	defer m.opMu.Unlock()
	m.mu.RLock()
	closing := m.generation != nil && m.generation.closing
	m.mu.RUnlock()
	if closing {
		return ErrManagerClosing
	}
	if m.initialized {
		return nil
	}
	if len(configs) == 0 {
		return errors.New("no mongodb configurations provided")
	}

	m.mu.RLock()
	staged := make(map[string]*mongo.Database, len(m.dbs)+len(configs))
	for name, db := range m.dbs {
		staged[name] = db
	}
	defaultName := m.defaultName
	clients := make(map[clientKey]managedClient, len(m.clients)+len(configs))
	for key, client := range m.clients {
		clients[key] = client
	}
	bindings := make(map[string]clientKey, len(m.bindings)+len(configs))
	for alias, key := range m.bindings {
		bindings[alias] = key
	}
	m.mu.RUnlock()

	names := make([]string, 0, len(configs))
	for name, cfg := range configs {
		if err := validateConnection(name, cfg); err != nil {
			return err
		}
		names = append(names, name)
	}
	sort.Strings(names)
	// Resolve every group before dialing or publishing any handles. Exact config
	// matching is intentional: URI normalization cannot establish policy equality.
	groupConfigs := make(map[clientKey]Config, len(clients)+len(configs))
	for key, client := range clients {
		groupConfigs[key] = client.config
	}
	for _, name := range names {
		cfg := configs[name]
		key := keyFor(name, cfg)
		if _, exists := staged[name]; exists {
			// Legacy ungrouped bindings keep their historical preserve-on-Init behavior.
			if cfg.ClientGroup == "" {
				continue
			}
			if bindings[name] != key || staged[name].Name() != cfg.DBName {
				return fmt.Errorf("alias %q: %w", name, ErrClientGroupConflict)
			}
		}
		if previous, exists := groupConfigs[key]; exists && previous != connectionConfig(cfg) {
			return groupConflict(previous, cfg)
		}
		groupConfigs[key] = connectionConfig(cfg)
	}
	if len(defaultDBName) > 0 && defaultDBName[0] != "" {
		defaultName = defaultDBName[0]
		if _, exists := staged[defaultName]; !exists {
			if _, exists := configs[defaultName]; !exists {
				return fmt.Errorf("default db name %q not found", defaultName)
			}
		}
	} else if defaultName == "" && len(configs) == 1 {
		defaultName = names[0]
	}

	var opened []*mongo.Client
	for _, name := range names {
		if _, exists := staged[name]; exists {
			continue
		}
		cfg := configs[name]
		key := keyFor(name, cfg)
		entry, exists := clients[key]
		if !exists {
			client, err := m.open(ctx, cfg)
			if err != nil {
				errs := []error{fmt.Errorf("failed to connect to mongodb instance %q: %w", name, err)}
				for _, client := range opened {
					errs = append(errs, disconnectClient(client))
				}
				return errors.Join(errs...)
			}
			opened = append(opened, client)
			entry = managedClient{client: client, config: connectionConfig(cfg)}
			clients[key] = entry
		}
		bindings[name] = key
		staged[name] = entry.client.Database(cfg.DBName)
	}
	if err := ctx.Err(); err != nil {
		for _, client := range opened {
			err = errors.Join(err, disconnectClient(client))
		}
		return err
	}
	m.mu.Lock()
	m.dbs, m.defaultName = staged, defaultName
	m.clients, m.bindings = clients, bindings
	if m.generation == nil {
		m.generation = &transactionGeneration{drained: make(chan struct{})}
	}
	m.mu.Unlock()
	m.initialized = true
	return nil
}

func validateConnection(name string, cfg Config) error {
	if strings.TrimSpace(name) == "" {
		return &ConfigError{Field: "connection_name", Code: "required"}
	}
	if cfg.ClientGroup != "" && strings.TrimSpace(cfg.ClientGroup) == "" {
		return &ConfigError{Field: "client_group", Code: "blank"}
	}
	if strings.TrimSpace(cfg.DBName) == "" || strings.ContainsAny(cfg.DBName, "/\\. \"$\x00") {
		return &ConfigError{Field: "dbname", Code: "invalid"}
	}
	if _, err := clientOptions(cfg); err != nil {
		return fmt.Errorf("mongodb instance %q: %w", name, err)
	}
	return nil
}

func (m *Manager) open(ctx context.Context, cfg Config) (*mongo.Client, error) {
	if m.connect != nil {
		return m.connect(ctx, cfg)
	}
	return New(ctx, cfg)
}

// Lookup returns an error instead of panicking. Empty name selects the default.
func (m *Manager) Lookup(name string) (*mongo.Database, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.generation != nil && m.generation.closing {
		return nil, ErrManagerClosing
	}
	if name == "" {
		name = m.defaultName
	}
	if name == "" {
		return nil, errors.New("no default mongodb configured")
	}
	db, exists := m.dbs[name]
	if !exists {
		return nil, fmt.Errorf("mongodb instance %q not found", name)
	}
	return db, nil
}

func (m *Manager) AddConnection(ctx context.Context, name string, cfg Config, setAsDefault bool) error {
	if err := m.opMu.Lock(ctx); err != nil {
		return err
	}
	defer m.opMu.Unlock()
	if err := validateConnection(name, cfg); err != nil {
		return err
	}
	m.mu.RLock()
	if m.generation != nil && m.generation.closing {
		m.mu.RUnlock()
		return ErrManagerClosing
	}
	_, exists := m.dbs[name]
	key := keyFor(name, cfg)
	entry, shared := m.clients[key]
	m.mu.RUnlock()
	if exists {
		return fmt.Errorf("mongodb instance %q already exists", name)
	}
	if shared && entry.config != connectionConfig(cfg) {
		return groupConflict(entry.config, cfg)
	}
	if !shared {
		client, err := m.open(ctx, cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to mongodb instance %q: %w", name, err)
		}
		entry = managedClient{client: client, config: connectionConfig(cfg)}
	}
	if err := ctx.Err(); err != nil {
		if !shared {
			err = errors.Join(err, disconnectClient(entry.client))
		}
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dbs == nil {
		m.dbs = make(map[string]*mongo.Database)
	}
	if m.clients == nil {
		m.clients = make(map[clientKey]managedClient)
	}
	if m.bindings == nil {
		m.bindings = make(map[string]clientKey)
	}
	m.clients[key], m.bindings[name] = entry, key
	if m.generation == nil {
		m.generation = &transactionGeneration{drained: make(chan struct{})}
	}
	m.dbs[name] = entry.client.Database(cfg.DBName)
	if setAsDefault || len(m.dbs) == 1 {
		m.defaultName = name
	}
	return nil
}

// DisconnectAll rejects new lookups/transactions, drains managed transactions,
// then closes clients. If draining times out, clients remain owned and closing;
// call DisconnectAll again after active work ends. Raw client operations are not
// tracked and must be stopped by the application before shutdown.
func (m *Manager) DisconnectAll(ctx context.Context) error {
	if err := m.opMu.Lock(ctx); err != nil {
		return err
	}
	defer m.opMu.Unlock()
	m.mu.Lock()
	g := m.generation
	if g != nil && !g.closing {
		g.closing = true
		if g.active == 0 {
			close(g.drained)
		}
	}
	m.mu.Unlock()
	if g != nil {
		select {
		case <-g.drained:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.mu.Lock()
	dbs := m.dbs
	m.dbs, m.defaultName = nil, ""
	m.clients, m.bindings = nil, nil
	m.generation = nil
	m.mu.Unlock()
	m.initialized = false
	seen := make(map[*mongo.Client]bool)
	var errs []error
	for name, db := range dbs {
		client := db.Client()
		if seen[client] {
			continue
		}
		seen[client] = true
		if err := client.Disconnect(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to disconnect %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func Init(ctx context.Context, configs map[string]Config, defaultDBName ...string) error {
	return globalManager.Init(ctx, configs, defaultDBName...)
}

// Get preserves the legacy panic-on-misconfiguration API. Prefer Lookup.
func Get(name ...string) *mongo.Database {
	target := ""
	if len(name) > 0 {
		target = name[0]
	}
	db, err := Lookup(target)
	if err != nil {
		panic(err)
	}
	return db
}

func Lookup(name string) (*mongo.Database, error) { return globalManager.Lookup(name) }

func AddConnection(ctx context.Context, name string, cfg Config, setAsDefault bool) error {
	return globalManager.AddConnection(ctx, name, cfg, setAsDefault)
}

func DisconnectAll(ctx context.Context) error { return globalManager.DisconnectAll(ctx) }

func WithTransaction(ctx context.Context, dbName string, fn func(context.Context) error) error {
	return globalManager.WithTransaction(ctx, dbName, fn)
}
