// Package orm provides a robust, production-ready PostgreSQL client using GORM.
//
// It includes features like multi-connection management (singleton manager),
// connection pooling, automatic OpenTelemetry instrumentation, pagination utilities,
// and a generic repository pattern for common CRUD operations.
//
// Basic usage:
//
//	err := orm.Init(map[string]orm.Config{
//		"main": { Host: "localhost", DBName: "app_db", User: "postgres" },
//	})
//	db := orm.Get("main")
package orm

import (
	"errors"
	"fmt"
	"sync"

	"gorm.io/gorm"
)

// Manager handles multiple ORM database connections.
type Manager struct {
	dbs         map[string]*gorm.DB
	defaultName string
	mu          sync.RWMutex
}

var (
	globalManager *Manager
	initMu        sync.Mutex
	isInitialized bool
)

// Init initializes the global ORM manager with multiple configurations.
// If defaultDBName is provided, it marks that key as the default DB.
// If configs contains only 1 entry, it automatically becomes the default DB regardless.
// Safe to call concurrently, guarantees initialization happens only once.
func Init(configs map[string]Config, defaultDBName ...string) error {
	initMu.Lock()
	defer initMu.Unlock()

	if isInitialized {
		return nil
	}

	if len(configs) == 0 {
		return errors.New("no orm configurations provided")
	}

	if globalManager == nil {
		globalManager = &Manager{
			dbs: make(map[string]*gorm.DB),
		}
	}

	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()

	// Connect to all configured databases
	for name, cfg := range configs {
		if _, exists := globalManager.dbs[name]; exists {
			continue // Skip if already added via AddConnection
		}

		db, err := New(cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to orm instance '%s': %w", name, err)
		}
		globalManager.dbs[name] = db
	}

	// Determine the default DB name
	if len(configs) == 1 && globalManager.defaultName == "" {
		for name := range configs {
			globalManager.defaultName = name
		}
	} else if len(defaultDBName) > 0 && defaultDBName[0] != "" {
		if _, exists := globalManager.dbs[defaultDBName[0]]; !exists {
			return fmt.Errorf("default db name '%s' not found in configs", defaultDBName[0])
		}
		globalManager.defaultName = defaultDBName[0]
	}

	isInitialized = true
	return nil
}

// AddConnection dynamically adds a new ORM connection at runtime.
// If the manager has not been initialized yet, it will initialize it.
// If setAsDefault is true (or if this is the very first connection), this connection becomes the default database.
func AddConnection(name string, cfg Config, setAsDefault bool) error {
	initMu.Lock()
	if globalManager == nil {
		globalManager = &Manager{
			dbs: make(map[string]*gorm.DB),
		}
	}
	initMu.Unlock()

	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()

	if _, exists := globalManager.dbs[name]; exists {
		return fmt.Errorf("orm instance '%s' already exists", name)
	}

	db, err := New(cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to orm instance '%s': %w", name, err)
	}

	globalManager.dbs[name] = db

	if setAsDefault || len(globalManager.dbs) == 1 {
		globalManager.defaultName = name
	}

	return nil
}

// Get returns a GORM database instance by name.
// If no name is provided (or empty), it returns the default database.
// Panics if the manager is not initialized, the default is not set, or the name is not found.
func Get(name ...string) *gorm.DB {
	if globalManager == nil {
		panic("orm manager is not initialized, call orm.Init or orm.AddConnection first")
	}

	globalManager.mu.RLock()
	defer globalManager.mu.RUnlock()

	targetName := globalManager.defaultName
	if len(name) > 0 && name[0] != "" {
		targetName = name[0]
	}

	if targetName == "" {
		panic("no default orm configured and no name provided")
	}

	db, exists := globalManager.dbs[targetName]
	if !exists {
		panic(fmt.Sprintf("orm instance '%s' not found", targetName))
	}

	return db
}

// Close gracefully closes all database connections managed by this instance.
// It is useful for graceful shutdown to prevent connection leaks.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for name, db := range m.dbs {
		sqlDB, err := db.DB()
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to get sql.DB for '%s': %w", name, err))
			continue
		}
		if err := sqlDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close db '%s': %w", name, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing databases: %v", errs)
	}
	return nil
}

// Close gracefully closes all database connections in the global manager.
// Safe to call even if the manager is not initialized.
func Close() error {
	if globalManager == nil {
		return nil
	}
	return globalManager.Close()
}
