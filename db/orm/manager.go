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
	initOnce      sync.Once
	initErr       error
)

// Init initializes the global ORM manager with multiple configurations.
// If defaultDBName is provided, it marks that key as the default DB.
// If configs contains only 1 entry, it automatically becomes the default DB regardless.
// Safe to call concurrently, guarantees initialization happens only once.
func Init(configs map[string]Config, defaultDBName ...string) error {
	initOnce.Do(func() {
		if len(configs) == 0 {
			initErr = errors.New("no orm configurations provided")
			return
		}

		mgr := &Manager{
			dbs: make(map[string]*gorm.DB),
		}

		// Connect to all configured databases
		for name, cfg := range configs {
			db, err := New(cfg)
			if err != nil {
				initErr = fmt.Errorf("failed to connect to orm instance '%s': %w", name, err)
				return
			}
			mgr.dbs[name] = db
		}

		// Determine the default DB name
		if len(configs) == 1 {
			for name := range configs {
				mgr.defaultName = name
			}
		} else if len(defaultDBName) > 0 && defaultDBName[0] != "" {
			if _, exists := mgr.dbs[defaultDBName[0]]; !exists {
				initErr = fmt.Errorf("default db name '%s' not found in configs", defaultDBName[0])
				return
			}
			mgr.defaultName = defaultDBName[0]
		}

		globalManager = mgr
	})

	return initErr
}

// AddConnection dynamically adds a new ORM connection at runtime.
// If the manager has not been initialized yet, it will initialize it.
// If setAsDefault is true (or if this is the very first connection), this connection becomes the default database.
func AddConnection(name string, cfg Config, setAsDefault bool) error {
	// Lazily initialize global manager if not done yet
	initOnce.Do(func() {
		globalManager = &Manager{
			dbs: make(map[string]*gorm.DB),
		}
	})

	if globalManager == nil {
		return fmt.Errorf("manager initialization previously failed: %v", initErr)
	}

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
