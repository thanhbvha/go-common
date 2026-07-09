// Package mongodb provides a robust, production-ready MongoDB client wrapper.
//
// It includes features like multi-connection management (singleton manager),
// automatic OpenTelemetry instrumentation, pagination utilities, and a generic
// repository pattern for common CRUD operations.
//
// Basic usage:
//
//	err := mongodb.Init(ctx, map[string]mongodb.Config{
//		"main": { URI: "mongodb://...", DBName: "app_db" },
//	})
//	db := mongodb.Get("main")
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Manager handles multiple MongoDB connections.
type Manager struct {
	dbs         map[string]*mongo.Database
	defaultName string
	mu          sync.RWMutex
}

// globalManager is the singleton instance used by package-level functions.
var (
	globalManager *Manager
	initOnce      sync.Once
	initErr       error
)

// Init initializes the global MongoDB manager with multiple configurations.
// If defaultDBName is provided, it marks that key as the default DB.
// If configs contains only 1 entry, it automatically becomes the default DB regardless.
// This function is safe to call concurrently; it guarantees initialization happens only once.
func Init(ctx context.Context, configs map[string]Config, defaultDBName ...string) error {
	initOnce.Do(func() {
		if len(configs) == 0 {
			initErr = errors.New("no mongodb configurations provided")
			return
		}

		mgr := &Manager{
			dbs: make(map[string]*mongo.Database),
		}

		// Connect to all configured databases
		for name, cfg := range configs {
			client, err := New(ctx, cfg)
			if err != nil {
				initErr = fmt.Errorf("failed to connect to mongodb instance '%s': %w", name, err)
				return
			}
			mgr.dbs[name] = GetDatabase(client, cfg.DBName)
		}

		// Determine the default DB name
		if len(configs) == 1 {
			// Automatically set as default if there's only one
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

// Get returns a MongoDB database instance by name.
// If no name is provided (or empty), it returns the default database.
// It panics if the manager is not initialized, the default is not set, or the name is not found.
func Get(name ...string) *mongo.Database {
	if globalManager == nil {
		panic("mongodb manager is not initialized, call mongodb.Init or mongodb.AddConnection first")
	}

	globalManager.mu.RLock()
	defer globalManager.mu.RUnlock()

	targetName := globalManager.defaultName
	if len(name) > 0 && name[0] != "" {
		targetName = name[0]
	}

	if targetName == "" {
		panic("no default mongodb configured and no name provided")
	}

	db, exists := globalManager.dbs[targetName]
	if !exists {
		panic(fmt.Sprintf("mongodb instance '%s' not found", targetName))
	}

	return db
}

// WithTransaction starts a MongoDB session and executes the given function within a transaction.
// Ensure your Repository operations use the sessCtx provided to the callback.
func WithTransaction(ctx context.Context, dbName string, fn func(sessCtx context.Context) error) error {
	db := Get(dbName)
	client := db.Client()

	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		return nil, fn(sessCtx)
	})
	return err
}

// AddConnection dynamically adds a new MongoDB connection at runtime.
// If the manager has not been initialized yet, it will initialize it.
// If setAsDefault is true (or if this is the very first connection), this connection becomes the default database.
func AddConnection(ctx context.Context, name string, cfg Config, setAsDefault bool) error {
	// Lazily initialize global manager if not done yet
	initOnce.Do(func() {
		globalManager = &Manager{
			dbs: make(map[string]*mongo.Database),
		}
	})

	if globalManager == nil {
		return fmt.Errorf("manager initialization previously failed: %v", initErr)
	}

	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()

	if _, exists := globalManager.dbs[name]; exists {
		return fmt.Errorf("mongodb instance '%s' already exists", name)
	}

	client, err := New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("failed to connect to mongodb instance '%s': %w", name, err)
	}

	globalManager.dbs[name] = GetDatabase(client, cfg.DBName)

	if setAsDefault || len(globalManager.dbs) == 1 {
		globalManager.defaultName = name
	}

	return nil
}

// DisconnectAll gracefully closes all connections.
func DisconnectAll(ctx context.Context) error {
	if globalManager == nil {
		return nil
	}

	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()

	var errs []error
	for name, db := range globalManager.dbs {
		if err := db.Client().Disconnect(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to disconnect '%s': %w", name, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while disconnecting: %v", errs)
	}
	return nil
}
