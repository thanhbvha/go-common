package mongodb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Transaction is valid only during one callback attempt. Use it sequentially;
// MongoDB sessions do not support parallel operations. Retries get fresh scopes.
// Scope checks are programming guards, not an authorization boundary.
type Transaction struct {
	ctx         context.Context
	dbs         map[string]*mongo.Database
	defaultName string
	active      atomic.Bool
}

type transactionContextKey struct{}

// Context returns the session context. Derived contexts may shorten its deadline.
func (tx *Transaction) Context() context.Context { return tx.ctx }

func (tx *Transaction) check() error {
	if tx == nil || !tx.active.Load() {
		return ErrTransactionClosed
	}
	return tx.ctx.Err()
}

// Database returns only aliases declared when opening the transaction. Native
// driver operations still require Context(); prefer NewTransactionRepository.
func (tx *Transaction) Database(alias string) (*mongo.Database, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if alias == "" {
		alias = tx.defaultName
	}
	db, ok := tx.dbs[alias]
	if !ok {
		return nil, fmt.Errorf("alias %q: %w", alias, ErrTransactionScope)
	}
	return db, nil
}

// WithTransaction retains the single-alias callback API. Always pass sessCtx to
// operations. Existing session contexts are rejected instead of being replaced.
func (m *Manager) WithTransaction(ctx context.Context, alias string, fn func(context.Context) error, opts ...options.Lister[options.TransactionOptions]) error {
	if fn == nil {
		return errors.New("mongodb transaction callback is required")
	}
	return m.WithTransactionAcross(ctx, []string{alias}, func(tx *Transaction) error { return fn(tx.Context()) }, opts...)
}

// WithTransactionAcross validates all aliases share one client before invoking fn.
// It delegates transaction and commit retries to the driver. Propagate all DB
// errors, keep callbacks idempotent, and use an outbox for external side effects.
// A context deadline bounds the whole operation; without one, TransactionTimeout
// applies (default 30s). Cancellation is cooperative: fn must honor the context.
func (m *Manager) WithTransactionAcross(ctx context.Context, aliases []string, fn func(*Transaction) error, opts ...options.Lister[options.TransactionOptions]) (err error) {
	if fn == nil {
		return errors.New("mongodb transaction callback is required")
	}
	if mongo.SessionFromContext(ctx) != nil {
		return ErrNestedTransaction
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(aliases) == 0 {
		return errors.New("mongodb transaction requires at least one database alias")
	}

	// One lookup snapshot prevents lifecycle changes from mixing generations.
	m.mu.Lock()
	if m.generation != nil && m.generation.closing {
		m.mu.Unlock()
		return ErrManagerClosing
	}
	dbs := make(map[string]*mongo.Database, len(aliases))
	defaultName := m.defaultName
	var client *mongo.Client
	timeout := DefaultConfig().TransactionTimeout
	var lookupErr error
	var monitor *TransactionMonitor
	var group string
	for _, alias := range aliases {
		if alias == "" {
			alias = defaultName
		}
		db, exists := m.dbs[alias]
		if !exists {
			lookupErr = fmt.Errorf("mongodb instance %q not found", alias)
			break
		}
		if client != nil && db.Client() != client {
			lookupErr = ErrTransactionClientMismatch
			break
		}
		if client == nil {
			client = db.Client()
			group = m.bindings[alias].name
			if entry, ok := m.clients[m.bindings[alias]]; ok {
				if entry.config.TransactionTimeout != 0 {
					timeout = entry.config.TransactionTimeout
				}
				monitor = entry.config.TransactionMonitor
			}
		}
		dbs[alias] = db
	}
	if lookupErr != nil {
		m.mu.Unlock()
		return lookupErr
	}
	g := m.generation
	g.active++
	m.stats.Started++
	m.stats.Active++
	m.mu.Unlock()
	started := time.Now()
	attempts := 0
	completed := false
	defer func() {
		outcome := "failed"
		var serverError mongo.ServerError
		if completed && err == nil {
			outcome = "committed"
		} else if errors.As(err, &serverError) && serverError.HasErrorLabel("UnknownTransactionCommitResult") {
			outcome = "unknown"
		}
		m.mu.Lock()
		m.stats.Active--
		switch outcome {
		case "committed":
			m.stats.Committed++
		case "unknown":
			m.stats.Unknown++
		default:
			m.stats.Failed++
		}
		m.mu.Unlock()
		m.releaseTransaction(g)
		observe(monitor, TransactionEvent{Phase: "finished", ClientGroup: group, Attempt: attempts, Duration: time.Since(started), Outcome: outcome})
	}()
	observe(monitor, TransactionEvent{Phase: "started", ClientGroup: group})
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	session, err := client.StartSession()
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		session.EndSession(cleanup)
	}()
	_, err = session.WithTransaction(ctx, func(sessCtx context.Context) (any, error) {
		attempts++
		m.mu.Lock()
		m.stats.CallbackAttempts++
		if attempts > 1 {
			m.stats.Retries++
		}
		m.mu.Unlock()
		observe(monitor, TransactionEvent{Phase: "attempt", ClientGroup: group, Attempt: attempts, Duration: time.Since(started)})
		tx := &Transaction{dbs: dbs, defaultName: defaultName}
		tx.ctx = context.WithValue(sessCtx, transactionContextKey{}, tx)
		tx.active.Store(true)
		defer tx.active.Store(false)
		return nil, fn(tx)
	}, opts...)
	completed = true
	return err
}

// WithTransactionAcross is the package-level scope API and accepts driver options.
func WithTransactionAcross(ctx context.Context, aliases []string, fn func(*Transaction) error, opts ...options.Lister[options.TransactionOptions]) error {
	return globalManager.WithTransactionAcross(ctx, aliases, fn, opts...)
}
