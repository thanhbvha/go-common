package orm

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/db/orm/backend"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

type TransactionPhase string

const (
	PhaseAdmission TransactionPhase = "admission"
	PhaseBegin     TransactionPhase = "begin"
	PhaseSetup     TransactionPhase = "setup"
	PhaseCallback  TransactionPhase = "callback"
	PhaseCommit    TransactionPhase = "commit"
)

type TransactionOutcome string

const (
	OutcomeNotStarted TransactionOutcome = "not_started"
	OutcomeRolledBack TransactionOutcome = "rolled_back"
	OutcomeCommitted  TransactionOutcome = "committed"
	OutcomeUnknown    TransactionOutcome = "unknown"
)

// TransactionError preserves safe phase/outcome metadata and both underlying
// error chains. Commit failure is conservatively unknown; never blindly retry.
type TransactionError struct {
	Phase         TransactionPhase
	Outcome       TransactionOutcome
	cause         error
	rollbackError error
}

func (e *TransactionError) Error() string {
	return "orm: transaction " + string(e.Phase) + ": " + string(e.Outcome)
}
func (e *TransactionError) Unwrap() []error {
	out := []error{}
	if e.cause != nil {
		out = append(out, e.cause)
	}
	if e.rollbackError != nil {
		out = append(out, e.rollbackError)
	}
	return out
}
func (e *TransactionError) Is(target error) bool {
	return target == ErrCommitUnknown && e.Phase == PhaseCommit && e.Outcome == OutcomeUnknown
}

// RollbackError is nil after acknowledged rollback or automatic context rollback.
func (e *TransactionError) RollbackError() error { return e.rollbackError }

type transactionKey struct{}

// Transaction is callback-scoped and sequential. Repository operations must use
// Context() or a derived context. All participating work uses this one pool.
type Transaction struct {
	db     *gorm.DB
	ctx    context.Context
	client *Client
	active atomic.Bool
}

func (tx *Transaction) Context() context.Context { return tx.ctx }
func (tx *Transaction) baseDB() *gorm.DB         { return tx.db }
func (tx *Transaction) owner() *Client           { return tx.client }
func (tx *Transaction) session(ctx context.Context) (*gorm.DB, context.CancelFunc, error) {
	if !tx.active.Load() {
		return nil, nil, ErrTransactionClosed
	}
	if ctx.Value(transactionKey{}) != tx {
		return nil, nil, ErrTransactionContext
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, nil, normalize("transaction", err)
	}
	ctx, cancel := context.WithTimeout(ctx, tx.client.cfg.QueryTimeout)
	if err := ctx.Err(); err != nil {
		cancel()
		return nil, nil, normalize("transaction", err)
	}
	return tx.db.WithContext(ctx), cancel, nil
}

// DB bypasses repository policies. Native work must preserve this context and
// must not commit/rollback or launch nested transactions itself.
func (tx *Transaction) DB() (*gorm.DB, error) {
	if !tx.active.Load() {
		return nil, ErrTransactionClosed
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, normalize("transaction", err)
	}
	return tx.db.WithContext(tx.ctx), nil
}

// WithTransaction admits one unit of work across begin, callback and settlement.
// Timeouts are cooperative: fn must return. No automatic retry occurs. Close
// must be called outside fn. Panic triggers rollback and propagates to the caller.
func (c *Client) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(*Transaction) error) (result error) {
	started := time.Now()
	phase := PhaseAdmission
	outcome := OutcomeNotStarted
	ctx, cancel := context.WithTimeout(ctx, c.cfg.TransactionTimeout)
	defer cancel()
	var span trace.Span
	if c.tracer != nil {
		ctx, span = c.tracer.Start(ctx, "orm.transaction", trace.WithSpanKind(trace.SpanKindClient))
		defer span.End()
	}
	c.mu.Lock()
	c.transactions.Started++
	c.mu.Unlock()
	defer func() {
		elapsed := time.Since(started)
		c.mu.Lock()
		c.transactions.Duration += elapsed
		switch outcome {
		case OutcomeCommitted:
			c.transactions.Committed++
		case OutcomeRolledBack:
			c.transactions.RolledBack++
		case OutcomeUnknown:
			c.transactions.Unknown++
		default:
			c.transactions.NotStarted++
		}
		if result != nil || outcome != OutcomeCommitted {
			c.transactions.Failed++
		}
		c.mu.Unlock()
		c.observe(ctx, Event{Kind: EventTransaction, Phase: phase, Outcome: string(outcome), Duration: elapsed})
		if span != nil {
			span.SetAttributes(attribute.String("orm.transaction.phase", string(phase)), attribute.String("orm.transaction.outcome", string(outcome)))
			if outcome != OutcomeCommitted {
				span.SetStatus(codes.Error, "transaction did not commit")
			}
		}
	}()
	fail := func(err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = errors.Join(ctxErr, err)
		}
		return &TransactionError{Phase: phase, Outcome: outcome, cause: c.normalize("transaction", err)}
	}
	if fn == nil {
		return fail(ErrInvalidArgument)
	}
	if ctx.Value(transactionKey{}) != nil {
		return fail(ErrNestedTransaction)
	}
	if opts != nil {
		if opts.ReadOnly && !c.capabilities.ReadOnlyTransactions {
			return fail(backend.Unsupported("read_only_transaction"))
		}
		if !c.capabilities.SupportsIsolation(opts.Isolation) {
			return fail(backend.Unsupported("transaction_isolation"))
		}
	}
	release, err := c.acquire(ctx)
	if err != nil {
		return fail(err)
	}
	defer release()
	c.mu.Lock()
	c.active++
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.active--; c.mu.Unlock() }()
	phase = PhaseBegin
	db := c.db.WithContext(ctx).Begin(opts)
	if db.Error != nil {
		return fail(db.Error)
	}
	tx := &Transaction{db: db, client: c}
	tx.ctx = context.WithValue(ctx, transactionKey{}, tx)
	settled := false
	rollback := func(cause error) error {
		settled = true
		rollbackErr := db.Rollback().Error
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		outcome = OutcomeRolledBack
		if rollbackErr != nil {
			outcome = OutcomeUnknown
		}
		return &TransactionError{Phase: phase, Outcome: outcome, cause: c.normalize("transaction", cause), rollbackError: c.normalize("rollback", rollbackErr)}
	}
	defer func() {
		tx.active.Store(false)
		if !settled {
			_ = rollback(nil)
		}
	}()
	phase = PhaseSetup
	if err := c.setupTransaction(db); err != nil {
		return rollback(err)
	}
	phase = PhaseCallback
	tx.active.Store(true)
	err = fn(tx)
	tx.active.Store(false)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return rollback(err)
	}
	phase = PhaseCommit
	err = db.Commit().Error
	if err != nil {
		// A failed commit's acknowledgement is not proof that it did not commit.
		// Attempt cleanup, but a successful cleanup cannot resolve an unknown commit.
		cleanup := db.Rollback().Error
		if errors.Is(cleanup, sql.ErrTxDone) {
			cleanup = nil
		}
		settled = true
		outcome = OutcomeUnknown
		return &TransactionError{Phase: phase, Outcome: outcome, cause: c.normalize("commit", err), rollbackError: c.normalize("rollback", cleanup)}
	}
	settled = true
	outcome = OutcomeCommitted
	return nil
}
