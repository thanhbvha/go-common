// Package backend defines the instance-scoped contract between ORM cores and SQL
// adapters. Adapters own configuration, never pool lifetime. No global registry.
package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

var (
	ErrInvalidConfig      = errors.New("orm: invalid configuration")
	ErrUnsupportedFeature = errors.New("orm: unsupported backend feature")
	ErrUnsupportedVersion = errors.New("orm: unsupported database version")
)

func Invalid(field string) error       { return fmt.Errorf("%w: %s", ErrInvalidConfig, field) }
func Unsupported(feature string) error { return fmt.Errorf("%w: %s", ErrUnsupportedFeature, feature) }

type ErrorKind string

const (
	Unknown          ErrorKind = ""
	Constraint       ErrorKind = "constraint"
	Deadlock         ErrorKind = "deadlock"
	Serialization    ErrorKind = "serialization"
	LockTimeout      ErrorKind = "lock_timeout"
	StatementTimeout ErrorKind = "statement_timeout"
)

// ErrorInfo contains validated codes, never raw driver messages.
type ErrorInfo struct {
	Kind     ErrorKind
	SQLState string
	Code     string
}

type Capabilities struct {
	Name                     string
	Version                  string
	Transactions             bool
	Savepoints               bool
	MatchedRows              bool
	StrictConflictTarget     bool
	AnyUniqueKeyUpsert       bool
	ConflictDoNothing        bool
	ReadOnlyTransactions     bool
	TransactionLocalTimeouts bool
	IsolationLevels          []sql.IsolationLevel
	MaxBindParameters        int
	MaxBatchBytes            int
}

func (c Capabilities) SupportsIsolation(level sql.IsolationLevel) bool {
	for _, v := range c.IsolationLevels {
		if v == level {
			return true
		}
	}
	return false
}

type Requirements struct {
	StrictConflictTarget     bool
	ReadOnlyTransactions     bool
	TransactionLocalTimeouts bool
}

func (r Requirements) Check(c Capabilities) error {
	switch {
	case !c.Transactions || !c.MatchedRows || c.MaxBindParameters <= 0 || c.MaxBatchBytes < 2048:
		return Unsupported("repository_contract")
	case r.StrictConflictTarget && !c.StrictConflictTarget:
		return Unsupported("strict_conflict_target")
	case r.ReadOnlyTransactions && !c.ReadOnlyTransactions:
		return Unsupported("read_only_transaction")
	case r.TransactionLocalTimeouts && !c.TransactionLocalTimeouts:
		return Unsupported("transaction_local_timeouts")
	}
	return nil
}

// Backend implementations must be immutable and safe for concurrent reuse.
// Prepare must avoid unbounded I/O; Inspect and setup must honor ctx. Return
// original driver errors so the core can wrap safely while preserving errors.As.
type Backend interface {
	Name() string
	Validate() error
	Capabilities() Capabilities
	Prepare(context.Context) (gorm.Dialector, error)
	Inspect(context.Context, *gorm.DB) (Capabilities, error)
	SetupTransaction(context.Context, *gorm.DB) error
	Classify(error) ErrorInfo
}

// PreparedPool is implemented by dialectors that allocate an owned pool before
// GORM initialization. The core closes it on every failed startup path.
type PreparedPool interface{ PreparedPool() *sql.DB }
