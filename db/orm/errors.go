package orm

import (
	"context"
	"errors"
	"fmt"

	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"gorm.io/gorm"
)

var (
	ErrNotFound                  = errors.New("orm: record not found")
	ErrConflict                  = errors.New("orm: constraint conflict")
	ErrInvalidArgument           = errors.New("orm: invalid argument")
	ErrInvalidConfig             = backend.ErrInvalidConfig
	ErrUnsupportedFeature        = backend.ErrUnsupportedFeature
	ErrUnsupportedVersion        = backend.ErrUnsupportedVersion
	ErrDeadlock                  = errors.New("orm: database deadlock")
	ErrSerialization             = errors.New("orm: serialization failure")
	ErrLockTimeout               = errors.New("orm: database lock timeout")
	ErrStatementTimeout          = errors.New("orm: database statement timeout")
	ErrClosed                    = errors.New("orm: closed")
	ErrAlreadyInitialized        = errors.New("orm: already initialized")
	ErrDuplicateConnection       = errors.New("orm: duplicate connection")
	ErrNestedTransaction         = errors.New("orm: nested transaction is not supported")
	ErrTransactionContext        = errors.New("orm: transaction context required")
	ErrTransactionClosed         = errors.New("orm: transaction scope has ended")
	ErrDatabase                  = errors.New("orm: database operation failed")
	ErrTransactionClientMismatch = errors.New("orm: transaction belongs to another client")
	ErrCommitUnknown             = errors.New("orm: commit outcome is unknown")
	ErrBusy                      = errors.New("orm: admission capacity exhausted")
	ErrVersionRequired           = errors.New("orm: versioned write required")
	ErrVersionConflict           = errors.New("orm: version did not match an accessible record")
)

// Error has a safe printable message. Unwrap preserves driver errors for
// errors.As; never log unwrapped errors, which may contain credentials or data.
type Error struct {
	Operation  string
	Kind       error
	SQLState   string
	DriverCode string
	cause      error
}

func (e *Error) Error() string        { return "orm: " + e.Operation + ": " + e.Kind.Error() }
func (e *Error) Unwrap() error        { return e.cause }
func (e *Error) Is(target error) bool { return target == e.Kind }
func invalid(field string) error      { return fmt.Errorf("%w: %s", ErrInvalidConfig, field) }

// GORM can wrap a rollback failure after a canceled statement and lose the
// cancellation in its error chain. Preserve the operation context as well.
func resultError(op string, db *gorm.DB) error {
	err := db.Error
	if err != nil && db.Statement != nil && db.Statement.Context != nil {
		if ctxErr := db.Statement.Context.Err(); ctxErr != nil {
			err = errors.Join(ctxErr, err)
		}
	}
	return normalize(op, err)
}

func normalize(op string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*Error); ok {
		return err
	}
	kind := ErrDatabase
	switch {
	case errors.Is(err, context.Canceled):
		kind = context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		kind = context.DeadlineExceeded
	case errors.Is(err, gorm.ErrRecordNotFound):
		kind = ErrNotFound
	case errors.Is(err, gorm.ErrDuplicatedKey), errors.Is(err, gorm.ErrForeignKeyViolated):
		kind = ErrConflict
	}
	for _, candidate := range []error{ErrNotFound, ErrConflict, ErrInvalidArgument, ErrInvalidConfig, ErrUnsupportedFeature, ErrUnsupportedVersion, ErrClosed, ErrNestedTransaction, ErrTransactionContext, ErrTransactionClosed, ErrTransactionClientMismatch, ErrBusy, ErrVersionRequired, ErrVersionConflict, ErrCommitUnknown} {
		if errors.Is(err, candidate) {
			kind = candidate
			break
		}
	}
	var state interface{ SQLState() string }
	code := ""
	if errors.As(err, &state) {
		code = state.SQLState()
		if len(code) != 5 {
			code = ""
		}
		for _, c := range code {
			if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z') {
				code = ""
				break
			}
		}
	}
	return &Error{Operation: op, Kind: kind, SQLState: code, cause: err}
}

func normalizeBackend(op string, err error, driver backend.Backend) error {
	if err == nil {
		return nil
	}
	safe := normalize(op, err)
	if driver == nil {
		return safe
	}
	info := driver.Classify(err)
	original := safe.(*Error)
	copy := *original
	validCode := func(s string, max int) string {
		if len(s) > max {
			return ""
		}
		for _, c := range s {
			if !(c >= '0' && c <= '9' || c >= 'A' && c <= 'Z') {
				return ""
			}
		}
		return s
	}
	if code := validCode(info.Code, 16); code != "" {
		copy.DriverCode = code
	}
	if len(info.SQLState) == 5 {
		copy.SQLState = validCode(info.SQLState, 5)
	}
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		switch info.Kind {
		case backend.Constraint:
			copy.Kind = ErrConflict
		case backend.Deadlock:
			copy.Kind = ErrDeadlock
		case backend.Serialization:
			copy.Kind = ErrSerialization
		case backend.LockTimeout:
			copy.Kind = ErrLockTimeout
		case backend.StatementTimeout:
			copy.Kind = ErrStatementTimeout
		}
	}
	return &copy
}
