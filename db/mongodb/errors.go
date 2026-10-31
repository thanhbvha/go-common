package mongodb

import (
	"errors"
	"strings"
)

// ErrNotFound is returned when a document is not found in the database.
// Callers should use errors.Is(err, mongodb.ErrNotFound) to check.
//
// Example:
//
//	user, err := repo.FindByID(ctx, id)
//	if errors.Is(err, mongodb.ErrNotFound) {
//	    return xerrors.ErrNotFound
//	}
var ErrNotFound = errors.New("mongodb: document not found")

var (
	ErrChangeStreamInTransaction = errors.New("mongodb: change streams cannot run inside a transaction")
	ErrClientGroupConflict       = errors.New("mongodb: client group configurations differ")
	ErrTransactionClientMismatch = errors.New("mongodb: transaction databases must share one client")
	ErrNestedTransaction         = errors.New("mongodb: transaction context already contains a session")
	ErrTransactionClosed         = errors.New("mongodb: transaction scope is no longer active")
	ErrTransactionScope          = errors.New("mongodb: database is outside the transaction scope")
	ErrTransactionContext        = errors.New("mongodb: repository requires its transaction context")
	ErrManagerClosing            = errors.New("mongodb: manager is closing")
	ErrTransactionsUnsupported   = errors.New("mongodb: deployment does not support transactions")
	ErrInvalidConfig             = errors.New("mongodb: invalid configuration")
	ErrInvalidCursor             = errors.New("mongodb: invalid pagination cursor")
)

// ConfigError exposes a safe field/code, never the URI, credentials or parser text.
type ConfigError struct {
	Field string
	Code  string
}

func (e *ConfigError) Error() string {
	return "mongodb: invalid configuration field " + e.Field + " (" + e.Code + ")"
}
func (e *ConfigError) Unwrap() error { return ErrInvalidConfig }

// ClientGroupError reports differing field names only, never their values.
type ClientGroupError struct{ Fields []string }

func (e *ClientGroupError) Error() string {
	return "mongodb: client group conflict in fields: " + strings.Join(e.Fields, ", ")
}
func (e *ClientGroupError) Unwrap() error { return ErrClientGroupConflict }
