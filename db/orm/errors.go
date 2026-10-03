package orm

import "errors"

// ErrNotFound is returned when a record is not found in the database.
// Callers should use errors.Is(err, orm.ErrNotFound) to check.
//
// Example:
//
//	user, err := repo.FindByID(ctx, id)
//	if errors.Is(err, orm.ErrNotFound) {
//	    return xerrors.ErrNotFound
//	}
var ErrNotFound = errors.New("orm: record not found")
