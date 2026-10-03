package mongodb

import "errors"

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
