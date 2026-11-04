package main

import (
	"context"
	"errors"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	dbmongodb "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Keep the original cause for errors.Is/As, but never render driver messages:
// they can contain connection strings, credentials or document contents.
type stepError struct {
	step string
	err  error
	hint string
}

func (e *stepError) Error() string {
	return e.step + ": " + safeReason(e.err) + "; " + e.hint
}
func (e *stepError) Unwrap() error { return e.err }

func safeReason(err error) string {
	switch {
	case errors.Is(err, dbmongodb.ErrTransactionsUnsupported):
		return "MongoDB standalone does not support the required transactions"
	case errors.Is(err, dbmongodb.ErrInvalidConfig):
		return "invalid MongoDB connection configuration"
	case errors.Is(err, messaging.ErrSchema):
		return "messaging schema validation failed"
	case errors.Is(err, context.Canceled):
		return "operation canceled"
	case errors.Is(err, context.DeadlineExceeded), mongo.IsTimeout(err):
		return "connection or operation timed out"
	}
	var serverErr mongo.ServerError
	if errors.As(err, &serverErr) {
		if serverErr.HasErrorCode(18) {
			return "MongoDB authentication failed"
		}
		if serverErr.HasErrorCode(13) {
			return "MongoDB authorization failed"
		}
	}
	return "operation failed"
}

func failureMessage(err error) string {
	var step *stepError
	if errors.As(err, &step) {
		return "messaging fixture failed: " + step.Error()
	}
	return "messaging fixture failed: " + safeReason(err) + "; check database/broker configuration"
}
