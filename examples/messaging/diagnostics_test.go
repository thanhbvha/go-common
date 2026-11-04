package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	dbmongodb "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestFailureDiagnosticsPreserveCauseAndHideSecrets(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		cause      error
	}{
		{"standalone", "standalone", dbmongodb.ErrTransactionsUnsupported},
		{"timeout", "timed out", context.DeadlineExceeded},
		{"canceled", "canceled", context.Canceled},
		{"authentication", "authentication failed", mongo.CommandError{Code: 18, Message: "private-credential"}},
		{"authorization", "authorization failed", mongo.CommandError{Code: 13, Message: "private-credential"}},
		{"unknown", "operation failed", errors.New("private-credential")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := fmt.Errorf("mongodb://user:private-credential@localhost: %w", tc.cause)
			err := &stepError{step: "mongodb.connect", err: cause, hint: "check mongodb.uri and replicaSet"}
			if !errors.Is(err, cause) {
				t.Fatal("original error identity lost")
			}
			message := failureMessage(errors.Join(err, errors.New("private-credential cleanup")))
			if !strings.Contains(message, "mongodb.connect") || !strings.Contains(message, tc.want) || !strings.Contains(message, "replicaSet") {
				t.Fatal("missing actionable diagnostic", message)
			}
			if strings.Contains(message, "private-credential") || strings.Contains(message, "mongodb://") {
				t.Fatal("raw error/credentials exposed")
			}
		})
	}
	if message := failureMessage(errors.New("private-credential")); strings.Contains(message, "private-credential") {
		t.Fatal("unclassified error exposed")
	}
}
