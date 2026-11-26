package core

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain fails the package when any test leaves a goroutine running.
func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }
