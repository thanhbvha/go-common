package logger_test

import (
	"testing"

	"github.com/thanhbvha/go-common/logger"
)

func TestLoggerWith(t *testing.T) {
	l := logger.New(logger.DefaultOptions())
	defer l.Close()

	child := l.With("service", "auth", "version", "v1")
	// child phải share cùng resources với parent nhưng không modify original
	if child == l {
		t.Error("child logger should be a different instance")
	}

	// Because fields in Logger are unexported, we can't directly check logChan in _test package.
	// But we can check that it doesn't panic when we log.
	child.Info("test message with pre-attached fields")
	// child.InfoAsync("test message with pre-attached fields", "key", "value")
}
