package core

import (
	"github.com/thanhbvha/go-common/v2/logger"
	"github.com/thanhbvha/go-common/v2/websocket/limiter"
)

var lifecycleLogLimiter = limiter.NewRateLimiter(20, 40)

func logLifecycle(message string, args ...any) {
	if lifecycleLogLimiter.Allow() {
		logger.InfoAsync(message, args...)
	}
}
