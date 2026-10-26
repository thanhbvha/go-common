package eventbus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goccy/go-json"
)

const (
	maxEnvelopeIDBytes     = 128
	maxEnvelopeHeaderCount = 64
	maxEnvelopeHeaderBytes = 8 * 1024
	maxEnvelopeHeaderKey   = 128
	maxEnvelopeHeaderValue = 1024
	maxEnvelopeKeyBytes    = 512
	maxRetryDelay          = 30 * 24 * time.Hour
	maxCreatedAtClockSkew  = 5 * time.Minute
)

var ErrRetryPolicyPanic = errors.New("eventbus: panic in retry policy")

func validateEnvelopeRoute(env *Envelope, kind TopologyKind, jobType string) (string, error) {
	if env == nil {
		return "invalid_envelope", errors.New("eventbus: envelope is nil")
	}
	if env.ID == "" || len(env.ID) > maxEnvelopeIDBytes {
		return "invalid_envelope_id", errors.New("eventbus: invalid envelope ID")
	}
	if env.Kind != kind {
		return "topology_kind_mismatch", errors.New("eventbus: envelope topology kind mismatch")
	}
	if env.Type != jobType {
		return "topology_type_mismatch", errors.New("eventbus: envelope topology type mismatch")
	}
	if env.Attempt < 1 || env.MaxAttempts < 1 {
		return "invalid_attempts", errors.New("eventbus: invalid envelope attempts")
	}
	if (!env.legacy && env.CreatedAt <= 0) || env.RunAt < 0 ||
		(env.CreatedAt > 0 && env.CreatedAt > time.Now().Add(maxCreatedAtClockSkew).UnixMilli()) ||
		(env.RunAt > 0 && env.CreatedAt > 0 && env.RunAt < env.CreatedAt) {
		return "invalid_timestamps", errors.New("eventbus: invalid envelope timestamps")
	}
	if len(env.Payload) == 0 || !json.Valid(env.Payload) {
		return "invalid_payload", errors.New("eventbus: invalid envelope payload")
	}
	if env.DedupeTTL < 0 || (env.DedupeTTL > 0 && env.DedupeKey == "") {
		return "invalid_dedupe", errors.New("eventbus: invalid envelope dedupe metadata")
	}
	if len(env.OrderingKey) > maxEnvelopeKeyBytes || len(env.ConcurrencyKey) > maxEnvelopeKeyBytes || len(env.DedupeKey) > maxEnvelopeKeyBytes {
		return "invalid_keys", errors.New("eventbus: envelope key metadata too large")
	}
	if len(env.Headers) > maxEnvelopeHeaderCount {
		return "invalid_headers", errors.New("eventbus: too many envelope headers")
	}
	total := 0
	for key, value := range env.Headers {
		if len(key) == 0 || len(key) > maxEnvelopeHeaderKey || len(value) > maxEnvelopeHeaderValue {
			return "invalid_headers", errors.New("eventbus: invalid envelope header")
		}
		if err := validateName("header key", key); err != nil {
			return "invalid_headers", errors.New("eventbus: invalid envelope header")
		}
		for _, r := range value {
			if r < 0x20 || r == 0x7f {
				return "invalid_headers", errors.New("eventbus: invalid envelope header")
			}
		}
		total += len(key) + len(value)
		if total > maxEnvelopeHeaderBytes {
			return "invalid_headers", errors.New("eventbus: envelope headers too large")
		}
	}
	return "", nil
}

func safeIs(err, target error) (match bool) {
	defer func() {
		if recover() != nil {
			match = false
		}
	}()
	return errors.Is(err, target)
}

func safeErrorCode(err error) (code string) {
	if err == nil {
		return ""
	}
	defer func() {
		if recover() != nil {
			code = "error_render_panic"
		}
	}()
	if !safeErrorInspectable(err) {
		return "error_render_panic"
	}
	switch {
	case safeIs(err, ErrHandlerPanic):
		return "handler_panic"
	case safeIs(err, ErrRetryPolicyPanic):
		return "retry_policy_panic"
	case safeIs(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case safeIs(err, context.Canceled):
		return "context_canceled"
	default:
		return "handler_error"
	}
}

func safeErrorInspectable(err error) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	_ = err.Error()
	return true
}

func safeErrorType(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T", err)
}

func safeAs[T error](err error) (target T, ok bool) {
	defer func() {
		if recover() != nil {
			var zero T
			target, ok = zero, false
		}
	}()
	ok = errors.As(err, &target)
	return target, ok
}

func safeRetryBackoff(policy RetryPolicy, attempt int) (delay time.Duration, policyErr error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		_ = recover()
		delay = 0
		policyErr = ErrRetryPolicyPanic
	}()
	delay = policy.NextBackoff(attempt)
	completed = true
	return normalizeRetryDelay(delay), nil
}

func normalizeRetryDelay(delay time.Duration) time.Duration {
	if delay < 0 {
		return 0
	}
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}
