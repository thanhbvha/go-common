package transport

import "strings"

const (
	MaxDLQReasonBytes        = 64
	MaxDLQErrorCodeBytes     = 64
	MaxEncodedDLQRecordBytes = 2 * 1024 * 1024
)

// NormalizeDecisionMetadata returns bounded fixed-code metadata suitable for
// logs and durable DLQ records. Cause is deliberately never rendered.
func NormalizeDecisionMetadata(decision Decision) (reason, errorCode string) {
	reason = normalizeMetadataCode(decision.Reason, "delivery_failure", MaxDLQReasonBytes)
	errorCode = normalizeMetadataCode(decision.ErrorCode, "delivery_error", MaxDLQErrorCodeBytes)
	return reason, errorCode
}

func normalizeMetadataCode(value, fallback string, limit int) string {
	if value == "" || len(value) > limit || strings.TrimSpace(value) != value {
		return fallback
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			continue
		}
		return fallback
	}
	return value
}
