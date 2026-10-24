package httpkit

import "github.com/google/uuid"

const maxRequestIDLength = 128

// RequestID returns an accepted caller-provided request ID or generates a new
// UUID when the value is empty, oversized, or contains unsupported characters.
func RequestID(value string) string {
	if validRequestID(value) {
		return value
	}
	return uuid.NewString()
}

func validRequestID(value string) bool {
	if value == "" || len(value) > maxRequestIDLength {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		switch c {
		case '.', '_', ':', '-':
			continue
		default:
			return false
		}
	}
	return true
}
