package eventbus

import "time"

// PermanentError wraps an error to indicate that the failure is fatal
// and the job should immediately be sent to the DLQ without further retries.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string {
	return "eventbus: permanent error"
}

func (e *PermanentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// MarkPermanent wraps an error as a PermanentError.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// RetryAfterError wraps an error to instruct the engine to retry the job
// after a specific duration, overriding the default RetryPolicy.
type RetryAfterError struct {
	Err   error
	After time.Duration
}

func (e *RetryAfterError) Error() string {
	return "eventbus: retry requested"
}

func (e *RetryAfterError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// MarkRetryAfter wraps an error to request a retry after a specific delay.
func MarkRetryAfter(err error, after time.Duration) error {
	if err == nil {
		return nil
	}
	return &RetryAfterError{Err: err, After: after}
}

// SkipError indicates that the job was skipped intentionally.
// The engine will treat it as a success (ACK) rather than a failure.
type SkipError struct {
	Reason string
}

func (e *SkipError) Error() string {
	return "eventbus: skipped"
}

// MarkSkip returns a SkipError.
func MarkSkip(reason string) error {
	return &SkipError{Reason: reason}
}
