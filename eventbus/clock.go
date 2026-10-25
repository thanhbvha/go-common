package eventbus

import "time"

// Clock defines a time provider for testing purposes.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
