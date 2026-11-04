package messaging

import (
	"errors"
)

var (
	ErrInvalidMessage      = errors.New("messaging: invalid message")
	ErrInvalidConfig       = errors.New("messaging: invalid configuration")
	ErrIdentityConflict    = errors.New("messaging: identity conflict")
	ErrLeaseLost           = errors.New("messaging: lease lost")
	ErrNotFound            = errors.New("messaging: record not found")
	ErrAlreadyRunning      = errors.New("messaging: dispatcher already started")
	ErrClosed              = errors.New("messaging: dispatcher closed")
	ErrSchema              = errors.New("messaging: unsupported or missing schema")
	ErrInfrastructurePanic = errors.New("messaging: infrastructure panic")
)
