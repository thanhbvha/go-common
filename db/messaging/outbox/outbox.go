// Package outbox defines durable publication records and database-neutral dispatch.
package outbox

import (
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

type State string

const (
	Pending    State = "pending"
	Processing State = "processing"
	Published  State = "published"
	Dead       State = "dead"
)

type Claim struct {
	Message    messaging.Message
	Token      string
	Attempt    int
	LeaseUntil time.Time
}
type ClaimRequest struct {
	Limit, MaxAttempts int
	Lease              time.Duration
}

type Record struct {
	Message     messaging.Message
	State       State
	Attempts    int
	CreatedAt   time.Time
	PublishedAt *time.Time
	FailedAt    *time.Time
	ErrorCode   string
	ReplayCount int
	ReplayedAt  *time.Time
}
type ListRequest struct {
	// Nil includes all tenants in this store namespace. Pointer to "" selects
	// the single-tenant/default tenant. Authorization belongs to the caller.
	TenantID *string
	State    State
	Cursor   string
	Limit    int
}
type Page struct {
	Records    []Record
	NextCursor string
}
type CleanupRequest struct {
	TenantID        *string
	PublishedBefore time.Time
	InboxBefore     time.Time
	Limit           int
}
type CleanupResult struct{ Published, Inbox int64 }
