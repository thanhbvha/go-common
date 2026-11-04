package outbox

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

type Publisher interface {
	Publish(context.Context, messaging.Message) error
}

// Claim commits ownership before returning. No claims may be returned alongside
// an error, including an ambiguous commit. Settlement must compare token AND an
// unexpired lease. Expired final-attempt claims must be moved to Dead.
type Store interface {
	Claim(context.Context, ClaimRequest) ([]Claim, error)
	MarkPublished(context.Context, Claim) error
	Retry(context.Context, Claim, time.Duration, string) error
	Quarantine(context.Context, Claim, string) error
}

type Admin interface {
	Get(context.Context, string, string) (Record, error) // tenant, message ID
	List(context.Context, ListRequest) (Page, error)
	Replay(context.Context, string, []string) (int64, error) // tenant, IDs
	Cleanup(context.Context, CleanupRequest) (CleanupResult, error)
	Stats(context.Context, *string) (messaging.Stats, error)
}
