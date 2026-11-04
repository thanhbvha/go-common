package inbox

import (
	"context"
)

// Processor commits a receipt and callback effects in one native transaction.
// Drivers return success only after commit or a matching committed duplicate.
// Callback failure must roll back both the receipt and business effects.
type Processor[T any] interface {
	Consume(context.Context, Receipt, func(T) error) (Result, error)
}
