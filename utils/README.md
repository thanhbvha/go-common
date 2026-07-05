# utils

A comprehensive toolset utilizing Go Generics, context safety, and graceful shutdown.

```go
import (
    "github.com/thanhbvha/go-common/utils/str"
    "github.com/thanhbvha/go-common/utils/slice"
    "github.com/thanhbvha/go-common/utils/graceful"
)

// str
random := str.Random(8)
slug := str.Slugify("Xin chào Việt Nam") // xin-chao-viet-nam

// slice (Generics)
uniqueInts := slice.Unique([]int{1, 2, 2, 3}) // [1, 2, 3]
isFound := slice.Contains([]string{"a", "b"}, "a") // true

// Graceful Shutdown
graceful.Register(func(ctx context.Context) error {
    log.Println("Closing database...")
    return db.Close()
})

graceful.Wait(10 * time.Second) // Blocks until SIGTERM/SIGINT
```
