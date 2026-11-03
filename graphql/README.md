# GraphQL

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

A bounded HTTP query/mutation wrapper around gqlgen. The core implements
`http.Handler`; Fiber, Gin and Echo adapters preserve the same core guards.
GraphQL authorization, backend capacity and deployment policy still belong to
the application.

## Server

```go
cfg := graphql.DefaultConfig()
cfg.EnableTelemetry = true
cfg.RequestSetup = func(ctx context.Context) (context.Context, error) {
    // Create typed loaders using this context, after authentication and after
    // the core has installed the request deadline. Return a derived context.
    return ctx, nil
}
srv, err := graphql.NewServer(executableSchema, cfg)
if err != nil {
    return err
}
mux.Handle("/query", srv)
```

| Config | Default |
| --- | --- |
| MaxBodyBytes | 1 MiB (also bounds GET query parameters) |
| ParserTokenLimit | 10,000 |
| MaxDepth | 15 field levels, root fields at level 1 |
| MaxFields | 1,000 expanded field occurrences, including aliases/repeated fragments |
| ComplexityLimit | 1,000 |
| RequestTimeout | 10 seconds |
| MaxConcurrentRequests | 128 per Server |
| QueryCacheSize | 256 parsed documents per Server |
| EnableGET / EnableIntrospection / EnableSuggestions | false |
| EnableTelemetry | false |

Zero numeric values select defaults; negative values are rejected.
Fragment traversal also has a finite visit budget (at least 10,000 visits).
Selection limits run before complexity and on cache hits. Applications must
configure gqlgen schema complexity functions for expensive list fields; field
counts alone do not bound result sizes. Paginate lists and reject oversized pages.

JSON POST and OPTIONS are enabled. GET is opt-in and only supports queries.
WebSocket, SSE, subscription operations, multipart uploads and APQ registration
are not supported. This is a breaking change from the old example defaults.
Introspection is opt-in. Register `PlaygroundHandler` only in a deliberate
development configuration; the core never installs a Playground route.

The body limit is enforced before JSON decoding, including unknown-length bodies.
Oversized requests return HTTP 413; saturation returns 503 immediately.
The core limits its active requests, not all connections to the hosting HTTP
server. Configure connection/header limits, ReadHeaderTimeout, ReadTimeout,
WriteTimeout and IdleTimeout on the hosting server or reverse proxy.
The deadline is cooperative: backend/resolver code must observe context. The core
does not run detached resolver goroutines to simulate forced cancellation.
`RequestSetup` must return a context derived from its argument, preserving
identity, trace, deadline and cancellation.

## Request-scoped DataLoader

```go
loader, err := graphql.NewDataLoader(requestCtx, fetchUsersBatch, graphql.LoaderConfig{})
if err != nil {
    return err
}
user, err := loader.Load(resolverCtx, userID)
values, perKeyErrors := loader.LoadAll(resolverCtx, userIDs)
```

The private implementation uses pinned `vikstrous/dataloadgen v0.0.10`.
Defaults: 1 ms batch window, 100 keys/batch, 1,000 admitted keys over the lifetime
of a loader, and four backend batches executing concurrently. Limits are per
loader, not process-wide; size backend pools for the number of active requests
and loaders. `LoadAll` enqueues before waiting and does not launch a goroutine
per key. Its output allocation is proportional to the caller's input length.

Batch functions return a map keyed by the requested IDs. Results/errors preserve
input ordering, including duplicates. Missing keys return `ErrNotFound`.
Values, missing keys and backend errors are memoized for the request.
Use `Clear(key)`, `ClearAll()` or `Clear(key)` followed by `Prime(key, value)`
after successful mutations. Clear does not reset the lifetime key budget; primed
keys consume that budget too. Exceeding it returns `ErrLoaderLimit`.

Create loaders once per request, after authentication, through `RequestSetup`.
Never share them across users/tenants/requests. The backend always receives the
constructor's request context, not the first field caller's context. Already
cancelled caller/request contexts are rejected before enqueue. A caller cancelled
after enqueue does not cancel the shared batch: waits end when the batch completes
or the backend honors the request cancellation/deadline. The dependency does not
provide immediate cancellation of an individual waiter.

Batch panic values are never included in responses, logs or returned errors.
The wrapper recovers before the dependency's default panic formatter.

## Error and telemetry contract

Responses expose only message, path, locations, `extensions.code` and
`extensions.http_status`. Internal errors and all xerrors 5xx are masked.
An xerrors 4xx message is explicitly public: do not put credentials, PII, causes,
or backend details in it. Arbitrary resolver gqlerror messages/extensions are
not trusted. Parse/variable-validation errors use fixed messages without input
values; malformed JSON never echoes the body.

HTTP status follows gqlgen transport semantics for engine/resolver responses;
`http_status` in an extension does not change the HTTP response status.
In particular, partial data with field errors can use HTTP 200. Pre-transport
guards return explicit HTTP errors. Cancelled operations return an error
envelope rather than a JSON null response.

EnableTelemetry creates one `GraphQL request` span covering admission, decode,
validation and execution. Only operation type is attached, never client-provided
operation names, query text, variables, headers or error causes.
Metrics use the `go-common/graphql` meter:
`graphql.request.duration` (seconds), `graphql.request.inflight`,
`graphql.request.rejected`, and `graphql.request.errors`.
The only label is operation type (query/mutation/unknown); validation rejection
may be unknown. Error counts include GraphQL field errors, not just HTTP failures.
Configure the application's OTel providers/exporters separately.

## Adapters and access control

Gin and Echo retain the original request context. Fiber always retains
`c.UserContext()`, even without a ContextSetup callback. Callbacks must derive
from the supplied context and must not retain framework context objects.
For Fiber, set `fiber.Config.BodyLimit` to the same body limit: fasthttp buffers
the request before the core runs. Fiber does not offer net/http's full
client-disconnect cancellation semantics. Use net/http, Gin or Echo when that
behavior is required.

Authenticate before calling the GraphQL server. Resolve object/tenant permissions
on every privileged resolver; context injection and cached data are not
authorization. Reuse `auth/middleware/*` and `ratelimit/middleware/*`.
For example, for net/http:

```go
protected := nethttpauth.Middleware(manager)(
    nethttplimit.Middleware(limiter, policy, func(r *http.Request) string {
        // Use an authenticated principal/tenant from r.Context().
        return authenticatedPrincipalKey(r.Context())
    })(srv),
)
mux.Handle("/query", protected)
```

Rate-limit middleware requires an explicit backend-failure policy. Protected
GraphQL endpoints should normally use `ratelimit.FailClosed`, which returns 503
when the quota backend is unavailable; use `FailOpen` only for an intentional,
documented availability tradeoff. Configure explicit
CORS allowlists and CSRF protection if credentials are cookie-based. Do not
trust request-supplied user/tenant IDs or untrusted forwarding headers.

## Migration

- Handle the error from NewServer; its result is now `*graphql.Server`, not
  `*handler.Server`. Mutable gqlgen transport setters are intentionally unavailable.
- Replace `ConfigDL` with `LoaderConfig`, pass the request context to
  NewDataLoader, and handle its constructor error.
- Move loader construction into RequestSetup and store a typed container
  in application-owned context keys. The example uses Users and PostsByAuthor.
- Handle ErrNotFound explicitly where a nullable field should return nil instead.
- Opt into GET/introspection deliberately; migrate subscription/upload/APQ users
  separately before deploying this HTTP-only release.
- The example defaults to no Playground; use `-development` locally.

## Verification and benchmarks

Run from the repository root:

```powershell
go test ./graphql/... ./examples/graphql/...
go test -race -count=3 ./graphql/... ./examples/graphql/...
go test ./graphql -run '^$' -bench . -benchmem -count=3
go test ./...
go vet ./...
```

Benchmark results and limitations are recorded in [BENCHMARKS.md](BENCHMARKS.md).
No universal RPS/p99 target is asserted without a representative workload.
