# GraphQL Web Example

A shared bounded GraphQL core runs on net/http, Fiber, Gin and Echo.

```powershell
go run ./examples/graphql_web -stack=nethttp
# Local Playground + introspection:
go run ./examples/graphql_web -stack=nethttp -development
```

Stacks: nethttp, fiber, gin, echo. The endpoint is
`http://localhost:3000/query`. Playground at `/` exists only with
`-development`. HTTP GET queries are disabled by default; Playground uses POST.

```powershell
Invoke-RestMethod -Method Post http://localhost:3000/query -ContentType application/json -Body '{"query":"{ posts(first: 2, offset: 0) { id title author { id name } } }"}'
```

All list fields accept first (default 20, range 1..100) and offset
(default 0, non-negative). Invalid pagination is a public BAD_USER_INPUT error.
List costs multiply child complexity with overflow protection. The in-memory
service demonstrates bounded output; real database services should apply
pagination in their query, not load all rows first.

The core's RequestSetup creates a typed Users/PostsByAuthor loader container on
every request, using the core deadline. Both relationship directions batch.
Loader identity and caches never cross requests. Backend query logging was
removed to avoid exporting IDs.

The example dataset is public and does not authenticate users. Before adapting
it to protected data, wrap the endpoint in the repository auth middleware,
then principal-based rate limiting, then authorize tenant/object access in
resolvers. See the [module wiring and migration guide](../../graphql/README.md).
All hosting stacks set read/write/idle limits; Fiber also sets BodyLimit before
fasthttp buffering. Deadline cancellation requires cooperative backend code.

Regenerate after schema changes, without manually editing generated outputs:

```powershell
# From examples/graphql_web:
go run github.com/99designs/gqlgen generate
```

The config disables automatic module tidy to preserve unrelated library
dependencies. gqlgen stays pinned to the version in the root go.mod.
