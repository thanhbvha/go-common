# GraphQL Web Example

A shared bounded GraphQL core runs on net/http, Fiber, Gin and Echo.

```powershell
go run ./examples/graphql -stack=nethttp
# Local Playground + introspection:
go run ./examples/graphql -stack=nethttp -development
```

Stacks: nethttp, fiber, gin, echo. The endpoint is
`http://localhost:3000/query`. Playground at `/` exists only with
`-development`. HTTP GET queries are disabled by default; Playground uses POST.

Run the query in another PowerShell terminal while the server is running:

```powershell
$body = @{ query = '{ posts(first: 2, offset: 0) { id title author { id name } } }' } | ConvertTo-Json -Compress
$response = Invoke-RestMethod -Method Post -Uri http://localhost:3000/query -ContentType application/json -Body $body
$response | ConvertTo-Json -Depth 10
```

`Invoke-RestMethod` deserializes JSON into PowerShell objects. Its default table
view may display `@{posts=System.Object[]}` for nested arrays; this does not mean
the response is empty. `ConvertTo-Json -Depth 10` shows the full nested response.
This query returns posts 101 and 102, both authored by Alice. For a readable table:

```powershell
$response.data.posts | Select-Object id, title, @{Name='author'; Expression={$_.author.name}} | Format-Table -AutoSize
```

```text
 id title               author
 -- -----               ------
101 Alice's First Post  Alice
102 Alice's Second Post Alice
```

Check `$response.errors` for GraphQL failures, even when HTTP returns 200. The JSON
view above includes both `data` and `errors`, if present.

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
# From examples/graphql:
go run github.com/99designs/gqlgen generate
```

The config disables automatic module tidy to preserve unrelated library
dependencies. gqlgen stays pinned to the version in the root go.mod.
