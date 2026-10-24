# GraphQL Web Example

One GraphQL core `http.Handler` runs directly on standard `net/http` or through
the Fiber, Gin, and Echo adapters.

```powershell
go run ./examples/graphql_web -stack=nethttp
```

Valid stacks are `nethttp`, `fiber`, `gin`, and `echo`. Every stack exposes:

- Playground: `http://localhost:3000/`
- GraphQL endpoint: `http://localhost:3000/query`

```powershell
Invoke-RestMethod -Method Post http://localhost:3000/query -ContentType application/json -Body '{"query":"{ posts { id title author { id name } } }"}'
```

A fresh DataLoader is created for every request so cached or authorization-bound
objects cannot cross request boundaries. Framework adapters inject that loader
through `ContextSetup` and typed context helpers.

The Playground is intended for development. Disable or protect it in production
when schema exploration is not meant to be public.
