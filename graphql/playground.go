package graphql

import (
	"net/http"

	"github.com/99designs/gqlgen/graphql/playground"
)

// PlaygroundHandler returns a standard net/http.HandlerFunc for the GraphQL Playground interface
func PlaygroundHandler(title string, endpoint string) http.HandlerFunc {
	return playground.Handler(title, endpoint)
}
