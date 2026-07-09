package graphql

import (
	"net/http"

	"github.com/99designs/gqlgen/graphql/playground"
)

// PlaygroundHandler trả về chuẩn net/http.HandlerFunc cho giao diện GraphQL Playground
func PlaygroundHandler(title string, endpoint string) http.HandlerFunc {
	return playground.Handler(title, endpoint)
}
