package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNetHTTPGraphQLHandler(t *testing.T) {
	body := `{"query":"{ posts { id author { id name } } }"}`
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	newNetHTTPHandler().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Data struct {
			Posts []struct {
				Author struct {
					Name string `json:"name"`
				} `json:"author"`
			} `json:"posts"`
		} `json:"data"`
		Errors []any `json:"errors"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Errors) != 0 {
		t.Fatalf("GraphQL errors = %#v", response.Errors)
	}
	if len(response.Data.Posts) != 3 || response.Data.Posts[0].Author.Name != "Alice" {
		t.Fatalf("unexpected GraphQL response: %#v", response.Data.Posts)
	}
}
