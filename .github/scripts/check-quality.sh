#!/usr/bin/env bash
set -euo pipefail
bash .github/scripts/check-boundaries.sh
unformatted=$(gofmt -l $(git ls-files '*.go'))
if [ -n "$unformatted" ]; then
  echo "These files are not gofmt-formatted:" >&2
  echo "$unformatted" >&2
  exit 1
fi
go mod verify
go build ./...
go test -race -count=1 -timeout=180s ./...
go vet ./...
go mod tidy
git diff --exit-code -- go.mod go.sum
