#!/usr/bin/env bash
set -euo pipefail
output=${1:-artifacts/govulncheck.json}
mkdir -p "$(dirname "$output")"
# JSON mode always exits zero for findings. Preserve that scan, then use the
# scanner's text handler on the SAME results to enforce reachable findings.
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 -json ./... > "$output"
"${PYTHON:-python3}" .github/scripts/release-evidence.py scan "$output"
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 -mode=convert < "$output"
