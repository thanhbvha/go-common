#!/usr/bin/env bash
set -euo pipefail
while read -r package target; do
  go test -parallel=4 "$package" -run '^$' -fuzz "^${target}$" -fuzztime "${FUZZ_TIME:-10s}"
done <<'CASES'
./utils/crypt FuzzParsePasswordHash
./auth FuzzRejectMalformedTokens
./utils/crypt FuzzDecryptMalformedCiphertext
./config FuzzLoadConfiguration
./graphql FuzzGraphQLInput
./db/orm FuzzPageValidation
./db/orm FuzzCursorDecode
./websocket/core FuzzIncomingEnvelope
./websocket/core FuzzConnectionLifecycle
./websocket/pubsub FuzzCrossNodeMessageDecode
CASES
