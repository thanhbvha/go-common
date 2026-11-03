#!/usr/bin/env bash
# Own newly created containers only. Never run against existing databases.
set -euo pipefail
containers=()
cleanup(){ for container in "${containers[@]}"; do docker stop --timeout 10 "$container" >/dev/null || true; done; }
trap cleanup EXIT
containers+=("$(docker run --rm -d -p 127.0.0.1::5432 -e POSTGRES_DB=reference_test -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18-alpine)")
containers+=("$(docker run --rm -d -p 127.0.0.1::6379 redis:7-alpine)")
ready=false
for attempt in {1..60}; do
 if docker exec "${containers[0]}" pg_isready -U postgres -d reference_test >/dev/null 2>&1 && docker exec "${containers[1]}" redis-cli ping >/dev/null 2>&1; then ready=true; break; fi
 sleep 1
done
[[ "$ready" == true ]] || { for container in "${containers[@]}"; do docker logs "$container"; done; exit 1; }
pgport=$(docker port "${containers[0]}" 5432/tcp | awk -F: '{print $NF}')
rport=$(docker port "${containers[1]}" 6379/tcp | awk -F: '{print $NF}')
export PRODUCTION_TEST_POSTGRES_DSN="host=127.0.0.1 port=$pgport user=postgres dbname=reference_test sslmode=disable"
export PRODUCTION_TEST_REDIS_ADDR="127.0.0.1:$rport"
export PRODUCTION_SOAK_DURATION="${PRODUCTION_SOAK_DURATION:-60m}"
go test -race -tags=integration -count=1 -run '^TestProductionSoak$' -v -timeout=75m ./examples/production
