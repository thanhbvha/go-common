#!/usr/bin/env bash
# Create fresh fixtures only; never reuse an existing database.
set -euo pipefail
containers=()
cleanup(){ for container in "${containers[@]}"; do docker stop --timeout 10 "$container" >/dev/null || true; done; }
trap cleanup EXIT
containers+=("$(docker run --rm -d -p 127.0.0.1::5432 -e POSTGRES_DB=orm_test -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18-alpine)")
containers+=("$(docker run --rm -d -p 127.0.0.1::3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_ROOT_HOST=% -e MYSQL_DATABASE=orm_test mysql:8.4)")
ready=false
for attempt in {1..90}; do
 if docker exec "${containers[0]}" pg_isready -U postgres -d orm_test >/dev/null 2>&1 && docker exec "${containers[1]}" mysqladmin ping -h 127.0.0.1 --silent >/dev/null 2>&1; then ready=true; break; fi
 sleep 1
done
[[ "$ready" == true ]] || { for container in "${containers[@]}"; do docker logs "$container"; done; exit 1; }
pgport=$(docker port "${containers[0]}" 5432/tcp | awk -F: '{print $NF}')
myport=$(docker port "${containers[1]}" 3306/tcp | awk -F: '{print $NF}')
export ORM_INTEGRATION_REQUIRED=1 ORM_MYSQL_REQUIRED=1
export ORM_POSTGRES_DSN="host=127.0.0.1 port=$pgport user=postgres dbname=orm_test sslmode=disable"
export ORM_MYSQL_DSN="root@tcp(127.0.0.1:$myport)/orm_test"
go test -v -race -tags=integration -count=1 -timeout=5m ./db/orm/...
go run ./examples/db -backend postgres -demo all
go run ./examples/db -backend mysql -demo all
go test -v -tags=integration -run '^$' -bench BenchmarkPostgresCursor -benchtime=1s -benchmem ./db/orm
