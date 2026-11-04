#!/usr/bin/env bash
# Only disposable containers created by this script are mutated or removed.
set -euo pipefail
postgres_version=${MESSAGING_POSTGRES_VERSION:-18}
case "$postgres_version" in 16|17|18) ;; *) echo 'MESSAGING_POSTGRES_VERSION must be 16, 17 or 18' >&2; exit 1;; esac
containers=()
cleanup() {
  for container in "${containers[@]}"; do docker stop --timeout 10 "$container" >/dev/null || true; done
}
trap cleanup EXIT
containers+=("$(docker run --rm -d -p 127.0.0.1::5432 -e POSTGRES_DB=messaging_test -e POSTGRES_HOST_AUTH_METHOD=trust "postgres:$postgres_version-alpine")")
containers+=("$(docker run --rm -d -p 127.0.0.1::6379 redis:7-alpine)")
# Kafka advertises its host port, so select a candidate port before startup.
kafka_port=$(python3 - <<'PY'
import socket
with socket.socket() as candidate:
    candidate.bind(('127.0.0.1', 0))
    print(candidate.getsockname()[1])
PY
)
containers+=("$(docker run --rm -d -p "127.0.0.1:$kafka_port:9092" \
 -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
 -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
 -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$kafka_port" \
 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
 -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
 -e KAFKA_HEAP_OPTS='-Xms256m -Xmx512m' apache/kafka:3.9.1)")
containers+=("$(docker run --rm -d -p 127.0.0.1::27017 mongo:8.0.16 mongod --replSet messaging_ci --bind_ip_all --setParameter enableTestCommands=1)")
containers+=("$(docker run --rm -d -p 127.0.0.1::3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_ROOT_HOST=% -e MYSQL_DATABASE=messaging_test mysql:8.4)")
ready=false
for attempt in {1..90}; do
  if docker exec "${containers[0]}" pg_isready -U postgres -d messaging_test >/dev/null 2>&1 && \
     docker exec "${containers[1]}" redis-cli ping >/dev/null 2>&1 && \
     docker exec "${containers[2]}" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1 && \
     docker exec "${containers[3]}" mongosh --quiet --eval 'quit(db.adminCommand({ping:1}).ok===1?0:1)' >/dev/null 2>&1 && \
     docker exec "${containers[4]}" mysql -h127.0.0.1 -uroot -e 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { for container in "${containers[@]}"; do docker logs "$container"; done; exit 1; }
pgport=$(docker port "${containers[0]}" 5432/tcp | awk -F: '{print $NF}')
redisport=$(docker port "${containers[1]}" 6379/tcp | awk -F: '{print $NF}')
mysqlport=$(docker port "${containers[4]}" 3306/tcp | awk -F: '{print $NF}')
mongoport=$(docker port "${containers[3]}" 27017/tcp | awk -F: '{print $NF}')
docker exec "${containers[3]}" mongosh --quiet --eval 'rs.initiate({_id:"messaging_ci",members:[{_id:0,host:"localhost:27017"}]})' >/dev/null
docker exec "${containers[3]}" mongosh --quiet --eval 'for(let i=0;i<60;i++){if(db.hello().isWritablePrimary){quit(0)}sleep(500)}quit(1)' >/dev/null
export ORM_MYSQL_DSN="root@tcp(127.0.0.1:$mysqlport)/messaging_test"
export MESSAGING_MYSQL_REQUIRED=1
export ORM_POSTGRES_DSN="host=127.0.0.1 port=$pgport user=postgres dbname=messaging_test sslmode=disable"
export EVENTBUS_REDIS_ADDR="127.0.0.1:$redisport"
export EVENTBUS_KAFKA_BROKERS="127.0.0.1:$kafka_port"
export MESSAGING_INTEGRATION_REQUIRED=1 MESSAGING_BROKERS_REQUIRED=1
unset MONGODB_TEST_FAILOVER MONGODB_STANDALONE_URI MONGODB_TEST_SECURITY
export MONGODB_INTEGRATION_URI="mongodb://127.0.0.1:$mongoport/?directConnection=true&replicaSet=messaging_ci&serverSelectionTimeoutMS=10000"
export MESSAGING_MONGODB_REQUIRED=1 MONGODB_TEST_FAILPOINTS=1
go test -v -race -p 1 -tags=integration -count=1 -timeout=5m ./db/messaging/... ./examples/messaging
go run -race ./examples/messaging -driver all -broker all
