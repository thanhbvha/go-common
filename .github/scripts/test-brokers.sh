#!/usr/bin/env bash
# All data and processes belong to newly created disposable fixtures.
set -euo pipefail
scratch=$(mktemp -d /tmp/go-common-brokers.XXXXXXXX)
pids=()
containers=()
cleanup() {
  for container in "${containers[@]}"; do docker stop --timeout 10 "$container" >/dev/null || true; done
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  [[ "$scratch" == /tmp/go-common-brokers.* ]] && rm -rf -- "$scratch"
}
trap cleanup EXIT
# Reserve candidate free ports, including Redis Cluster bus ports. Verify each
# redis-server PID before allowing any data-mutating fixture command.
mapfile -t fixture_ports < <(python3 - <<'PORTS'
import socket, random
held=[]; ports=[]
try:
 for i in range(5):
  while True:
   port=random.randrange(20000,40000)
   pair=[]
   try:
    for p in ([port,port+10000] if 1<=i<=3 else [port]):
     sock=socket.socket(); sock.bind(('127.0.0.1',p)); pair.append(sock)
   except OSError:
    for sock in pair:sock.close()
    continue
   held.extend(pair);ports.append(port);break
 print(*ports,sep='\n')
finally:
 for sock in held:sock.close()
PORTS
)
[[ "${#fixture_ports[@]}" == 5 ]] || exit 1
single_port=${fixture_ports[0]}
cluster_ports=("${fixture_ports[@]:1:3}")
kafka_port=${fixture_ports[4]}
for index in 0 1 2 3; do
  port=${fixture_ports[$index]}
  mkdir "$scratch/$port"
  opts=()
  if [[ "$index" != 0 ]]; then opts=(--cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 1000 --cluster-announce-ip 127.0.0.1); fi
  redis-server --bind 127.0.0.1 --port "$port" --dir "$scratch/$port" --save '' --appendonly no "${opts[@]}" >"$scratch/$port.log" 2>&1 &
  pids+=("$!")
  ready=false
  for attempt in {1..50}; do
    kill -0 "${pids[$index]}" 2>/dev/null || { cat "$scratch/$port.log"; exit 1; }
    server_pid=$(redis-cli -p "$port" info server 2>/dev/null | tr -d '\r' | awk -F: '$1=="process_id"{print $2}') || true
    if [[ "$server_pid" == "${pids[$index]}" ]]; then ready=true; break; fi
    sleep .1
  done
  [[ "$ready" == true ]] || { cat "$scratch/$port.log"; exit 1; }
done
cluster_addresses=()
for port in "${cluster_ports[@]}"; do cluster_addresses+=("127.0.0.1:$port"); done
redis-cli --cluster create "${cluster_addresses[@]}" --cluster-replicas 0 --cluster-yes >/dev/null
ready=false
for attempt in {1..50}; do
  if redis-cli -p "${cluster_ports[0]}" cluster info | tr -d '\r' | grep -q '^cluster_state:ok'; then ready=true; break; fi
  sleep .1
done
[[ "$ready" == true ]] || exit 1
containers+=("$(docker run --rm -d -p 127.0.0.1::4222 nats:2.11-alpine -js)")
nats_port=$(docker port "${containers[0]}" 4222/tcp | awk -F: '{print $NF}')
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
ready=false
for attempt in {1..60}; do
  if docker exec "${containers[1]}" /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1; then ready=true; break; fi
  sleep 1
done
[[ "$ready" == true ]] || { docker logs "${containers[1]}"; exit 1; }
export EVENTBUS_REDIS_ADDR="127.0.0.1:$single_port"
export EVENTBUS_REDIS_CLUSTER_ADDRS=$(IFS=,; echo "${cluster_addresses[*]}")
export EVENTBUS_KAFKA_BROKERS="127.0.0.1:$kafka_port"
export EVENTBUS_INTEGRATION_REQUIRED=1 REDIS_INTEGRATION_REQUIRED=1
export WS_REDIS_INTEGRATION=1 WS_NATS_INTEGRATION=1 WS_KAFKA_INTEGRATION=1
export WS_KAFKA_BROKERS="$EVENTBUS_KAFKA_BROKERS"
export WS_REDIS_ADDR="$EVENTBUS_REDIS_ADDR" WS_NATS_URL="nats://127.0.0.1:$nats_port"
# p=1 prevents topology-changing suites from interfering with adjacent packages.
go test -race -p 1 -tags=integration -count=1 -timeout=15m ./eventbus/driver/... ./redis ./cache ./cron/... ./auth ./kafka ./nats ./websocket/pubsub/... ./websocket/integration/... ./examples/websocket/cluster
