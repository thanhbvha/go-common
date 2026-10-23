#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)

driver=""
workload="immediate"
messages=100000
concurrency=10
payload_kb=1
preload=false
duration="0s"
rate=0
delay="5s"
timeout_duration="6h"
sample_every="5s"
sample_topology=true
broker_restart_after="0s"
broker_restart_every="0s"
kafka_second_worker_after="0s"
artifact_root="eventbus/testdata/benchmark/artifacts"
run_label=""
validate_only=false
cleanup_self_test=false
evidence_self_test=false

usage() {
  cat <<'EOF'
Usage: run-production-scale.sh --driver redis|nats|kafka [options]
  --workload immediate|delayed|retry10|retry50
  --messages 100000|1000000
  --concurrency 1|10|100
  --payload-kb 1|10|100
  --preload
  --duration 2h --rate 5000
  --delay 5s --timeout 6h --sample-every 5s
  --sample-topology true|false
  --broker-restart-after 30m
  --broker-restart-every 30m
  --kafka-second-worker-after 10m
  --artifact-root PATH --run-label LABEL
  --validate-only | --cleanup-self-test | --evidence-self-test
EOF
}

while (($#)); do
  case "$1" in
    --driver) driver=${2:?}; shift 2 ;;
    --workload) workload=${2:?}; shift 2 ;;
    --messages) messages=${2:?}; shift 2 ;;
    --concurrency) concurrency=${2:?}; shift 2 ;;
    --payload-kb) payload_kb=${2:?}; shift 2 ;;
    --preload) preload=true; shift ;;
    --duration) duration=${2:?}; shift 2 ;;
    --rate) rate=${2:?}; shift 2 ;;
    --delay) delay=${2:?}; shift 2 ;;
    --timeout) timeout_duration=${2:?}; shift 2 ;;
    --sample-every) sample_every=${2:?}; shift 2 ;;
    --sample-topology) sample_topology=${2:?}; shift 2 ;;
    --broker-restart-after) broker_restart_after=${2:?}; shift 2 ;;
    --broker-restart-every) broker_restart_every=${2:?}; shift 2 ;;
    --kafka-second-worker-after) kafka_second_worker_after=${2:?}; shift 2 ;;
    --artifact-root) artifact_root=${2:?}; shift 2 ;;
    --run-label) run_label=${2:?}; shift 2 ;;
    --validate-only) validate_only=true; shift ;;
    --cleanup-self-test) cleanup_self_test=true; shift ;;
    --evidence-self-test) evidence_self_test=true; shift ;;
    --help|-h) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

duration_seconds() {
  python3 - "$1" <<'PY'
import re, sys
value = sys.argv[1].strip()
if re.fullmatch(r"\d+(?:\.\d+)?", value):
    print(float(value)); raise SystemExit
m = re.fullmatch(r"(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?", value)
if not m or not any(m.groups()):
    raise SystemExit(f"invalid duration: {value}")
h, minute, sec = (float(x or 0) for x in m.groups())
print(h * 3600 + minute * 60 + sec)
PY
}

duration_arg() {
  printf '%ss' "$(duration_seconds "$1")"
}

in_values() {
  local needle=$1; shift
  local value
  for value in "$@"; do [[ $needle == "$value" ]] && return 0; done
  return 1
}

duration_s=$(duration_seconds "$duration")
timeout_s=$(duration_seconds "$timeout_duration")
sample_s=$(duration_seconds "$sample_every")
restart_s=$(duration_seconds "$broker_restart_after")
restart_every_s=$(duration_seconds "$broker_restart_every")
second_worker_s=$(duration_seconds "$kafka_second_worker_after")
duration_mode=$(python3 -c "print('true' if float('$duration_s') > 0 else 'false')")
restart_requested=$(python3 -c "print('true' if float('$restart_s') > 0 else 'false')")
second_worker_requested=$(python3 -c "print('true' if float('$second_worker_s') > 0 else 'false')")

[[ $cleanup_self_test == true || $evidence_self_test == true ]] || in_values "$driver" redis nats kafka || { echo "driver must be redis, nats, or kafka" >&2; exit 2; }
in_values "$sample_topology" true false || { echo "sample-topology must be true or false" >&2; exit 2; }
in_values "$workload" immediate delayed retry10 retry50 || { echo "unsupported workload: $workload" >&2; exit 2; }
in_values "$messages" 100000 1000000 || { echo "messages must be 100000 or 1000000" >&2; exit 2; }
in_values "$concurrency" 1 10 100 || { echo "concurrency must be 1, 10, or 100" >&2; exit 2; }
in_values "$payload_kb" 1 10 100 || { echo "payload-kb must be 1, 10, or 100" >&2; exit 2; }
python3 - "$duration_s" "$timeout_s" "$sample_s" "$restart_s" "$restart_every_s" "$second_worker_s" "$rate" "$preload" "$driver" "$run_label" <<'PY'
import re, sys
d, timeout, sample, restart, restart_every, worker = map(float, sys.argv[1:7])
rate = int(sys.argv[7]); preload = sys.argv[8] == "true"; driver = sys.argv[9]; label = sys.argv[10]
if d < 0 or timeout <= 0 or sample <= 0 or restart < 0 or restart_every < 0 or worker < 0: raise SystemExit("invalid duration")
if d == 0 and rate != 0: raise SystemExit("rate requires duration")
if d > 0 and rate <= 0: raise SystemExit("duration requires a positive rate")
if d > 0 and preload: raise SystemExit("preload cannot be combined with duration")
if d > 0 and timeout <= d: raise SystemExit("timeout must exceed duration")
if d > 0 and d * rate > 100_000_000: raise SystemExit("duration and rate exceed verification bound")
if worker > 0 and driver != "kafka": raise SystemExit("second worker requires kafka")
if worker > 0 and preload: raise SystemExit("second worker cannot be combined with preload")
if restart_every > 0 and restart <= 0: raise SystemExit("repeated restart requires broker-restart-after")
if restart_every > 0 and d <= 0: raise SystemExit("repeated restart requires duration mode")
if restart >= timeout or worker >= timeout: raise SystemExit("fault action times must be less than timeout")
if d > 0 and (restart >= d or worker >= d): raise SystemExit("fault action times must be less than duration")
if label and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", label): raise SystemExit("unsafe run label")
PY

if [[ $cleanup_self_test == true ]]; then
  sleep 60 & child=$!
  kill "$child"
  wait "$child" 2>/dev/null || true
  kill -0 "$child" 2>/dev/null && { echo "benchmark process cleanup self-test failed" >&2; exit 1; }
  echo "benchmark process cleanup self-test passed"
  exit 0
fi

validate_evidence() {
  local benchmark_samples=$1 broker_samples=$2 require_preload=$3
  [[ -s $benchmark_samples && -s $broker_samples ]] || return 1
  jq -e -s 'any(.[]; .type=="sample" and .phase=="measurement")' "$benchmark_samples" >/dev/null || return 1
  if [[ $require_preload == true ]]; then
    jq -e -s 'any(.[]; .type=="sample" and .phase=="preload")' "$benchmark_samples" >/dev/null || return 1
  fi
  jq -e -s 'length>0 and all(.[]; (.phase=="preload" or .phase=="measurement") and (.named_volume_allocated_bytes|type)=="number" and .named_volume_allocated_bytes>=0) and any(.[]; .phase=="measurement")' "$broker_samples" >/dev/null || return 1
  if [[ $require_preload == true ]]; then
    jq -e -s 'any(.[]; .phase=="preload")' "$broker_samples" >/dev/null || return 1
  fi
}

if [[ $evidence_self_test == true ]]; then
  command -v jq >/dev/null || { echo "missing command: jq" >&2; exit 1; }
  evidence_dir=$(mktemp -d "/tmp/eventbus-evidence.XXXXXX")
  trap 'rm -rf -- "$evidence_dir"' EXIT
  printf '%s\n' '{"type":"sample","phase":"preload"}' '{"type":"sample","phase":"measurement"}' >"$evidence_dir/benchmark.jsonl"
  printf '%s\n' '{"phase":"preload","named_volume_allocated_bytes":100}' '{"phase":"measurement","named_volume_allocated_bytes":123}' >"$evidence_dir/broker.jsonl"
  validate_evidence "$evidence_dir/benchmark.jsonl" "$evidence_dir/broker.jsonl" true
  printf '%s\n' '{"named_volume_allocated_bytes":null}' >"$evidence_dir/invalid-broker.jsonl"
  if validate_evidence "$evidence_dir/benchmark.jsonl" "$evidence_dir/invalid-broker.jsonl" true 2>/dev/null; then
    echo "invalid volume evidence accepted" >&2
    exit 1
  fi
  printf '%s\n' '{"phase":"","named_volume_allocated_bytes":123}' >"$evidence_dir/invalid-phase.jsonl"
  if validate_evidence "$evidence_dir/benchmark.jsonl" "$evidence_dir/invalid-phase.jsonl" true 2>/dev/null; then
    echo "invalid broker phase accepted" >&2
    exit 1
  fi
  printf '%s\n' '{"phase":"preload","named_volume_allocated_bytes":123}' >"$evidence_dir/no-measurement-broker.jsonl"
  if validate_evidence "$evidence_dir/benchmark.jsonl" "$evidence_dir/no-measurement-broker.jsonl" true 2>/dev/null; then
    echo "missing measurement broker evidence accepted" >&2
    exit 1
  fi
  echo "production evidence validation self-test passed"
  exit 0
fi

if [[ $validate_only == true ]]; then
  jq -cn --arg driver "$driver" --arg mode "$([[ $duration_mode == true ]] && echo duration || echo messages)" --argjson preload "$preload" --argjson rate "$rate" \
    '{valid:true,driver:$driver,mode:$mode,preload:$preload,rate:$rate}'
  exit 0
fi

for command in go docker jq python3; do command -v "$command" >/dev/null || { echo "missing command: $command" >&2; exit 1; }; done

compose_file="$script_dir/compose.yaml"
project="ebbench-$driver-$(printf '%s' "$RANDOM$RANDOM$(date +%s%N)" | sha256sum | cut -c1-12)"
[[ $project =~ ^ebbench-(redis|nats|kafka)-[0-9a-f]{12}$ ]] || { echo "unsafe Compose project: $project" >&2; exit 1; }
suffix=$messages
if [[ $duration_mode == true ]]; then suffix="soak-${duration_s}s-$rate"; fi
label_suffix=""; [[ -n $run_label ]] && label_suffix="-$run_label"
run_id="$(date -u +%Y%m%dT%H%M%S%3NZ)-$driver-$workload-$suffix$label_suffix-$(printf '%s' "$RANDOM$RANDOM" | sha256sum | cut -c1-8)"
artifact_dir="$artifact_root/$run_id"
[[ $artifact_root = /* ]] || artifact_dir="$repo_root/$artifact_dir"
[[ ! -e $artifact_dir ]] || { echo "artifact directory already exists: $artifact_dir" >&2; exit 1; }
mkdir -p -- "$artifact_dir"

manifest_path="$artifact_dir/manifest.json"
samples_path="$artifact_dir/benchmark-samples.jsonl"
broker_samples_path="$artifact_dir/broker-samples.jsonl"
faults_path="$artifact_dir/fault-actions.jsonl"
stdout_path="$artifact_dir/result.json"
stderr_path="$artifact_dir/benchmark.stderr.log"
heap_profile_path="$artifact_dir/heap.pprof"
goroutine_profile_path="$artifact_dir/goroutines.txt"
broker_inspect_path="$artifact_dir/broker-inspect.json"
broker_logs_path="$artifact_dir/broker.log"
summary_path="$artifact_dir/summary.json"
binary_path="$artifact_dir/eventbus-benchmark"
namespace="p676-$driver-$(printf '%s' "$RANDOM$RANDOM$(date +%s%N)" | sha256sum | cut -c1-24)"
planned_messages=$messages
if [[ $duration_mode == true ]]; then planned_messages=$(python3 -c "import math; print(math.ceil($duration_s * $rate))"); fi

free_port() { python3 - <<'PY'
import socket
s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()
PY
}
redis_port=$(free_port); nats_port=$(free_port); nats_http_port=$(free_port); kafka_port=$(free_port)
export REDIS_PORT=$redis_port NATS_PORT=$nats_port NATS_HTTP_PORT=$nats_http_port KAFKA_PORT=$kafka_port

driver_args=()
data_path="/data"
case "$driver" in
  redis) driver_args=(--redis-addr "127.0.0.1:$redis_port") ;;
  nats) driver_args=(--nats-urls "nats://127.0.0.1:$nats_port") ;;
  kafka) driver_args=(--kafka-brokers "127.0.0.1:$kafka_port"); data_path="/var/lib/kafka/data" ;;
esac
benchmark_args=(--driver "$driver" --namespace "$namespace" --payload-kb "$payload_kb" --concurrency "$concurrency" --workload "$workload" --messages "$messages" --delay "$(duration_arg "$delay")" --timeout "$(duration_arg "$timeout_duration")" --sample-every "$(duration_arg "$sample_every")" "--sample-topology=$sample_topology" --samples-jsonl "$samples_path" --heap-profile "$heap_profile_path" --goroutine-profile "$goroutine_profile_path" "${driver_args[@]}")
[[ $preload == true ]] && benchmark_args+=(--preload)
[[ $duration_mode == true ]] && benchmark_args+=(--duration "$(duration_arg "$duration")" --rate "$rate")
[[ $second_worker_requested == true ]] && benchmark_args+=(--second-worker-after "$(duration_arg "$kafka_second_worker_after")")

jq -n --arg run_id "$run_id" --arg project "$project" --arg compose "$compose_file" --arg driver "$driver" --arg workload "$workload" --arg namespace "$namespace" \
  --argjson messages "$messages" --argjson concurrency "$concurrency" --argjson payload_kb "$payload_kb" --argjson preload "$preload" --argjson rate "$rate" --arg duration "$duration" \
  --argjson kafka_partitions "$([[ $driver == kafka ]] && echo "$concurrency" || echo 0)" \
  --argjson kafka_delay_batch_size "$([[ $driver == kafka ]] && echo 1000 || echo 0)" \
  --argjson nats_delay_batch_size "$([[ $driver == nats ]] && echo 1000 || echo 0)" \
  --argjson redis_delay_batch_size "$([[ $driver == redis ]] && echo 1000 || echo 0)" \
  --arg broker_restart_after "$broker_restart_after" --arg broker_restart_every "$broker_restart_every" --argjson sample_topology "$sample_topology" \
  '{schema_version:2,run_id:$run_id,compose_project:$project,compose_file:$compose,driver:$driver,workload:$workload,messages:$messages,concurrency:$concurrency,payload_kb:$payload_kb,preload:$preload,rate:$rate,duration:$duration,namespace:$namespace,kafka_partitions:$kafka_partitions,kafka_delay_batch_size:$kafka_delay_batch_size,nats_delay_batch_size:$nats_delay_batch_size,redis_delay_batch_size:$redis_delay_batch_size,broker_restart_after:$broker_restart_after,broker_restart_every:$broker_restart_every,topology_sampling:$sample_topology,synthetic_data:true,shared_broker_allowed:false}' >"$manifest_path"

process_pid=""; container_id=""; restart_issued=false; restart_count=0; next_restart_s=$restart_s; process_forced_stop=false; cleanup_verified=false; run_error=""
cleanup() {
  local exit_code=$?
  set +e
  if [[ -n $process_pid ]] && kill -0 "$process_pid" 2>/dev/null; then
    kill "$process_pid" 2>/dev/null; sleep 1
    kill -0 "$process_pid" 2>/dev/null && kill -KILL "$process_pid" 2>/dev/null
    wait "$process_pid" 2>/dev/null
    process_forced_stop=true
  fi
  docker compose --project-name "$project" --file "$compose_file" --profile "$driver" logs --no-color "$driver" >"$broker_logs_path" 2>&1 || true
  docker compose --project-name "$project" --file "$compose_file" --profile "$driver" down -v --remove-orphans >/dev/null 2>&1
  cleanup_exit=$?
  remaining_containers=$(docker ps -aq --filter "label=com.docker.compose.project=$project" | jq -Rsc 'split("\n")|map(select(length>0))')
  remaining_volumes=$(docker volume ls -q --filter "label=com.docker.compose.project=$project" | jq -Rsc 'split("\n")|map(select(length>0))')
  [[ $cleanup_exit -eq 0 && $remaining_containers == "[]" && $remaining_volumes == "[]" ]] && cleanup_verified=true
  [[ -s $stdout_path ]] && result_json=$(jq -c . "$stdout_path" 2>/dev/null || echo null) || result_json=null
  benchmark_count=$([[ -f $samples_path ]] && wc -l <"$samples_path" || echo 0)
  broker_count=$([[ -f $broker_samples_path ]] && wc -l <"$broker_samples_path" || echo 0)
  measurement_count=$([[ -f $samples_path ]] && jq -s '[.[]|select(.phase=="measurement")]|length' "$samples_path" 2>/dev/null || echo 0)
  preload_count=$([[ -f $samples_path ]] && jq -s '[.[]|select(.phase=="preload")]|length' "$samples_path" 2>/dev/null || echo 0)
  volume_count=$([[ -f $broker_samples_path ]] && jq -s '[.[]|select((.named_volume_allocated_bytes|type)=="number")]|length' "$broker_samples_path" 2>/dev/null || echo 0)
  max_volume=$([[ -f $broker_samples_path ]] && jq -s '[.[]|.named_volume_allocated_bytes|select(type=="number")]|max // null' "$broker_samples_path" 2>/dev/null || echo null)
  fault_count=$([[ -f $faults_path ]] && wc -l <"$faults_path" || echo 0)
  [[ -z $run_error && $exit_code -ne 0 ]] && run_error="runner exited with code $exit_code"
  execution_status=completed; acceptance_status=passed
  [[ -n $run_error ]] && execution_status=failed && acceptance_status=failed
  [[ $cleanup_verified != true ]] && execution_status=cleanup_failed && acceptance_status=failed
  jq -n --arg run_id "$run_id" --arg execution "$execution_status" --arg acceptance "$acceptance_status" --argjson result "$result_json" \
    --argjson benchmark_count "$benchmark_count" --argjson broker_count "$broker_count" --argjson measurement_count "$measurement_count" --argjson preload_count "$preload_count" --argjson volume_count "$volume_count" --argjson max_volume "$max_volume" --argjson fault_count "$fault_count" --argjson restart "$restart_issued" --argjson restart_count "$restart_count" --argjson forced "$process_forced_stop" --argjson cleanup "$cleanup_verified" --arg error "$run_error" \
    '{schema_version:2,run_id:$run_id,execution_status:$execution,acceptance_status:$acceptance,result:$result,benchmark_sample_count:$benchmark_count,measurement_sample_count:$measurement_count,preload_sample_count:$preload_count,broker_sample_count:$broker_count,named_volume_sample_count:$volume_count,named_volume_measurement:"du_allocated_1k_blocks",max_named_volume_allocated_bytes:$max_volume,fault_action_count:$fault_count,broker_restart_issued:$restart,broker_restart_count:$restart_count,benchmark_process_forced_stop:$forced,cleanup_verified:$cleanup,error:(if $error=="" then null else $error end)}' >"$summary_path"
  set -e
  [[ -n $run_error || $cleanup_verified != true ]] && exit 1
  printf '%s\n' "$artifact_dir"
}
trap cleanup EXIT
trap 'run_error="line $LINENO: $BASH_COMMAND"' ERR

go build -o "$binary_path" ./examples/eventbus/benchmark
if ! docker compose --project-name "$project" --file "$compose_file" --profile "$driver" up -d --wait; then
  docker compose --project-name "$project" --file "$compose_file" --profile "$driver" logs --no-color "$driver" >"$broker_logs_path" 2>&1 || true
  run_error="Compose startup failed; see $broker_logs_path"
  exit 1
fi
container_id=$(docker compose --project-name "$project" --file "$compose_file" ps -q "$driver")
[[ -n $container_id ]] || { run_error="Compose did not return a container ID"; exit 1; }
docker inspect "$container_id" >"$broker_inspect_path"
docker compose --project-name "$project" --file "$compose_file" --profile "$driver" logs --no-color "$driver" >"$broker_logs_path" 2>&1 || true

"$binary_path" "${benchmark_args[@]}" >"$stdout_path" 2>"$stderr_path" &
process_pid=$!
started_epoch=$(date +%s)
sample_sleep=$(python3 -c "import math; print(max(1, math.ceil($sample_s)))")
while kill -0 "$process_pid" 2>/dev/null; do
  elapsed=$(( $(date +%s) - started_epoch ))
  if python3 -c "raise SystemExit(not ($next_restart_s > 0 and $elapsed >= $next_restart_s))"; then
    before=$(tail -n 1 "$samples_path" 2>/dev/null || echo '{}')
    produced_before=$(jq -r '.produced // 0' <<<"$before"); completed_before=$(jq -r '.completed // 0' <<<"$before"); lag_before=$(jq -r '.lag // 0' <<<"$before")
    if (( produced_before > 0 && (produced_before < planned_messages || produced_before > completed_before) )); then
      action_started=$(date -u +%FT%T.%NZ)
      docker compose --project-name "$project" --file "$compose_file" --profile "$driver" restart "$driver"
      after=$(tail -n 1 "$samples_path" 2>/dev/null || echo '{}')
      jq -cn --arg started "$action_started" --arg completed "$(date -u +%FT%T.%NZ)" --argjson pb "$produced_before" --argjson cb "$completed_before" --argjson lb "$lag_before" --argjson pa "$(jq -r '.produced // 0' <<<"$after")" --argjson ca "$(jq -r '.completed // 0' <<<"$after")" --argjson la "$(jq -r '.lag // 0' <<<"$after")" \
        '{type:"broker_restart",started_utc:$started,completed_utc:$completed,exit_code:0,active_work:true,produced_before:$pb,completed_before:$cb,lag_before:$lb,produced_after:$pa,completed_after:$ca,lag_after:$la}' >>"$faults_path"
      restart_issued=true
      restart_count=$((restart_count + 1))
      if python3 -c "raise SystemExit(not ($restart_every_s > 0))"; then
        next_restart_s=$(python3 -c "print($next_restart_s + $restart_every_s)")
      else
        next_restart_s=0
      fi
    fi
  fi
  host_cpu=$(ps -p "$process_pid" -o cputime= 2>/dev/null | xargs || true)
  host_rss=$(ps -p "$process_pid" -o rss= 2>/dev/null | xargs || true)
  container_stats=$(docker stats --no-stream --format '{{json .}}' "$container_id" 2>/dev/null || echo null)
  volume_kib=$(docker exec "$container_id" sh -c "du -sk '$data_path' 2>/dev/null | cut -f1" 2>/dev/null || true)
  if [[ $volume_kib =~ ^[0-9]+$ ]]; then volume_bytes=$((volume_kib * 1024)); else volume_bytes=null; fi
  phase=$(tail -n 1 "$samples_path" 2>/dev/null | jq -r '.phase // empty' 2>/dev/null || true)
  if [[ $phase != preload && $phase != measurement ]]; then
    phase=measurement
    [[ $preload == true ]] && phase=preload
  fi
  jq -cn --arg timestamp "$(date -u +%FT%T.%NZ)" --arg phase "$phase" --argjson pid "$process_pid" --arg cpu "$host_cpu" --arg rss "$host_rss" --argjson container "$container_stats" --argjson volume "$volume_bytes" \
    '{timestamp:$timestamp,phase:$phase,launcher_pid:$pid,launcher_cpu:$cpu,launcher_rss_kb:$rss,container:$container,named_volume_measurement:"du_allocated_1k_blocks",named_volume_allocated_bytes:$volume}' >>"$broker_samples_path"
  sleep "$sample_sleep"
done
wait "$process_pid" || { code=$?; process_pid=""; run_error="benchmark exited with code $code; see $stderr_path"; exit 1; }
process_pid=""
jq -e . "$stdout_path" >/dev/null
[[ -s $heap_profile_path ]] || { run_error="post-drain heap profile is missing"; exit 1; }
[[ -s $goroutine_profile_path ]] || { run_error="post-drain goroutine profile is missing"; exit 1; }
validate_evidence "$samples_path" "$broker_samples_path" "$preload" || { run_error="measurement phase or named-volume evidence validation failed"; exit 1; }
jq -e --arg driver "$driver" --arg namespace "$namespace" --argjson sample_topology "$sample_topology" '
  .driver==$driver and .namespace==$namespace and .topology_sampling==$sample_topology and .final_stats_available==true and
  (.post_drain_heap_bytes|type)=="number" and .post_drain_heap_bytes>=0 and
  (.post_drain_goroutines|type)=="number" and .post_drain_goroutines>0 and
  (.expected_retries == (.retry_candidates_failed_at_attempt_one + .retry_first_seen_after_attempt_one)) and
  ((.primary_worker_handled + .second_worker_handled) == (.messages + .synthetic_retry_failures + .duplicates)) and
  .final_pending==0 and .final_delayed==0 and .final_in_flight==0 and .final_dlq==0 and .final_lag==0
' "$stdout_path" >/dev/null || { run_error="result identity, retry accounting, handler accounting, or final settled-state validation failed"; exit 1; }
if [[ $driver == kafka ]]; then jq -e '.kafka_delay_batch_size==1000' "$stdout_path" >/dev/null || { run_error="Kafka benchmark delay batch size was not recorded as 1000"; exit 1; }; fi
if [[ $driver == nats ]]; then jq -e '.nats_delay_batch_size==1000' "$stdout_path" >/dev/null || { run_error="NATS benchmark delay batch size was not recorded as 1000"; exit 1; }; fi
if [[ $driver == redis ]]; then jq -e '.redis_delay_batch_size==1000' "$stdout_path" >/dev/null || { run_error="Redis benchmark delay batch size was not recorded as 1000"; exit 1; }; fi
if [[ $second_worker_requested == true ]]; then jq -e '.second_worker_started==true and .second_worker_active_work==true and .second_worker_handled>0' "$stdout_path" >/dev/null || { run_error="Kafka second worker did not process work after joining the active group"; exit 1; }; fi
if [[ $restart_requested == true ]]; then
  [[ $restart_issued == true ]] || { run_error="requested broker restart was not issued"; exit 1; }
  jq -e 'select(.active_work==true)' "$faults_path" >/dev/null || { run_error="broker restart did not occur during active work"; exit 1; }
fi
if python3 -c "raise SystemExit(not ($restart_every_s > 0))"; then
  expected_restarts=$(python3 -c "import math; print(max(0, math.ceil(($duration_s - $restart_s) / $restart_every_s)))")
  [[ $restart_count -eq $expected_restarts ]] || { run_error="issued $restart_count broker restarts, expected $expected_restarts"; exit 1; }
fi
