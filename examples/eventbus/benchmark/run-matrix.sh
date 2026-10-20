#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../../.." && pwd)
drivers=redis
workloads=immediate,delayed,retry10,retry50
payload_kb=1,10,100
concurrency=1,10,100
messages=100000
preload=false
duration=0s
rate=0
artifact_root=eventbus/testdata/benchmark/artifacts
validate_only=false

while (($#)); do
  case "$1" in
    --drivers) drivers=${2:?}; shift 2;; --workloads) workloads=${2:?}; shift 2;;
    --payload-kb) payload_kb=${2:?}; shift 2;; --concurrency) concurrency=${2:?}; shift 2;;
    --messages) messages=${2:?}; shift 2;; --preload) preload=true; shift;;
    --duration) duration=${2:?}; shift 2;; --rate) rate=${2:?}; shift 2;;
    --artifact-root) artifact_root=${2:?}; shift 2;; --validate-only) validate_only=true; shift;;
    *) echo "unknown argument: $1" >&2; exit 2;;
  esac
done

split_validate() {
  local name=$1 value=$2; shift 2
  local -n output=$name
  IFS=',' read -ra output <<<"$value"
  ((${#output[@]})) || { echo "$name must not be empty" >&2; exit 2; }
  local item allowed found
  for item in "${output[@]}"; do
    item=${item//[[:space:]]/}; found=false
    for allowed in "$@"; do [[ $item == "$allowed" ]] && found=true; done
    [[ $found == true ]] || { echo "unsupported $name value: $item" >&2; exit 2; }
  done
}
split_validate driver_values "$drivers" memory redis nats kafka
split_validate workload_values "$workloads" immediate delayed retry10 retry50
split_validate payload_values "$payload_kb" 1 10 100
split_validate concurrency_values "$concurrency" 1 10 100
split_validate message_values "$messages" 100000 1000000
[[ " ${driver_values[*]} " != *" memory "* ]] || { echo "production matrix does not orchestrate memory" >&2; exit 2; }
duration_s=$(python3 - "$duration" <<'PY'
import re,sys
v=sys.argv[1]; m=re.fullmatch(r'(?:(\d+(?:\.\d+)?)h)?(?:(\d+(?:\.\d+)?)m)?(?:(\d+(?:\.\d+)?)s)?',v)
if not m or not any(m.groups()): raise SystemExit('invalid duration')
h,mn,s=(float(x or 0) for x in m.groups()); print(h*3600+mn*60+s)
PY
)
python3 - "$duration_s" "$rate" "$preload" "${#message_values[@]}" <<'PY'
import sys
d=float(sys.argv[1]); rate=int(sys.argv[2]); preload=sys.argv[3]=='true'; count=int(sys.argv[4])
if d == 0 and rate != 0: raise SystemExit('rate requires duration')
if d > 0 and (rate <= 0 or preload): raise SystemExit('duration requires rate and cannot use preload')
if d > 0 and count != 1: raise SystemExit('duration mode accepts exactly one messages value')
PY
if [[ $validate_only == true ]]; then
  jq -cn --arg drivers "$drivers" --arg workloads "$workloads" --arg payload "$payload_kb" --arg concurrency "$concurrency" --arg messages "$messages" \
    '{valid:true,drivers:($drivers|split(",")),workloads:($workloads|split(",")),payload_kb:($payload|split(",")|map(tonumber)),concurrency:($concurrency|split(",")|map(tonumber)),messages:($messages|split(",")|map(tonumber))}'
  exit 0
fi

matrix_id="$(date -u +%Y%m%dT%H%M%S%3NZ)-matrix-$(printf '%s' "$RANDOM$RANDOM" | sha256sum | cut -c1-8)"
matrix_dir="$artifact_root/$matrix_id"; [[ $artifact_root = /* ]] || matrix_dir="$repo_root/$matrix_dir"
mkdir -p -- "$matrix_dir"; index_path="$matrix_dir/matrix.jsonl"
runner="$repo_root/eventbus/testdata/benchmark/run-production-scale.sh"
for driver in "${driver_values[@]}"; do for workload in "${workload_values[@]}"; do for payload in "${payload_values[@]}"; do for workers in "${concurrency_values[@]}"; do for count in "${message_values[@]}"; do
  label="p${payload}-c${workers}-m${count}"
  args=(--driver "$driver" --workload "$workload" --payload-kb "$payload" --concurrency "$workers" --messages "$count" --artifact-root "$matrix_dir" --run-label "$label")
  [[ $preload == true ]] && args+=(--preload)
  [[ $duration_s != 0 ]] && args+=(--duration "$duration" --rate "$rate")
  if run_dir=$($runner "${args[@]}"); then
    jq -cn --arg driver "$driver" --arg workload "$workload" --argjson payload "$payload" --argjson concurrency "$workers" --argjson messages "$count" --arg dir "$run_dir" '{driver:$driver,workload:$workload,payload_kb:$payload,concurrency:$concurrency,messages:$messages,artifact_dir:$dir,status:"completed"}' >>"$index_path"
  else
    jq -cn --arg driver "$driver" --arg workload "$workload" --argjson payload "$payload" --argjson concurrency "$workers" --argjson messages "$count" '{driver:$driver,workload:$workload,payload_kb:$payload,concurrency:$concurrency,messages:$messages,status:"failed"}' >>"$index_path"
    exit 1
  fi
done; done; done; done; done
printf '%s\n' "$matrix_dir"
