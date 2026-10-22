#!/usr/bin/env bash
set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
runner="$script_dir/run-production-scale.sh"
matrix="$script_dir/../../../examples/eventbus/benchmark/run-matrix.sh"
bash -n "$runner" "$matrix" "$0"

result=$($runner --driver kafka --duration 1m --rate 100 --kafka-second-worker-after 30s --validate-only)
jq -e '.mode=="duration"' <<<"$result" >/dev/null
finite_result=$($runner --driver redis --validate-only)
jq -e '.mode=="messages"' <<<"$finite_result" >/dev/null

if $runner --driver redis --duration 1m --rate 100 --preload --validate-only >/dev/null 2>&1; then echo "invalid preload duration accepted" >&2; exit 1; fi
if $runner --driver nats --kafka-second-worker-after 30s --validate-only >/dev/null 2>&1; then echo "invalid non-Kafka worker accepted" >&2; exit 1; fi
if $runner --driver kafka --duration 2h --rate 1000000 --validate-only >/dev/null 2>&1; then echo "unbounded duration accepted" >&2; exit 1; fi
if $runner --driver kafka --duration 1m --rate 100 --kafka-second-worker-after 1m --validate-only >/dev/null 2>&1; then echo "fault at duration boundary accepted" >&2; exit 1; fi
if $runner --driver kafka --duration 1m --rate 100 --broker-restart-every 10s --validate-only >/dev/null 2>&1; then echo "restart cadence without initial restart accepted" >&2; exit 1; fi
$runner --driver kafka --duration 2h --rate 100 --broker-restart-after 30m --broker-restart-every 30m --kafka-second-worker-after 15m --validate-only >/dev/null
$runner --driver nats --sample-topology false --validate-only >/dev/null
if $runner --driver nats --sample-topology maybe --validate-only >/dev/null 2>&1; then echo "invalid topology sampling flag accepted" >&2; exit 1; fi

matrix_result=$($matrix --drivers redis,nats --workloads immediate,retry10 --payload-kb 1,10 --concurrency 10,100 --messages 100000,1000000 --validate-only)
jq -e '.drivers|length==2' <<<"$matrix_result" >/dev/null
jq -e '.messages|length==2' <<<"$matrix_result" >/dev/null
if $matrix --drivers redis --messages 100000,1000000 --duration 1m --rate 100 --validate-only >/dev/null 2>&1; then echo "duration matrix accepted multiple messages" >&2; exit 1; fi

cleanup_result=$($runner --cleanup-self-test)
[[ $cleanup_result == "benchmark process cleanup self-test passed" ]]

evidence_result=$($runner --evidence-self-test)
[[ $evidence_result == "production evidence validation self-test passed" ]]

space_root=$(mktemp -d "/tmp/eventbus benchmark scripts.XXXXXX")
trap 'rm -rf -- "$space_root"' EXIT
$runner --driver redis --artifact-root "$space_root" --validate-only >/dev/null

echo "benchmark Bash validation passed"
