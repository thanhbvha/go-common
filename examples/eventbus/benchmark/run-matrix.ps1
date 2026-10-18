param(
    [ValidateSet("memory", "redis", "nats", "kafka")]
    [string]$Driver = "memory",
    [int]$Messages = 1000,
    [string]$Output = "eventbus-benchmark-$Driver.jsonl",
    [string]$NamespacePrefix = "phase6_bench"
)

$ErrorActionPreference = "Stop"
$payloads = 1, 10, 100
$concurrencyLevels = 1, 10, 100
$workloads = "immediate", "delayed", "retry10", "retry50"
if (Test-Path -LiteralPath $Output) {
    throw "output already exists: $Output"
}

foreach ($payload in $payloads) {
    foreach ($concurrency in $concurrencyLevels) {
        foreach ($workload in $workloads) {
            $namespace = "$NamespacePrefix`_$Driver`_${payload}k_c$concurrency`_$workload"
            $raw = go run ./examples/eventbus/benchmark --driver $Driver --namespace $namespace `
                --payload-kb $payload --concurrency $concurrency --workload $workload `
                --messages $Messages
            if ($LASTEXITCODE -ne 0) {
                throw "benchmark failed: $Driver/$payload/$concurrency/$workload"
            }
            $raw | ConvertFrom-Json | ConvertTo-Json -Compress | Add-Content -LiteralPath $Output
        }
    }
}
