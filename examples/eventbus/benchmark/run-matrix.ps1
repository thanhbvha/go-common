param(
    [ValidateSet("memory", "redis", "nats", "kafka")]
    [string[]]$Drivers = @("redis"),
    [ValidateSet("immediate", "delayed", "retry10", "retry50")]
    [string[]]$Workloads = @("immediate", "delayed", "retry10", "retry50"),
    [ValidateSet(1, 10, 100)]
    [int[]]$PayloadKB = @(1, 10, 100),
    [ValidateSet(1, 10, 100)]
    [int[]]$Concurrency = @(1, 10, 100),
    [ValidateSet(100000, 1000000)]
    [int[]]$Messages = @(100000),
    [switch]$Preload,
    [timespan]$Duration = [timespan]::Zero,
    [ValidateRange(0, 1000000)]
    [int]$Rate = 0,
    [string]$ArtifactRoot = "eventbus/testdata/benchmark/artifacts"
)

$ErrorActionPreference = "Stop"
if ($Duration -eq [timespan]::Zero -and $Rate -ne 0) { throw "Rate requires Duration" }
if ($Duration -gt [timespan]::Zero -and ($Rate -le 0 -or $Preload)) { throw "Duration requires Rate and cannot use Preload" }
if ($Drivers -contains "memory") { throw "production matrix requires disposable durable brokers; memory is not orchestrated here" }

$orchestrator = Join-Path $PSScriptRoot "../../../eventbus/testdata/benchmark/run-production-scale.ps1"
$matrixID = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ") + "-matrix-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
$matrixDir = Join-Path $ArtifactRoot $matrixID
if (Test-Path -LiteralPath $matrixDir) { throw "matrix artifact directory already exists: $matrixDir" }
New-Item -ItemType Directory -Path $matrixDir -Force | Out-Null
$indexPath = Join-Path $matrixDir "matrix.jsonl"

foreach ($driver in $Drivers) {
    foreach ($workload in $Workloads) {
        foreach ($payload in $PayloadKB) {
            foreach ($workers in $Concurrency) {
                foreach ($count in $Messages) {
                    $label = "p${payload}-c${workers}-m${count}"
                    $arguments = @("-Driver", $driver, "-Workload", $workload, "-PayloadKB", $payload, "-Concurrency", $workers, "-Messages", $count, "-ArtifactRoot", $matrixDir, "-RunLabel", $label)
                    if ($Preload) { $arguments += "-Preload" }
                    if ($Duration -gt [timespan]::Zero) { $arguments += @("-Duration", $Duration.ToString(), "-Rate", $Rate) }
                    try {
                        $runDir = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $orchestrator @arguments
                        if ($LASTEXITCODE -ne 0) { throw "orchestrator exited with $LASTEXITCODE" }
                        [ordered]@{ driver = $driver; workload = $workload; payload_kb = $payload; concurrency = $workers; messages = $count; artifact_dir = @($runDir)[-1]; status = "completed" } | ConvertTo-Json -Compress | Add-Content -LiteralPath $indexPath -Encoding utf8
                    } catch {
                        [ordered]@{ driver = $driver; workload = $workload; payload_kb = $payload; concurrency = $workers; messages = $count; status = "failed"; error = $_.Exception.Message } | ConvertTo-Json -Compress | Add-Content -LiteralPath $indexPath -Encoding utf8
                        throw
                    }
                }
            }
        }
    }
}
$matrixDir
