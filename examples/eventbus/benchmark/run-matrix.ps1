param(
    [string]$Drivers = "redis",
    [string]$Workloads = "immediate,delayed,retry10,retry50",
    [string]$PayloadKB = "1,10,100",
    [string]$Concurrency = "1,10,100",
    [string]$Messages = "100000",
    [switch]$Preload,
    [timespan]$Duration = [timespan]::Zero,
    [ValidateRange(0, 1000000)]
    [int]$Rate = 0,
    [string]$ArtifactRoot = "eventbus/testdata/benchmark/artifacts",
    [switch]$ValidateOnly
)

$ErrorActionPreference = "Stop"
function Convert-MatrixValues([string]$Name, [string]$Value, [string[]]$Allowed) {
    $values = @($Value.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    if ($values.Count -eq 0) { throw "$Name must not be empty" }
    foreach ($item in $values) {
        if ($Allowed -notcontains $item) { throw "unsupported ${Name} value: $item" }
    }
    return $values
}

$driverValues = Convert-MatrixValues "Drivers" $Drivers @("memory", "redis", "nats", "kafka")
$workloadValues = Convert-MatrixValues "Workloads" $Workloads @("immediate", "delayed", "retry10", "retry50")
$payloadValues = Convert-MatrixValues "PayloadKB" $PayloadKB @("1", "10", "100")
$concurrencyValues = Convert-MatrixValues "Concurrency" $Concurrency @("1", "10", "100")
$messageValues = Convert-MatrixValues "Messages" $Messages @("100000", "1000000")
if ($Duration -eq [timespan]::Zero -and $Rate -ne 0) { throw "Rate requires Duration" }
if ($Duration -gt [timespan]::Zero -and ($Rate -le 0 -or $Preload)) { throw "Duration requires Rate and cannot use Preload" }
if ($Duration -gt [timespan]::Zero -and $messageValues.Count -ne 1) { throw "Duration mode accepts exactly one Messages value because production is rate-controlled" }
if ($driverValues -contains "memory") { throw "production matrix requires disposable durable brokers; memory is not orchestrated here" }
if ($ValidateOnly) {
    [ordered]@{ valid = $true; drivers = $driverValues; workloads = $workloadValues; payload_kb = @($payloadValues | ForEach-Object { [int]$_ }); concurrency = @($concurrencyValues | ForEach-Object { [int]$_ }); messages = @($messageValues | ForEach-Object { [int]$_ }) } | ConvertTo-Json -Compress
    return
}

$orchestrator = Join-Path $PSScriptRoot "../../../eventbus/testdata/benchmark/run-production-scale.ps1"
$matrixID = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ") + "-matrix-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
$matrixDir = Join-Path $ArtifactRoot $matrixID
if (Test-Path -LiteralPath $matrixDir) { throw "matrix artifact directory already exists: $matrixDir" }
New-Item -ItemType Directory -Path $matrixDir -Force | Out-Null
$indexPath = Join-Path $matrixDir "matrix.jsonl"

foreach ($driver in $driverValues) {
    foreach ($workload in $workloadValues) {
        foreach ($payload in $payloadValues) {
            foreach ($workers in $concurrencyValues) {
                foreach ($count in $messageValues) {
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
