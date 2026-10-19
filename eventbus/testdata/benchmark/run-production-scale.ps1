param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("redis", "nats", "kafka")]
    [string]$Driver,
    [ValidateSet("immediate", "delayed", "retry10", "retry50")]
    [string]$Workload = "immediate",
    [ValidateSet(100000, 1000000)]
    [int]$Messages = 100000,
    [ValidateSet(1, 10, 100)]
    [int]$Concurrency = 10,
    [ValidateSet(1, 10, 100)]
    [int]$PayloadKB = 1,
    [switch]$Preload,
    [timespan]$Duration = [timespan]::Zero,
    [ValidateRange(0, 1000000)]
    [int]$Rate = 0,
    [timespan]$Delay = "00:00:05",
    [timespan]$Timeout = "06:00:00",
    [timespan]$SampleEvery = "00:00:05",
    [timespan]$BrokerRestartAfter = [timespan]::Zero,
    [timespan]$KafkaSecondWorkerAfter = [timespan]::Zero,
    [string]$ArtifactRoot = "eventbus/testdata/benchmark/artifacts",
    [string]$RunLabel = "",
    [switch]$ValidateOnly
)

$ErrorActionPreference = "Stop"

function Convert-DurationArgument([timespan]$Value) {
    return ([string]::Format([Globalization.CultureInfo]::InvariantCulture, "{0:R}s", $Value.TotalSeconds))
}

function Get-FreeLoopbackPort {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
    $listener.Start()
    try { return ([Net.IPEndPoint]$listener.LocalEndpoint).Port } finally { $listener.Stop() }
}

function Assert-Inputs {
    if ($Duration -lt [timespan]::Zero -or $Timeout -le [timespan]::Zero -or $SampleEvery -le [timespan]::Zero) { throw "durations must be positive (duration may be zero)" }
    if ($Duration -eq [timespan]::Zero -and $Rate -ne 0) { throw "Rate requires Duration" }
    if ($Duration -gt [timespan]::Zero -and $Rate -le 0) { throw "Duration requires a positive Rate" }
    if ($Duration -gt [timespan]::Zero -and $Preload) { throw "Preload cannot be combined with Duration" }
    if ($Duration -gt [timespan]::Zero -and $Timeout -le $Duration) { throw "Timeout must exceed Duration" }
    if ($Duration -gt [timespan]::Zero -and [math]::Ceiling($Duration.TotalSeconds * $Rate) -gt 100000000) { throw "Duration and Rate exceed the 100000000-message verification bound" }
    if ($BrokerRestartAfter -lt [timespan]::Zero -or $KafkaSecondWorkerAfter -lt [timespan]::Zero) { throw "fault action times must not be negative" }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and $Driver -ne "kafka") { throw "KafkaSecondWorkerAfter requires Driver kafka" }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and $Preload) { throw "KafkaSecondWorkerAfter cannot be combined with Preload" }
    if ($BrokerRestartAfter -ge $Timeout -or $KafkaSecondWorkerAfter -ge $Timeout) { throw "fault action times must be less than Timeout" }
    if ($RunLabel -and $RunLabel -notmatch '^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$') { throw "RunLabel contains unsafe characters" }
}

function Add-JSONLine([string]$Path, $Value) {
    $Value | ConvertTo-Json -Compress -Depth 8 | Add-Content -LiteralPath $Path -Encoding utf8
}

function Read-JSONLines([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) { return @() }
    $lineNumber = 0
    return @(Get-Content -LiteralPath $Path | ForEach-Object {
        $lineNumber++
        if (-not [string]::IsNullOrWhiteSpace($_)) {
            try { $_ | ConvertFrom-Json } catch { throw "invalid JSONL at ${Path}:$lineNumber" }
        }
    })
}

Assert-Inputs
if ($ValidateOnly) {
    [ordered]@{
        valid = $true; driver = $Driver
        mode = if ($Duration -gt [timespan]::Zero) { "duration" } else { "messages" }
        preload = [bool]$Preload; rate = $Rate
    } | ConvertTo-Json -Compress
    return
}
$composeFile = Join-Path $PSScriptRoot "compose.yaml"
$project = "ebbench-$Driver-$([guid]::NewGuid().ToString('N').Substring(0, 12))"
if ($project -notmatch '^ebbench-(redis|nats|kafka)-[0-9a-f]{12}$') { throw "refusing unsafe Compose project name: $project" }

$suffix = if ($Duration -gt [timespan]::Zero) { "soak-$($Duration.TotalSeconds)s-$Rate" } else { "$Messages" }
$labelSuffix = if ($RunLabel) { "-$RunLabel" } else { "" }
$runID = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssfffZ") + "-$Driver-$Workload-$suffix$labelSuffix-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
$artifactDir = Join-Path $ArtifactRoot $runID
if (Test-Path -LiteralPath $artifactDir) { throw "artifact directory already exists: $artifactDir" }
New-Item -ItemType Directory -Path $ArtifactRoot -Force | Out-Null
New-Item -ItemType Directory -Path $artifactDir | Out-Null

$manifestPath = Join-Path $artifactDir "manifest.json"
$samplesPath = Join-Path $artifactDir "benchmark-samples.jsonl"
$brokerSamplesPath = Join-Path $artifactDir "broker-samples.jsonl"
$faultsPath = Join-Path $artifactDir "fault-actions.jsonl"
$stdoutPath = Join-Path $artifactDir "result.json"
$stderrPath = Join-Path $artifactDir "benchmark.stderr.log"
$brokerInspectPath = Join-Path $artifactDir "broker-inspect.json"
$summaryPath = Join-Path $artifactDir "summary.json"
$binaryPath = Join-Path $artifactDir "eventbus-benchmark.exe"
$namespace = "p676-$Driver-$([guid]::NewGuid().ToString('N'))"

$ports = [ordered]@{ redis = Get-FreeLoopbackPort; nats = Get-FreeLoopbackPort; nats_http = Get-FreeLoopbackPort; kafka = Get-FreeLoopbackPort }
$env:REDIS_PORT = [string]$ports.redis
$env:NATS_PORT = [string]$ports.nats
$env:NATS_HTTP_PORT = [string]$ports.nats_http
$env:KAFKA_PORT = [string]$ports.kafka
$driverArgs = switch ($Driver) {
    "redis" { @("--redis-addr", "127.0.0.1:$($ports.redis)") }
    "nats" { @("--nats-urls", "nats://127.0.0.1:$($ports.nats)") }
    "kafka" { @("--kafka-brokers", "127.0.0.1:$($ports.kafka)") }
}
$benchmarkArgs = @(
    "--driver", $Driver, "--namespace", $namespace, "--payload-kb", $PayloadKB,
    "--concurrency", $Concurrency, "--workload", $Workload, "--messages", $Messages,
    "--delay", (Convert-DurationArgument $Delay), "--timeout", (Convert-DurationArgument $Timeout),
    "--sample-every", (Convert-DurationArgument $SampleEvery), "--samples-jsonl", $samplesPath
) + $driverArgs
if ($Preload) { $benchmarkArgs += "--preload" }
if ($Duration -gt [timespan]::Zero) { $benchmarkArgs += @("--duration", (Convert-DurationArgument $Duration), "--rate", $Rate) }
if ($KafkaSecondWorkerAfter -gt [timespan]::Zero) { $benchmarkArgs += @("--second-worker-after", (Convert-DurationArgument $KafkaSecondWorkerAfter)) }

$manifest = [ordered]@{
    schema_version = 2; run_id = $runID; created_utc = (Get-Date).ToUniversalTime().ToString("o")
    compose_project = $project; compose_file = $composeFile; driver = $Driver; workload = $Workload
    mode = if ($Duration -gt [timespan]::Zero) { "duration" } else { "messages" }
    messages = $Messages; duration = $Duration.ToString(); rate = $Rate; preload = [bool]$Preload
    concurrency = $Concurrency; payload_kb = $PayloadKB; delay = $Delay.ToString(); timeout = $Timeout.ToString()
    sample_every = $SampleEvery.ToString(); broker_restart_after = $BrokerRestartAfter.ToString()
    kafka_second_worker_after = $KafkaSecondWorkerAfter.ToString(); namespace = $namespace; loopback_ports = $ports
    synthetic_data = $true; shared_broker_allowed = $false; go_version = (& go version)
    docker_compose_version = (& docker compose version); cleanup = "docker compose --project-name $project --file $composeFile --profile $Driver down -v --remove-orphans"
}
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $manifestPath -Encoding utf8

$containerID = $null
$process = $null
$runError = $null
$cleanupVerified = $false
$restartIssued = $false
$startedAt = $null
try {
    & go build -o $binaryPath ./examples/eventbus/benchmark
    if ($LASTEXITCODE -ne 0) { throw "benchmark build failed" }
    & docker compose --project-name $project --file $composeFile --profile $Driver up -d --wait
    if ($LASTEXITCODE -ne 0) { throw "Compose startup failed" }
    $containerID = (& docker compose --project-name $project --file $composeFile ps -q $Driver).Trim()
    if (-not $containerID) { throw "Compose did not return a container ID for $Driver" }
    (& docker inspect $containerID) | Set-Content -LiteralPath $brokerInspectPath -Encoding utf8

    $process = Start-Process -FilePath $binaryPath -ArgumentList $benchmarkArgs -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $null = $process.Handle
    $startedAt = Get-Date
    while (-not $process.HasExited) {
        $elapsed = (Get-Date) - $startedAt
        if (-not $restartIssued -and $BrokerRestartAfter -gt [timespan]::Zero -and $elapsed -ge $BrokerRestartAfter) {
            $actionStarted = (Get-Date).ToUniversalTime()
            & docker compose --project-name $project --file $composeFile --profile $Driver restart $Driver
            $actionExit = $LASTEXITCODE
            Add-JSONLine $faultsPath ([ordered]@{ type = "broker_restart"; started_utc = $actionStarted.ToString("o"); completed_utc = (Get-Date).ToUniversalTime().ToString("o"); exit_code = $actionExit })
            $restartIssued = $true
            if ($actionExit -ne 0) { throw "scoped broker restart failed" }
        }
        $timestamp = (Get-Date).ToUniversalTime().ToString("o")
        $hostProcess = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
        $containerStats = & docker stats --no-stream --format '{{json .}}' $containerID
        $inspectSize = @(& docker inspect --size --format '{{json .SizeRw}}' $containerID)
        $diskBytes = if ($inspectSize.Count -gt 0 -and $inspectSize[0]) { $inspectSize[0] | ConvertFrom-Json } else { $null }
        Add-JSONLine $brokerSamplesPath ([ordered]@{
            timestamp = $timestamp; launcher_pid = $process.Id
            launcher_cpu_seconds = if ($hostProcess) { $hostProcess.CPU } else { $null }
            launcher_working_set_bytes = if ($hostProcess) { $hostProcess.WorkingSet64 } else { $null }
            container = if ($containerStats) { $containerStats | ConvertFrom-Json } else { $null }
            container_writable_bytes = $diskBytes
        })
        Start-Sleep -Seconds ([math]::Max(1, [int][math]::Ceiling($SampleEvery.TotalSeconds)))
        $process.Refresh()
    }
    $process.WaitForExit(); $process.Refresh()
    if ($null -eq $process.ExitCode) { throw "benchmark exit code unavailable; see $stderrPath" }
    if ($process.ExitCode -ne 0) { throw "benchmark exited with code $($process.ExitCode); see $stderrPath" }
    try { $result = Get-Content -LiteralPath $stdoutPath -Raw | ConvertFrom-Json } catch { throw "result.json is not valid JSON: $($_.Exception.Message)" }
    foreach ($field in @("driver", "namespace", "messages", "elapsed_seconds", "throughput_per_second", "duplicates")) {
        if ($null -eq $result.$field) { throw "result.json is missing required field $field" }
    }
    if ($result.driver -ne $Driver -or $result.namespace -ne $namespace) { throw "result identity does not match manifest" }
    $null = Read-JSONLines $samplesPath
    $null = Read-JSONLines $brokerSamplesPath
}
catch {
    $runError = $_
}
finally {
    & docker compose --project-name $project --file $composeFile --profile $Driver down -v --remove-orphans
    $cleanupExit = $LASTEXITCODE
    $remainingContainers = @(& docker ps -aq --filter "label=com.docker.compose.project=$project") | Where-Object { $_ }
    $remainingVolumes = @(& docker volume ls -q --filter "label=com.docker.compose.project=$project") | Where-Object { $_ }
    $cleanupVerified = $cleanupExit -eq 0 -and $remainingContainers.Count -eq 0 -and $remainingVolumes.Count -eq 0
    $resultForSummary = $null
    if (Test-Path -LiteralPath $stdoutPath) {
        try { $resultForSummary = Get-Content -LiteralPath $stdoutPath -Raw | ConvertFrom-Json } catch { }
    }
    $benchmarkSamples = try { @(Read-JSONLines $samplesPath) } catch { @() }
    $brokerSamples = try { @(Read-JSONLines $brokerSamplesPath) } catch { @() }
    $faultActions = try { @(Read-JSONLines $faultsPath) } catch { @() }
    $summary = [ordered]@{
        schema_version = 1; run_id = $runID; status = if ($runError) { "failed" } elseif (-not $cleanupVerified) { "cleanup_failed" } else { "completed" }
        result = $resultForSummary; benchmark_sample_count = $benchmarkSamples.Count; broker_sample_count = $brokerSamples.Count
        max_heap_alloc_bytes = ($benchmarkSamples | Measure-Object -Property heap_alloc_bytes -Maximum).Maximum
        max_goroutines = ($benchmarkSamples | Measure-Object -Property goroutines -Maximum).Maximum
        max_pending = ($benchmarkSamples | Where-Object topology_stats_available | Measure-Object -Property pending -Maximum).Maximum
        max_lag = ($benchmarkSamples | Where-Object topology_stats_available | Measure-Object -Property lag -Maximum).Maximum
        max_container_writable_bytes = ($brokerSamples | Measure-Object -Property container_writable_bytes -Maximum).Maximum
        fault_action_count = $faultActions.Count; broker_restart_issued = $restartIssued; cleanup_exit_code = $cleanupExit; cleanup_verified = $cleanupVerified
        remaining_container_ids = @($remainingContainers); remaining_volume_names = @($remainingVolumes)
        error = if ($runError) { $runError.Exception.Message } elseif (-not $cleanupVerified) { "scoped Compose cleanup verification failed" } else { $null }
    }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $summaryPath -Encoding utf8
}
if ($runError) { throw $runError }
if (-not $cleanupVerified) { throw "scoped Compose cleanup verification failed; see $summaryPath" }
$artifactDir
