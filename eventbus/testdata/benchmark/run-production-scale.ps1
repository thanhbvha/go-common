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
    [timespan]$BrokerRestartEvery = [timespan]::Zero,
    [timespan]$KafkaSecondWorkerAfter = [timespan]::Zero,
    [string]$ArtifactRoot = "eventbus/testdata/benchmark/artifacts",
    [string]$RunLabel = "",
    [switch]$ValidateOnly,
    [switch]$CleanupSelfTest,
    [switch]$EvidenceSelfTest
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
    if ($BrokerRestartAfter -lt [timespan]::Zero -or $BrokerRestartEvery -lt [timespan]::Zero -or $KafkaSecondWorkerAfter -lt [timespan]::Zero) { throw "fault action times must not be negative" }
    if ($BrokerRestartEvery -gt [timespan]::Zero -and $BrokerRestartAfter -le [timespan]::Zero) { throw "BrokerRestartEvery requires BrokerRestartAfter" }
    if ($BrokerRestartEvery -gt [timespan]::Zero -and $Duration -le [timespan]::Zero) { throw "BrokerRestartEvery requires Duration mode" }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and $Driver -ne "kafka") { throw "KafkaSecondWorkerAfter requires Driver kafka" }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and $Preload) { throw "KafkaSecondWorkerAfter cannot be combined with Preload" }
    if ($BrokerRestartAfter -ge $Timeout -or $KafkaSecondWorkerAfter -ge $Timeout) { throw "fault action times must be less than Timeout" }
    if ($Duration -gt [timespan]::Zero -and ($BrokerRestartAfter -ge $Duration -or $KafkaSecondWorkerAfter -ge $Duration)) { throw "fault action times must be less than Duration" }
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

function Get-LatestBenchmarkState([string]$Path) {
    $samples = @(Read-JSONLines $Path)
    if ($samples.Count -eq 0) { return $null }
    return $samples[-1]
}

function Stop-BenchmarkProcess($Process) {
    if ($null -eq $Process -or $Process.HasExited) { return $false }
    Stop-Process -Id $Process.Id -Force -ErrorAction Stop
    $Process.WaitForExit()
    $Process.Refresh()
    return $true
}

function Assert-Evidence([string]$BenchmarkPath, [string]$BrokerPath, [bool]$RequirePreload) {
    $benchmarkSamples = @(Read-JSONLines $BenchmarkPath)
    $brokerSamples = @(Read-JSONLines $BrokerPath)
    if (-not ($benchmarkSamples | Where-Object { $_.type -eq "sample" -and $_.phase -eq "measurement" })) { throw "measurement phase sample is missing" }
    if ($RequirePreload -and -not ($benchmarkSamples | Where-Object { $_.type -eq "sample" -and $_.phase -eq "preload" })) { throw "preload phase sample is missing" }
    if ($brokerSamples.Count -eq 0 -or ($brokerSamples | Where-Object { $_.phase -notin @("preload", "measurement") -or $null -eq $_.named_volume_allocated_bytes -or [int64]$_.named_volume_allocated_bytes -lt 0 })) { throw "broker phase or named-volume evidence is invalid" }
    if (-not ($brokerSamples | Where-Object { $_.phase -eq "measurement" })) { throw "measurement broker sample is missing" }
    if ($RequirePreload -and -not ($brokerSamples | Where-Object { $_.phase -eq "preload" })) { throw "preload broker sample is missing" }
}

if ($CleanupSelfTest) {
    $child = Start-Process -FilePath "powershell.exe" -ArgumentList @("-NoProfile", "-Command", "Start-Sleep -Seconds 60") -PassThru -WindowStyle Hidden
    if (-not (Stop-BenchmarkProcess $child) -or -not $child.HasExited) { throw "benchmark process cleanup self-test failed" }
    "benchmark process cleanup self-test passed"
    return
}

if ($EvidenceSelfTest) {
    $root = Join-Path ([IO.Path]::GetTempPath()) ("eventbus-evidence-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $root | Out-Null
    try {
        $benchmark = Join-Path $root "benchmark.jsonl"
        $broker = Join-Path $root "broker.jsonl"
        Add-JSONLine $benchmark ([ordered]@{ type = "sample"; phase = "preload" })
        Add-JSONLine $benchmark ([ordered]@{ type = "sample"; phase = "measurement" })
        Add-JSONLine $broker ([ordered]@{ phase = "preload"; named_volume_allocated_bytes = 100 })
        Add-JSONLine $broker ([ordered]@{ phase = "measurement"; named_volume_allocated_bytes = 123 })
        Assert-Evidence $benchmark $broker $true
        "production evidence validation self-test passed"
    }
    finally { Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue }
    return
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
$brokerLogsPath = Join-Path $artifactDir "broker.log"
$summaryPath = Join-Path $artifactDir "summary.json"
$binaryPath = Join-Path $artifactDir "eventbus-benchmark.exe"
$namespace = "p676-$Driver-$([guid]::NewGuid().ToString('N'))"
$plannedMessages = if ($Duration -gt [timespan]::Zero) { [int64][math]::Ceiling($Duration.TotalSeconds * $Rate) } else { [int64]$Messages }

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
$dataPath = if ($Driver -eq "kafka") { "/var/lib/kafka/data" } else { "/data" }
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
    concurrency = $Concurrency; payload_kb = $PayloadKB; kafka_partitions = if ($Driver -eq "kafka") { $Concurrency } else { 0 }; nats_delay_batch_size = if ($Driver -eq "nats") { 1000 } else { 0 }; redis_delay_batch_size = if ($Driver -eq "redis") { 1000 } else { 0 }; delay = $Delay.ToString(); timeout = $Timeout.ToString()
    sample_every = $SampleEvery.ToString(); broker_restart_after = $BrokerRestartAfter.ToString(); broker_restart_every = $BrokerRestartEvery.ToString()
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
$restartCount = 0
$nextRestartAfter = $BrokerRestartAfter
$processForcedStop = $false
$startedAt = $null
try {
    & go build -o $binaryPath ./examples/eventbus/benchmark
    if ($LASTEXITCODE -ne 0) { throw "benchmark build failed" }
    & docker compose --project-name $project --file $composeFile --profile $Driver up -d --wait
    if ($LASTEXITCODE -ne 0) {
        (& docker compose --project-name $project --file $composeFile --profile $Driver logs --no-color $Driver) | Set-Content -LiteralPath $brokerLogsPath -Encoding utf8
        throw "Compose startup failed; see $brokerLogsPath"
    }
    $containerID = (& docker compose --project-name $project --file $composeFile ps -q $Driver).Trim()
    if (-not $containerID) { throw "Compose did not return a container ID for $Driver" }
    (& docker inspect $containerID) | Set-Content -LiteralPath $brokerInspectPath -Encoding utf8
    (& docker compose --project-name $project --file $composeFile --profile $Driver logs --no-color $Driver) | Set-Content -LiteralPath $brokerLogsPath -Encoding utf8

    $process = Start-Process -FilePath $binaryPath -ArgumentList $benchmarkArgs -PassThru -NoNewWindow -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
    $null = $process.Handle
    $startedAt = Get-Date
    while (-not $process.HasExited) {
        $elapsed = (Get-Date) - $startedAt
        if ($nextRestartAfter -gt [timespan]::Zero -and $elapsed -ge $nextRestartAfter) {
            $beforeFault = Get-LatestBenchmarkState $samplesPath
            $faultActive = $beforeFault -and [int64]$beforeFault.produced -gt 0 -and (([int64]$beforeFault.produced -gt [int64]$beforeFault.completed) -or ([int64]$beforeFault.produced -lt $plannedMessages))
            if ($faultActive) {
                $actionStarted = (Get-Date).ToUniversalTime()
                & docker compose --project-name $project --file $composeFile --profile $Driver restart $Driver
                $actionExit = $LASTEXITCODE
                $afterFault = Get-LatestBenchmarkState $samplesPath
                Add-JSONLine $faultsPath ([ordered]@{
                    type = "broker_restart"; started_utc = $actionStarted.ToString("o"); completed_utc = (Get-Date).ToUniversalTime().ToString("o"); exit_code = $actionExit
                    active_work = $true
                    produced_before = $beforeFault.produced; completed_before = $beforeFault.completed; lag_before = $beforeFault.lag
                    produced_after = if ($afterFault) { $afterFault.produced } else { $null }; completed_after = if ($afterFault) { $afterFault.completed } else { $null }; lag_after = if ($afterFault) { $afterFault.lag } else { $null }
                })
                $restartIssued = $true
                $restartCount++
                $nextRestartAfter = if ($BrokerRestartEvery -gt [timespan]::Zero) { $nextRestartAfter + $BrokerRestartEvery } else { [timespan]::Zero }
                if ($actionExit -ne 0) { throw "scoped broker restart failed" }
            }
        }
        $timestamp = (Get-Date).ToUniversalTime().ToString("o")
        $hostProcess = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
        $containerStats = & docker stats --no-stream --format '{{json .}}' $containerID
        $volumeRaw = @(& docker exec $containerID sh -c "du -sk '$dataPath' 2>/dev/null | cut -f1")
        $volumeBytes = if ($volumeRaw.Count -gt 0 -and $volumeRaw[0] -match '^\d+$') { [int64]$volumeRaw[0] * 1024 } else { $null }
        $latestState = Get-LatestBenchmarkState $samplesPath
        Add-JSONLine $brokerSamplesPath ([ordered]@{
            timestamp = $timestamp; phase = if ($latestState -and $latestState.phase -in @("preload", "measurement")) { $latestState.phase } elseif ($Preload) { "preload" } else { "measurement" }; launcher_pid = $process.Id
            launcher_cpu_seconds = if ($hostProcess) { $hostProcess.CPU } else { $null }
            launcher_working_set_bytes = if ($hostProcess) { $hostProcess.WorkingSet64 } else { $null }
            container = if ($containerStats) { $containerStats | ConvertFrom-Json } else { $null }
            named_volume_measurement = "du_allocated_1k_blocks"
            named_volume_allocated_bytes = $volumeBytes
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
    if ($Driver -eq "nats" -and [int]$result.nats_delay_batch_size -ne 1000) { throw "NATS benchmark delay batch size was not recorded as 1000" }
    if ($Driver -eq "redis" -and [int]$result.redis_delay_batch_size -ne 1000) { throw "Redis benchmark delay batch size was not recorded as 1000" }
    foreach ($field in @("final_stats_available", "final_pending", "final_delayed", "final_in_flight", "final_dlq", "final_lag", "second_worker_started", "second_worker_active_work", "primary_worker_handled", "second_worker_handled", "expected_retries", "synthetic_retry_failures", "retry_candidates_failed_at_attempt_one", "retry_first_seen_after_attempt_one")) {
        if ($null -eq $result.$field) { throw "result.json is missing required field $field" }
    }
    if ([int64]$result.expected_retries -ne ([int64]$result.retry_candidates_failed_at_attempt_one + [int64]$result.retry_first_seen_after_attempt_one)) { throw "retry accounting does not cover the expected retry set" }
    if (([int64]$result.primary_worker_handled + [int64]$result.second_worker_handled) -ne ([int64]$result.messages + [int64]$result.synthetic_retry_failures + [int64]$result.duplicates)) { throw "handler invocation accounting does not match completions, retries, and duplicates" }
    if (-not $result.final_stats_available) { throw "benchmark did not obtain final topology stats" }
    foreach ($field in @("final_pending", "final_delayed", "final_in_flight", "final_dlq", "final_lag")) {
        if ([int64]$result.$field -ne 0) { throw "benchmark final topology state is not settled: $field=$($result.$field)" }
    }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and -not $result.second_worker_started) { throw "requested Kafka second worker did not start" }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and -not $result.second_worker_active_work) { throw "Kafka second worker did not start while production or backlog was active" }
    if ($KafkaSecondWorkerAfter -gt [timespan]::Zero -and [int64]$result.second_worker_handled -le 0) { throw "Kafka second worker did not process work after joining the active group" }
    if ($BrokerRestartAfter -gt [timespan]::Zero -and -not $restartIssued) { throw "requested broker restart was not issued during active benchmark execution" }
    if ($BrokerRestartEvery -gt [timespan]::Zero) {
        $expectedRestarts = [math]::Ceiling(($Duration - $BrokerRestartAfter).TotalSeconds / $BrokerRestartEvery.TotalSeconds)
        if ($restartCount -ne $expectedRestarts) { throw "issued $restartCount broker restarts, expected $expectedRestarts" }
    }
    Assert-Evidence $samplesPath $brokerSamplesPath ([bool]$Preload)
    $faultActions = @(Read-JSONLines $faultsPath)
    if ($BrokerRestartAfter -gt [timespan]::Zero -and ($faultActions.Count -ne 1 -or -not $faultActions[0].active_work)) { throw "broker restart did not occur while production or backlog was active" }
}
catch {
    $runError = $_
}
finally {
    try { $processForcedStop = Stop-BenchmarkProcess $process } catch { if (-not $runError) { $runError = $_ } }
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
        schema_version = 2; run_id = $runID
        execution_status = if ($runError) { "failed" } elseif (-not $cleanupVerified) { "cleanup_failed" } else { "completed" }
        acceptance_status = if ($runError -or -not $cleanupVerified) { "failed" } else { "passed" }
        result = $resultForSummary; benchmark_sample_count = $benchmarkSamples.Count; broker_sample_count = $brokerSamples.Count
        measurement_sample_count = @($benchmarkSamples | Where-Object { $_.phase -eq "measurement" }).Count
        preload_sample_count = @($benchmarkSamples | Where-Object { $_.phase -eq "preload" }).Count
        named_volume_sample_count = @($brokerSamples | Where-Object { $null -ne $_.named_volume_allocated_bytes }).Count
        named_volume_measurement = "du_allocated_1k_blocks"
        max_heap_alloc_bytes = ($benchmarkSamples | Measure-Object -Property heap_alloc_bytes -Maximum).Maximum
        max_goroutines = ($benchmarkSamples | Measure-Object -Property goroutines -Maximum).Maximum
        max_pending = ($benchmarkSamples | Where-Object topology_stats_available | Measure-Object -Property pending -Maximum).Maximum
        max_lag = ($benchmarkSamples | Where-Object topology_stats_available | Measure-Object -Property lag -Maximum).Maximum
        max_named_volume_allocated_bytes = ($brokerSamples | Where-Object { $null -ne $_.named_volume_allocated_bytes } | Measure-Object -Property named_volume_allocated_bytes -Maximum).Maximum
        fault_action_count = $faultActions.Count; broker_restart_issued = $restartIssued; broker_restart_count = $restartCount; benchmark_process_forced_stop = $processForcedStop; cleanup_exit_code = $cleanupExit; cleanup_verified = $cleanupVerified
        remaining_container_ids = @($remainingContainers); remaining_volume_names = @($remainingVolumes)
        error = if ($runError) { $runError.Exception.Message } elseif (-not $cleanupVerified) { "scoped Compose cleanup verification failed" } else { $null }
    }
    $summary | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $summaryPath -Encoding utf8
}
if ($runError) { throw $runError }
if (-not $cleanupVerified) { throw "scoped Compose cleanup verification failed; see $summaryPath" }
$artifactDir
