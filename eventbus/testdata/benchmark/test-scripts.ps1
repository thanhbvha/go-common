$ErrorActionPreference = "Stop"
$scripts = @(
    (Join-Path $PSScriptRoot "run-production-scale.ps1"),
    (Join-Path $PSScriptRoot "../../../examples/eventbus/benchmark/run-matrix.ps1")
)

foreach ($script in $scripts) {
    $tokens = $null
    $errors = $null
    [void][Management.Automation.Language.Parser]::ParseFile($script, [ref]$tokens, [ref]$errors)
    if ($errors.Count -ne 0) {
        throw "PowerShell parser rejected ${script}: $($errors | ForEach-Object Message)"
    }
}

$runner = Join-Path $PSScriptRoot "run-production-scale.ps1"
$matrix = Join-Path $PSScriptRoot "../../../examples/eventbus/benchmark/run-matrix.ps1"
$valid = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver kafka -Duration 00:01:00 -Rate 100 -KafkaSecondWorkerAfter 00:00:30 -ValidateOnly
if ($LASTEXITCODE -ne 0 -or ($valid | ConvertFrom-Json).mode -ne "duration") {
    throw "duration validation case failed"
}

$previousPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver redis -Duration 00:01:00 -Rate 100 -Preload -ValidateOnly *> $null
$ErrorActionPreference = $previousPreference
if ($LASTEXITCODE -eq 0) {
    throw "invalid preload duration case was accepted"
}

$ErrorActionPreference = "Continue"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver nats -KafkaSecondWorkerAfter 00:00:30 -ValidateOnly *> $null
$ErrorActionPreference = $previousPreference
if ($LASTEXITCODE -eq 0) {
    throw "invalid non-Kafka second worker case was accepted"
}

$ErrorActionPreference = "Continue"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver kafka -Duration 02:00:00 -Rate 1000000 -ValidateOnly *> $null
$ErrorActionPreference = $previousPreference
if ($LASTEXITCODE -eq 0) {
    throw "unbounded duration/rate case was accepted"
}

$ErrorActionPreference = "Continue"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver kafka -Duration 00:01:00 -Rate 100 -KafkaSecondWorkerAfter 00:01:00 -ValidateOnly *> $null
$ErrorActionPreference = $previousPreference
if ($LASTEXITCODE -eq 0) {
    throw "fault action at duration boundary was accepted"
}

$matrixResult = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $matrix -Drivers redis,nats -Workloads immediate,retry10 -PayloadKB 1,10 -Concurrency 10,100 -Messages 100000,1000000 -ValidateOnly
if ($LASTEXITCODE -ne 0) { throw "matrix comma-delimited validation failed" }
$matrixJSON = $matrixResult | ConvertFrom-Json
if ($matrixJSON.drivers.Count -ne 2 -or $matrixJSON.messages.Count -ne 2) { throw "matrix comma-delimited values were not expanded" }

$ErrorActionPreference = "Continue"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $matrix -Drivers redis -Messages 100000,1000000 -Duration 00:01:00 -Rate 100 -ValidateOnly *> $null
$ErrorActionPreference = $previousPreference
if ($LASTEXITCODE -eq 0) { throw "duration matrix accepted multiple Messages values" }

$cleanupResult = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver redis -CleanupSelfTest
if ($LASTEXITCODE -ne 0 -or @($cleanupResult)[-1] -ne "benchmark process cleanup self-test passed") {
    throw "benchmark process cleanup self-test failed"
}

$evidenceResult = & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $runner -Driver redis -EvidenceSelfTest
if ($LASTEXITCODE -ne 0 -or @($evidenceResult)[-1] -ne "production evidence validation self-test passed") {
    throw "production evidence validation self-test failed"
}

"benchmark PowerShell validation passed"
