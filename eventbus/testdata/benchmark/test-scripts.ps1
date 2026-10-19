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

"benchmark PowerShell validation passed"
