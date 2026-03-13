[CmdletBinding()]
param(
    [string]$DatasetDir = "",
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
    $OutputRoot = Join-Path $repoRoot ".doc\a\0.2p02_test"
}

$steps = @(
    @{ Name = "prepare"; Script = "02p02_prepare_dataset.ps1"; Args = @("-OutputRoot", $OutputRoot) },
    @{ Name = "exp1"; Script = "02p02_exp1_baseline_throughput.ps1"; Args = @("-OutputRoot", $OutputRoot) },
    @{ Name = "exp2"; Script = "02p02_exp2_storage_failure_repair.ps1"; Args = @("-OutputRoot", $OutputRoot) },
    @{ Name = "exp3"; Script = "02p02_exp3_storage_degraded_throughput.ps1"; Args = @("-OutputRoot", $OutputRoot) },
    @{ Name = "exp4"; Script = "02p02_exp4_meta_failover_throughput.ps1"; Args = @("-OutputRoot", $OutputRoot) }
)

if (-not [string]::IsNullOrWhiteSpace($DatasetDir)) {
    $steps[0].Args += @("-DatasetDir", $DatasetDir)
}

$results = @()
foreach ($step in $steps) {
    $scriptPath = Join-Path $PSScriptRoot $step.Script
    if (-not (Test-Path -LiteralPath $scriptPath)) {
        throw "missing script: $scriptPath"
    }

    Write-Host ("[run] {0} -> {1}" -f $step.Name, $step.Script)
    & powershell -ExecutionPolicy Bypass -File $scriptPath @($step.Args)
    $ok = ($LASTEXITCODE -eq 0)
    $results += [pscustomobject]@{
        step = $step.Name
        script = $step.Script
        success = $ok
        exit_code = $LASTEXITCODE
    }
    if (-not $ok) {
        throw ("step failed: {0} exit_code={1}" -f $step.Name, $LASTEXITCODE)
    }
}

Write-Host ""
Write-Host "== 0.2p02 run_all summary =="
$results | ForEach-Object {
    Write-Host ("{0}: success={1} exit_code={2}" -f $_.step, $_.success, $_.exit_code)
}
Write-Host "output_root=$OutputRoot"
