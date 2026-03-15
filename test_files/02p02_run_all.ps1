[CmdletBinding()]
param(
    [switch]$PrepareDataset,
    [switch]$DownloadGutenberg,
    [switch]$GenerateMixedFiles,
    [switch]$ForceRedownload,
    [int]$SamplesPerSize = 20,
    [int]$RandomSeed = 20260313,
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
if ($DownloadGutenberg) {
    $steps[0].Args += @("-DownloadGutenberg")
}
if ($GenerateMixedFiles) {
    $steps[0].Args += @("-GenerateMixedFiles")
}
if ($ForceRedownload) {
    $steps[0].Args += @("-ForceRedownload")
}
$steps[0].Args += @("-SamplesPerSize", $SamplesPerSize, "-RandomSeed", $RandomSeed)

$results = @()
foreach ($step in $steps) {
    if (($step.Name -eq "prepare") -and (-not $PrepareDataset)) {
        Write-Host "[skip] prepare (enable with -PrepareDataset)"
        $results += [pscustomobject]@{
            step = $step.Name
            script = $step.Script
            success = $true
            exit_code = 0
            skipped = $true
        }
        continue
    }

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
        skipped = $false
    }
    if (-not $ok) {
        throw ("step failed: {0} exit_code={1}" -f $step.Name, $LASTEXITCODE)
    }
}

Write-Host ""
Write-Host "== 0.2p02 run_all summary =="
$results | ForEach-Object {
    Write-Host ("{0}: success={1} exit_code={2} skipped={3}" -f $_.step, $_.success, $_.exit_code, $_.skipped)
}
Write-Host "output_root=$OutputRoot"
