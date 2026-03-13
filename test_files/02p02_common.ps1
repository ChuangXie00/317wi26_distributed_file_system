[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-02p02RepoRoot {
    $repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
    return $repoRoot.Path
}

function New-02p02Context {
    param(
        [Parameter(Mandatory = $true)][string]$Experiment,
        [string]$OutputRoot = ""
    )

    $repoRoot = Get-02p02RepoRoot
    if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
        $OutputRoot = Join-Path $repoRoot ".doc\a\0.2p02_test"
    }

    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $expRoot = Join-Path $OutputRoot $Experiment
    $rawDir = Join-Path $expRoot "raw"
    $summaryDir = Join-Path $expRoot "summary"
    $logDir = Join-Path $expRoot "logs"

    foreach ($path in @($OutputRoot, $expRoot, $rawDir, $summaryDir, $logDir)) {
        if (-not (Test-Path $path)) {
            New-Item -ItemType Directory -Path $path -Force | Out-Null
        }
    }

    return [pscustomobject]@{
        RepoRoot = $repoRoot
        OutputRoot = $OutputRoot
        Experiment = $Experiment
        Stamp = $stamp
        ExpRoot = $expRoot
        RawDir = $rawDir
        SummaryDir = $summaryDir
        LogDir = $logDir
    }
}

function Write-02p02Json {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object]$Data
    )
    $json = $Data | ConvertTo-Json -Depth 20
    Set-Content -LiteralPath $Path -Value $json -Encoding UTF8
}

function Write-02p02LogLine {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Line
    )
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -LiteralPath $Path -Value "[$ts] $Line" -Encoding UTF8
}

function Save-02p02SkeletonResult {
    param(
        [Parameter(Mandatory = $true)][object]$Context,
        [Parameter(Mandatory = $true)][string]$CaseId,
        [Parameter(Mandatory = $true)][string]$CaseName,
        [Parameter(Mandatory = $true)][string]$Status,
        [Parameter(Mandatory = $true)][string[]]$NextActions
    )

    $summaryPath = Join-Path $Context.SummaryDir ("summary_{0}_{1}.json" -f $CaseId, $Context.Stamp)
    $rawPath = Join-Path $Context.RawDir ("raw_{0}_{1}.jsonl" -f $CaseId, $Context.Stamp)
    $logPath = Join-Path $Context.LogDir ("run_{0}_{1}.log" -f $CaseId, $Context.Stamp)

    $summary = [pscustomobject]@{
        case_id = $CaseId
        case_name = $CaseName
        experiment = $Context.Experiment
        status = $Status
        timestamp = (Get-Date).ToString("o")
        output_root = $Context.OutputRoot
        repo_root = $Context.RepoRoot
        next_actions = $NextActions
    }

    Write-02p02Json -Path $summaryPath -Data $summary
    Write-02p02LogLine -Path $logPath -Line ("skeleton status={0}" -f $Status)
    Add-Content -LiteralPath $rawPath -Value (($summary | ConvertTo-Json -Depth 20 -Compress)) -Encoding UTF8

    return [pscustomobject]@{
        SummaryPath = $summaryPath
        RawPath = $rawPath
        LogPath = $logPath
        Summary = $summary
    }
}
