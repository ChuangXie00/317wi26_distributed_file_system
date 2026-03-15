[CmdletBinding()]
param(
    [int[]]$Concurrency = @(1, 5, 10, 20, 40),
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "02p02_common.ps1")

$ctx = New-02p02Context -Experiment "exp1_baseline_throughput" -OutputRoot $OutputRoot
$status = "skeleton_ready"
$next = @(
    "Bind dataset from 02p02_prepare_dataset output.",
    "Call client/cli.py upload in parallel and collect per-file latency.",
    "Emit p50/p95/p99, throughput_mbps, error_rate."
)

$result = Save-02p02SkeletonResult `
    -Context $ctx `
    -CaseId "TC-02" `
    -CaseName "Concurrent upload throughput curve" `
    -Status $status `
    -NextActions $next

$summary = $result.Summary | Select-Object *
$summary | Add-Member -NotePropertyName concurrency_levels -NotePropertyValue $Concurrency -Force
$summary | Add-Member -NotePropertyName metric_targets -NotePropertyValue @(
    "files_per_sec",
    "logical_throughput_mbps",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "error_rate"
) -Force
$summary | Add-Member -NotePropertyName dataset_role_mapping -NotePropertyValue @(
    "DS-MixedFiles => files_per_sec + latency_p50/p95/p99",
    "Wiki raw/shards => logical_throughput_mbps"
) -Force
Write-02p02Json -Path $result.SummaryPath -Data $summary

Write-Host "== 0.2p02 E1 skeleton =="
Write-Host "status=$status"
Write-Host "concurrency=$($Concurrency -join ',')"
Write-Host "summary=$($result.SummaryPath)"
Write-Host "raw=$($result.RawPath)"
Write-Host "log=$($result.LogPath)"
