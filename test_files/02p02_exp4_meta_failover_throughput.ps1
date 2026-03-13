[CmdletBinding()]
param(
    [string]$EntryBaseUrl = "http://localhost:8000",
    [int]$WarmupSec = 60,
    [int]$WindowSec = 10,
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "02p02_common.ps1")

$ctx = New-02p02Context -Experiment "exp4_meta_failover_throughput" -OutputRoot $OutputRoot
$status = "skeleton_ready"
$next = @(
    "Get current leader from /debug/leader, then stop that meta node.",
    "Poll /api/demo/events and /api/demo/metrics during failover.",
    "Compute drop_ratio_vs_baseline and recovery_to_90pct_time."
)

$result = Save-02p02SkeletonResult `
    -Context $ctx `
    -CaseId "TC-08" `
    -CaseName "Meta leader failover throughput window" `
    -Status $status `
    -NextActions $next

$summary = $result.Summary | Select-Object *
$summary | Add-Member -NotePropertyName entry_base_url -NotePropertyValue $EntryBaseUrl -Force
$summary | Add-Member -NotePropertyName warmup_sec -NotePropertyValue $WarmupSec -Force
$summary | Add-Member -NotePropertyName window_sec -NotePropertyValue $WindowSec -Force
$summary | Add-Member -NotePropertyName probe_endpoints -NotePropertyValue @(
    "/debug/leader",
    "/api/demo/events",
    "/api/demo/metrics"
) -Force
$summary | Add-Member -NotePropertyName metric_targets -NotePropertyValue @(
    "failover_recovery_seconds",
    "leader_switch_count_delta",
    "throughput_failover_window",
    "drop_ratio_vs_baseline",
    "recovery_to_90pct_time"
) -Force
Write-02p02Json -Path $result.SummaryPath -Data $summary

Write-Host "== 0.2p02 E4 skeleton =="
Write-Host "status=$status"
Write-Host "entry_base_url=$EntryBaseUrl warmup_sec=$WarmupSec window_sec=$WindowSec"
Write-Host "summary=$($result.SummaryPath)"
Write-Host "raw=$($result.RawPath)"
Write-Host "log=$($result.LogPath)"
