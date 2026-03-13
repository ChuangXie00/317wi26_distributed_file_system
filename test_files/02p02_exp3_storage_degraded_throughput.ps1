[CmdletBinding()]
param(
    [string]$FaultNode = "storage-03",
    [int]$WarmupSec = 60,
    [int]$WindowSec = 10,
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "02p02_common.ps1")

$ctx = New-02p02Context -Experiment "exp3_storage_degraded_throughput" -OutputRoot $OutputRoot
$status = "skeleton_ready"
$next = @(
    "Start steady upload workload before fault injection.",
    "Inject storage fault during workload.",
    "Record throughput windows before/during/after fault."
)

$result = Save-02p02SkeletonResult `
    -Context $ctx `
    -CaseId "TC-06" `
    -CaseName "Storage fault degraded service throughput" `
    -Status $status `
    -NextActions $next

$summary = $result.Summary | Select-Object *
$summary | Add-Member -NotePropertyName fault_node -NotePropertyValue $FaultNode -Force
$summary | Add-Member -NotePropertyName warmup_sec -NotePropertyValue $WarmupSec -Force
$summary | Add-Member -NotePropertyName window_sec -NotePropertyValue $WindowSec -Force
$summary | Add-Member -NotePropertyName metric_targets -NotePropertyValue @(
    "throughput_before_fault",
    "throughput_during_fault",
    "throughput_after_recover",
    "drop_ratio",
    "recovery_to_90pct_time"
) -Force
Write-02p02Json -Path $result.SummaryPath -Data $summary

Write-Host "== 0.2p02 E3 skeleton =="
Write-Host "status=$status"
Write-Host "fault_node=$FaultNode warmup_sec=$WarmupSec window_sec=$WindowSec"
Write-Host "summary=$($result.SummaryPath)"
Write-Host "raw=$($result.RawPath)"
Write-Host "log=$($result.LogPath)"
