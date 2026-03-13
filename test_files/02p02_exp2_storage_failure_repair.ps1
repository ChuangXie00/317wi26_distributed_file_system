[CmdletBinding()]
param(
    [string]$FaultNode = "storage-03",
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "02p02_common.ps1")

$ctx = New-02p02Context -Experiment "exp2_storage_failure_repair" -OutputRoot $OutputRoot
$status = "skeleton_ready"
$next = @(
    "Inject storage fault with docker compose stop.",
    "Poll /debug/membership and /debug/replication.",
    "Compute detection_time_sec and repair_completion_time_sec."
)

$result = Save-02p02SkeletonResult `
    -Context $ctx `
    -CaseId "TC-04" `
    -CaseName "Storage failure detection and repair progress" `
    -Status $status `
    -NextActions $next

$summary = $result.Summary | Select-Object *
$summary | Add-Member -NotePropertyName fault_node -NotePropertyValue $FaultNode -Force
$summary | Add-Member -NotePropertyName probe_endpoints -NotePropertyValue @(
    "/debug/membership",
    "/debug/replication"
) -Force
$summary | Add-Member -NotePropertyName metric_targets -NotePropertyValue @(
    "detection_time_sec",
    "repair_completion_time_sec",
    "repaired_chunks",
    "estimated_bytes_copied"
) -Force
Write-02p02Json -Path $result.SummaryPath -Data $summary

Write-Host "== 0.2p02 E2 skeleton =="
Write-Host "status=$status"
Write-Host "fault_node=$FaultNode"
Write-Host "summary=$($result.SummaryPath)"
Write-Host "raw=$($result.RawPath)"
Write-Host "log=$($result.LogPath)"
