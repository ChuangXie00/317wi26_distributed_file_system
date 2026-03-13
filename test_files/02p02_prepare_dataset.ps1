[CmdletBinding()]
param(
    [string]$DatasetDir = "",
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "02p02_common.ps1")

$ctx = New-02p02Context -Experiment "prepare_dataset" -OutputRoot $OutputRoot
$resolvedDataset = ""
$fileCount = 0
$totalBytes = 0
$status = "skeleton_ready"
$next = @(
    "Provide -DatasetDir to lock dataset snapshot.",
    "Add checksum manifest for reproducible rounds."
)

if (-not [string]::IsNullOrWhiteSpace($DatasetDir)) {
    if (-not (Test-Path -LiteralPath $DatasetDir)) {
        throw "DatasetDir not found: $DatasetDir"
    }
    $resolvedDataset = (Resolve-Path -LiteralPath $DatasetDir).Path
    $files = Get-ChildItem -LiteralPath $resolvedDataset -File -Recurse
    $fileCount = @($files).Count
    $totalBytes = ($files | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $totalBytes) { $totalBytes = 0 }
    $status = "dataset_scanned"
}

$result = Save-02p02SkeletonResult `
    -Context $ctx `
    -CaseId "TC-00" `
    -CaseName "Dataset preparation and inventory" `
    -Status $status `
    -NextActions $next

$summary = $result.Summary | Select-Object *
$summary | Add-Member -NotePropertyName dataset_dir -NotePropertyValue $resolvedDataset -Force
$summary | Add-Member -NotePropertyName file_count -NotePropertyValue $fileCount -Force
$summary | Add-Member -NotePropertyName total_bytes -NotePropertyValue $totalBytes -Force
Write-02p02Json -Path $result.SummaryPath -Data $summary

Write-Host "== 0.2p02 prepare dataset =="
Write-Host "status=$status"
Write-Host "dataset_dir=$resolvedDataset"
Write-Host "file_count=$fileCount"
Write-Host "total_bytes=$totalBytes"
Write-Host "summary=$($result.SummaryPath)"
Write-Host "raw=$($result.RawPath)"
Write-Host "log=$($result.LogPath)"
