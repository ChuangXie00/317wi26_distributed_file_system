[CmdletBinding()]
param(
    [switch]$DownloadGutenberg,
    [switch]$GenerateMixedFiles,
    [switch]$ForceRedownload,
    [string]$DatasetDir = "",
    [string]$DatasetRoot = "",
    [int]$SamplesPerSize = 20,
    [int]$RandomSeed = 20260313,
    [string]$OutputRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "02p02_common.ps1")

$ctx = New-02p02Context -Experiment "prepare_dataset" -OutputRoot $OutputRoot
$repoRoot = $ctx.RepoRoot
if ([string]::IsNullOrWhiteSpace($DatasetRoot)) {
    $DatasetRoot = Join-Path $ctx.OutputRoot "datasets"
}

$rawDir = Join-Path $DatasetRoot "raw"
$largeTextDir = Join-Path $DatasetRoot "DS-LargeText"
$mixedDir = Join-Path $DatasetRoot "DS-MixedFiles"

foreach ($path in @($DatasetRoot, $rawDir, $largeTextDir, $mixedDir)) {
    if (-not (Test-Path -LiteralPath $path)) {
        New-Item -ItemType Directory -Path $path -Force | Out-Null
    }
}

function Download-GutenbergText {
    param(
        [string]$RawFolder,
        [string]$LargeTextFolder,
        [bool]$Redownload
    )

    $urls = @(
        "https://www.gutenberg.org/ebooks/26885.txt.utf-8",
        "https://www.gutenberg.org/ebooks/11407.txt.utf-8",
        "https://www.gutenberg.org/ebooks/11405.txt.utf-8",
        "https://www.gutenberg.org/ebooks/12574.txt.utf-8"
    )

    $downloaded = @()
    foreach ($url in $urls) {
        $name = Split-Path $url -Leaf
        $rawTarget = Join-Path $RawFolder $name
        if ((-not $Redownload) -and (Test-Path -LiteralPath $rawTarget)) {
            $downloaded += $rawTarget
            continue
        }
        Invoke-WebRequest -Uri $url -OutFile $rawTarget -TimeoutSec 60
        $downloaded += $rawTarget
    }

    foreach ($file in $downloaded) {
        $dst = Join-Path $LargeTextFolder (Split-Path $file -Leaf)
        Copy-Item -LiteralPath $file -Destination $dst -Force
    }

    return @($downloaded)
}

function New-MixedFilesFromLargeText {
    param(
        [string]$LargeTextFolder,
        [string]$MixedFolder,
        [int]$PerSize,
        [int]$Seed
    )

    $sizes = @(1024, 4096, 16384, 65536, 262144, 1048576)
    $sourceFiles = Get-ChildItem -LiteralPath $LargeTextFolder -File | Sort-Object Name
    # Ensure reproducible dataset snapshots by resetting output folder before generation.
    Get-ChildItem -LiteralPath $MixedFolder -File -ErrorAction SilentlyContinue | Remove-Item -Force
    $rng = [System.Random]::new($Seed)
    $created = 0
    $index = 0

    foreach ($file in $sourceFiles) {
        $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
        foreach ($size in $sizes) {
            if ($bytes.Length -lt $size) {
                continue
            }
            for ($i = 0; $i -lt $PerSize; $i++) {
                $startMax = $bytes.Length - $size
                $start = $rng.Next(0, $startMax + 1)
                $slice = New-Object byte[] $size
                [System.Buffer]::BlockCopy($bytes, $start, $slice, 0, $size)
                $name = "mixed_{0:D5}_{1}.bin" -f $index, $size
                $outPath = Join-Path $MixedFolder $name
                [System.IO.File]::WriteAllBytes($outPath, $slice)
                $index++
                $created++
            }
        }
    }
    return $created
}

function Measure-Dataset {
    param([string]$PathText)
    if (-not (Test-Path -LiteralPath $PathText)) {
        return [pscustomobject]@{
            path = $PathText
            file_count = 0
            total_bytes = 0
        }
    }
    $files = Get-ChildItem -LiteralPath $PathText -File -Recurse
    $total = ($files | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $total) { $total = 0 }
    return [pscustomobject]@{
        path = $PathText
        file_count = @($files).Count
        total_bytes = [int64]$total
    }
}

$downloadedFiles = @()
$mixedCreated = 0
$resolvedDataset = ""
$status = "dataset_scanned"
$next = @(
    "Use DS-LargeText for E1/E4 upload baseline.",
    "Use DS-MixedFiles for metadata pressure and tail latency."
)

if ($DownloadGutenberg) {
    $downloadedFiles = Download-GutenbergText -RawFolder $rawDir -LargeTextFolder $largeTextDir -Redownload ([bool]$ForceRedownload
    )
}

if ($GenerateMixedFiles) {
    $mixedCreated = New-MixedFilesFromLargeText -LargeTextFolder $largeTextDir -MixedFolder $mixedDir -PerSize $SamplesPerSize -Seed $RandomSeed
}

if (-not [string]::IsNullOrWhiteSpace($DatasetDir)) {
    if (-not (Test-Path -LiteralPath $DatasetDir)) {
        throw "DatasetDir not found: $DatasetDir"
    }
    $resolvedDataset = (Resolve-Path -LiteralPath $DatasetDir).Path
} else {
    $resolvedDataset = $DatasetRoot
}

$overall = Measure-Dataset -PathText $resolvedDataset
$large = Measure-Dataset -PathText $largeTextDir
$mixed = Measure-Dataset -PathText $mixedDir
$manifestPath = Join-Path $DatasetRoot "manifest.json"
$manifest = [pscustomobject]@{
    created_at = (Get-Date).ToString("o")
    repo_root = $repoRoot
    dataset_root = $DatasetRoot
    raw_dir = $rawDir
    large_text = $large
    mixed_files = $mixed
    downloaded_files = $downloadedFiles
    mixed_created_this_run = $mixedCreated
    random_seed = $RandomSeed
}
Write-02p02Json -Path $manifestPath -Data $manifest

$result = Save-02p02SkeletonResult `
    -Context $ctx `
    -CaseId "TC-00" `
    -CaseName "Dataset preparation and inventory" `
    -Status $status `
    -NextActions $next

$summary = $result.Summary | Select-Object *
$summary | Add-Member -NotePropertyName dataset_dir -NotePropertyValue $resolvedDataset -Force
$summary | Add-Member -NotePropertyName file_count -NotePropertyValue $overall.file_count -Force
$summary | Add-Member -NotePropertyName total_bytes -NotePropertyValue $overall.total_bytes -Force
$summary | Add-Member -NotePropertyName dataset_root -NotePropertyValue $DatasetRoot -Force
$summary | Add-Member -NotePropertyName large_text -NotePropertyValue $large -Force
$summary | Add-Member -NotePropertyName mixed_files -NotePropertyValue $mixed -Force
$summary | Add-Member -NotePropertyName manifest_path -NotePropertyValue $manifestPath -Force
$summary | Add-Member -NotePropertyName download_gutenberg -NotePropertyValue ([bool]$DownloadGutenberg) -Force
$summary | Add-Member -NotePropertyName generate_mixed_files -NotePropertyValue ([bool]$GenerateMixedFiles) -Force
$summary | Add-Member -NotePropertyName samples_per_size -NotePropertyValue $SamplesPerSize -Force
$summary | Add-Member -NotePropertyName mixed_created_this_run -NotePropertyValue $mixedCreated -Force
$summary | Add-Member -NotePropertyName random_seed -NotePropertyValue $RandomSeed -Force
Write-02p02Json -Path $result.SummaryPath -Data $summary

Write-Host "== 0.2p02 prepare dataset =="
Write-Host "status=$status"
Write-Host "dataset_dir=$resolvedDataset"
Write-Host "file_count=$($overall.file_count)"
Write-Host "total_bytes=$($overall.total_bytes)"
Write-Host "large_text_files=$($large.file_count) large_text_bytes=$($large.total_bytes)"
Write-Host "mixed_files=$($mixed.file_count) mixed_bytes=$($mixed.total_bytes)"
Write-Host "mixed_created_this_run=$mixedCreated"
Write-Host "random_seed=$RandomSeed"
Write-Host "manifest=$manifestPath"
Write-Host "summary=$($result.SummaryPath)"
Write-Host "raw=$($result.RawPath)"
Write-Host "log=$($result.LogPath)"
