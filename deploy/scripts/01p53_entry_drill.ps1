param(
    [string]$ComposeFile = "deploy/docker-compose.yml",
    [string]$EntryBaseUrl = "http://localhost:8000",
    [int]$PollIntervalSec = 2,
    [int]$SwitchTimeoutSec = 120,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$scriptStartAt = Get-Date
$stoppedNodes = New-Object 'System.Collections.Generic.HashSet[string]'

function Invoke-Compose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Args
    )
    $command = @("compose", "-f", $ComposeFile) + $Args
    docker @command | Out-Host
}

function Get-EntryStatus {
    return Invoke-RestMethod -Uri "$EntryBaseUrl/debug/entry" -TimeoutSec 8
}

function Wait-EntryReady {
    param([int]$TimeoutSec = 60)

    $started = Get-Date
    while ($true) {
        try {
            $status = Get-EntryStatus
            if ($status -and $status.active_leader_id) {
                return $status
            }
        }
        catch {
            # Entry may be briefly unavailable during container recreation.
        }

        $elapsed = ((Get-Date) - $started).TotalSeconds
        if ($elapsed -ge $TimeoutSec) {
            throw "entry not ready within ${TimeoutSec}s"
        }
        Start-Sleep -Seconds 1
    }
}

function Stop-MetaNode {
    param([Parameter(Mandatory = $true)][string]$NodeId)
    Invoke-Compose -Args @("stop", $NodeId)
    [void]$stoppedNodes.Add($NodeId)
}

function Start-MetaNode {
    param([Parameter(Mandatory = $true)][string]$NodeId)
    Invoke-Compose -Args @("start", $NodeId)
    [void]$stoppedNodes.Remove($NodeId)
}

function Wait-EntrySwitch {
    param(
        [Parameter(Mandatory = $true)][string]$FromLeader,
        [Parameter(Mandatory = $true)][string]$Phase
    )

    $started = Get-Date
    while ($true) {
        $elapsed = ((Get-Date) - $started).TotalSeconds
        $status = $null
        try {
            $status = Get-EntryStatus
        }
        catch {
            Write-Output ("[{0}] elapsed={1:n1}s active=<unreachable> pending=<na> decision=<na>" -f $Phase, $elapsed)
            if ($elapsed -ge $SwitchTimeoutSec) {
                throw "phase=$Phase timeout: entry unreachable for ${SwitchTimeoutSec}s"
            }
            Start-Sleep -Seconds $PollIntervalSec
            continue
        }
        Write-Output ("[{0}] elapsed={1:n1}s active={2} pending={3} decision={4}" -f $Phase, $elapsed, $status.active_leader_id, $status.pending_leader_id, $status.last_decision)

        if ($status.active_leader_id -and $status.active_leader_id -ne $FromLeader) {
            return @{
                status = $status
                latency_sec = [math]::Round($elapsed, 3)
            }
        }

        if ($elapsed -ge $SwitchTimeoutSec) {
            throw "phase=$Phase timeout: entry did not switch from $FromLeader within ${SwitchTimeoutSec}s"
        }
        Start-Sleep -Seconds $PollIntervalSec
    }
}

function Assert-EntryStable {
    param(
        [Parameter(Mandatory = $true)][string]$ExpectedLeader,
        [Parameter(Mandatory = $true)][string]$Phase,
        [int]$DurationSec = 20
    )

    $started = Get-Date
    while ($true) {
        $elapsed = ((Get-Date) - $started).TotalSeconds
        $status = $null
        try {
            $status = Get-EntryStatus
        }
        catch {
            Write-Output ("[{0}] elapsed={1:n1}s active=<unreachable> decision=<na>" -f $Phase, $elapsed)
            if ($elapsed -ge $DurationSec) {
                throw "phase=$Phase unstable: entry unreachable during stability window"
            }
            Start-Sleep -Seconds $PollIntervalSec
            continue
        }
        Write-Output ("[{0}] elapsed={1:n1}s active={2} decision={3}" -f $Phase, $elapsed, $status.active_leader_id, $status.last_decision)

        if ($status.active_leader_id -ne $ExpectedLeader) {
            throw "phase=$Phase unstable: expected=$ExpectedLeader actual=$($status.active_leader_id)"
        }

        if ($elapsed -ge $DurationSec) {
            return
        }
        Start-Sleep -Seconds $PollIntervalSec
    }
}

function Assert-EntryNoRapidFlap {
    param(
        [Parameter(Mandatory = $true)][string]$Phase,
        [int]$DurationSec = 20,
        [int]$ExpectedMaxExtraSwitch = 1
    )

    $baseline = Get-EntryStatus
    $baseSwitchCount = [int]$baseline.switch_count
    $started = Get-Date
    $lastStatus = $baseline
    while ($true) {
        $elapsed = ((Get-Date) - $started).TotalSeconds
        $status = $null
        try {
            $status = Get-EntryStatus
        }
        catch {
            Write-Output ("[{0}] elapsed={1:n1}s active=<unreachable> decision=<na>" -f $Phase, $elapsed)
            if ($elapsed -ge $DurationSec) {
                throw "phase=$Phase unstable: entry unreachable during flap window"
            }
            Start-Sleep -Seconds $PollIntervalSec
            continue
        }

        $extraSwitch = [int]$status.switch_count - $baseSwitchCount
        Write-Output ("[{0}] elapsed={1:n1}s active={2} extra_switch={3} decision={4}" -f $Phase, $elapsed, $status.active_leader_id, $extraSwitch, $status.last_decision)
        if ($extraSwitch -gt $ExpectedMaxExtraSwitch) {
            throw "phase=$Phase unstable: extra_switch=$extraSwitch exceeds limit=$ExpectedMaxExtraSwitch"
        }

        $lastStatus = $status
        if ($elapsed -ge $DurationSec) {
            return $lastStatus
        }
        Start-Sleep -Seconds $PollIntervalSec
    }
}

try {
    if ($SkipBuild) {
        Invoke-Compose -Args @("up", "-d")
    }
    else {
        Invoke-Compose -Args @("up", "-d", "--build")
    }

    $initial = Wait-EntryReady -TimeoutSec 60
    $initialLeader = [string]$initial.active_leader_id
    if (-not $initialLeader) {
        throw "entry returned empty initial active_leader_id"
    }
    Write-Output ("[bootstrap] initial active leader: {0}" -f $initialLeader)

    # Drill-1: stop initial leader and wait for entry failover.
    Stop-MetaNode -NodeId $initialLeader
    $firstSwitch = Wait-EntrySwitch -FromLeader $initialLeader -Phase "failover-1"
    $leaderAfterFirstFailover = [string]$firstSwitch.status.active_leader_id
    Start-MetaNode -NodeId $initialLeader

    # recover-1 allows one additional convergence switch, but no rapid repeated flaps.
    $recover1 = Assert-EntryNoRapidFlap -Phase "recover-1" -DurationSec 20 -ExpectedMaxExtraSwitch 1

    # Drill-2: stop current leader again and verify another failover.
    Stop-MetaNode -NodeId $leaderAfterFirstFailover
    $secondSwitch = Wait-EntrySwitch -FromLeader $leaderAfterFirstFailover -Phase "failover-2"
    $leaderAfterSecondFailover = [string]$secondSwitch.status.active_leader_id
    Start-MetaNode -NodeId $leaderAfterFirstFailover

    # recover-2 允许一次再收敛，不应出现连续快速来回切换。
    $recover2 = Assert-EntryNoRapidFlap -Phase "recover-2" -DurationSec 20 -ExpectedMaxExtraSwitch 1

    $final = Get-EntryStatus
    $summary = [ordered]@{
        script_started_at            = $scriptStartAt.ToString("o")
        script_finished_at           = (Get-Date).ToString("o")
        initial_leader               = $initialLeader
        first_failover_target        = $leaderAfterFirstFailover
        first_failover_latency_sec   = [double]$firstSwitch.latency_sec
        recover_1_final_leader       = [string]$recover1.active_leader_id
        second_failover_target       = $leaderAfterSecondFailover
        second_failover_latency_sec  = [double]$secondSwitch.latency_sec
        recover_2_final_leader       = [string]$recover2.active_leader_id
        final_active_leader          = [string]$final.active_leader_id
        switch_count                 = [int]$final.switch_count
        flap_count                   = [int]$final.flap_count
        last_switch_latency_sec      = [double]$final.last_switch_latency_sec
        last_decision                = [string]$final.last_decision
        require_single_writable_gate = [bool]$final.switch_policy.require_single_writable_leader
        pass                         = $true
    }
    $summary | ConvertTo-Json -Depth 8
}
catch {
    $failed = [ordered]@{
        script_started_at  = $scriptStartAt.ToString("o")
        script_finished_at = (Get-Date).ToString("o")
        pass               = $false
        error              = $_.Exception.Message
    }
    $failed | ConvertTo-Json -Depth 8
    throw
}
finally {
    foreach ($nodeId in @($stoppedNodes)) {
        try {
            Start-MetaNode -NodeId $nodeId
        }
        catch {
            Write-Warning ("failed to restart node: {0}; error={1}" -f $nodeId, $_.Exception.Message)
        }
    }
}
