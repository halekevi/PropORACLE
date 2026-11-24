# ============================================================
#  Register_Daily_Task.ps1
#  PropOracle automation scheduler (four PrizePicks fetches a day):
#   - 9:00 PM  day-ahead fetch + live /tickets for Eastern tomorrow (look / build tickets the night before)
#   - 3:00 AM  grader + A1 historical actuals (yesterday's games are final)
#   - 5:00 AM  early-morning fetch + line snapshot + live payout CDP + publish; season on/off sync
#   - 10:30 AM fetch before the PrizePicks morning line moves (rebuild + patch tickets)
#   - 4:30 PM  afternoon line-move pass before the 7pm boards
#   - 2:00 PM  payout ladder fill (Chrome CDP, between fetches)
#   - 11:00 PM / 6:00 PM  injury refresh (night, then before evening games; no PP fetch)
#   Retired: Daily 1AM / 8AM, Refresh 9AM / 945AM / 1PM, MLB Fill 11PM/12AM, Injuries T-60,
#   Tennis Early 3AM, Grader 1AM. Line changes are archived on every fetch.
#   Each 9PM+ refresh publishes live tickets/slate JSON to origin/main (site + Railway).
#
# CDP payout scrape runs at 9PM, 5AM, 10:30 and 4:30.
# Standalone 11:00 / 15:00 Payout CDP tasks are retired (use refresh path or manual).
#
# FD/DK/UD book-line ladder capture (run_book_line_capture.ps1) also runs on those
# windows soft-fail until ladders are complete — disable with data/cache/book_line_capture_off.flag.
#
# Each task opens ONE visible PowerShell console (direct pwsh.exe action).
# Do NOT wrap with cmd.exe "start /wait" — that leaves an empty cmd.exe window
# plus a second titled window. Requires "Run only when user is logged on".
#
# Run from PropORACLE_main_cp\scripts (Task Scheduler must not point at a feature branch).
# Re-running replaces tasks so paths stay in sync after moving the clone off OneDrive.
# ============================================================

$PipelineRoot = Split-Path -Parent $PSScriptRoot
# Prefer pwsh (UTF-8 / PS7). Windows PowerShell 5.1 mis-parses UTF-8 em-dashes in wrappers.
$PowerShellExe = $null
foreach ($cand in @(
    (Get-Command pwsh.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source),
    "$env:ProgramFiles\PowerShell\7\pwsh.exe",
    (Get-Command powershell.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)
)) {
    if ($cand -and (Test-Path -LiteralPath $cand)) { $PowerShellExe = $cand; break }
}
if (-not $PowerShellExe) {
    Write-Error "No pwsh.exe/powershell.exe found"
    exit 1
}
Write-Host "Registering tasks with: $PowerShellExe" -ForegroundColor Cyan
Write-Host "Windows: one visible console (pwsh.exe directly; no cmd start wrapper)" -ForegroundColor Cyan

$ScriptDayAhead = Join-Path $PipelineRoot "scripts\run_daily_day_ahead.ps1"
$ScriptMlbFill = Join-Path $PipelineRoot "scripts\run_mlb_day_ahead_fill.ps1"
$Script1 = Join-Path $PipelineRoot "scripts\run_daily_1am.ps1"
$ScriptGrader = Join-Path $PipelineRoot "scripts\run_grader_evening.ps1"
$Script5 = Join-Path $PipelineRoot "scripts\run_daily_5am.ps1"
$Script8 = Join-Path $PipelineRoot "scripts\run_daily_8am.ps1"
$ScriptRefresh = Join-Path $PipelineRoot "scripts\run_refresh_with_log.ps1"
$ScriptInjuries = Join-Path $PipelineRoot "scripts\run_injuries.ps1"
$ScriptInjuryRefresh = Join-Path $PipelineRoot "scripts\run_injury_refresh.ps1"

foreach ($s in @($ScriptDayAhead, $ScriptMlbFill, $Script1, $ScriptGrader, $Script5, $Script8, $ScriptRefresh, $ScriptInjuries, $ScriptInjuryRefresh, (Join-Path $PipelineRoot "scripts\Publish-LiveSite.ps1"))) {
    if (-not (Test-Path $s)) {
        Write-Error "Required script missing: $s"
        exit 1
    }
}

# Legacy tasks to remove (superseded schedule).
$LegacyTasksToRemove = @(
    "PropORACLE Daily Pipeline",
    "PropOracle - Daily 4AM",
    "PropOracle - Grader 5AM",
    "PropOracle - Daily 7AM",
    "PropOracle - Refresh 11AM",
    # Overnight cluster: tennis-only 3AM and 5AM daily retired; grader moved 1AM → 3AM
    "PropOracle - Tennis Early 3AM",
    "PropOracle - Grader 1AM",
    # CDP only rides with fetch/refresh — no standalone payout timers
    "PropOracle - Payout CDP",
    "PropOracle - Payout CDP Update",
    # Extra overnight graders removed — keep only Grader 3AM
    "PropOracle - Grader 7PM",
    "PropOracle - Grader 8PM",
    "PropOracle - Grader 9PM",
    "PropOracle - Grader 10PM",
    "PropOracle - Grader 11PM",
    "PropOracle - Grader 12AM"
)
foreach ($legacy in $LegacyTasksToRemove) {
    $existing = Get-ScheduledTask -TaskName $legacy -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $legacy -Confirm:$false
        Write-Host "Removed legacy task: $legacy" -ForegroundColor Yellow
    }
}

function Register-PropTask {
    param(
        [string]$TaskName,
        [string]$Description,
        [string]$ScriptPath,
        [string]$At,
        [string]$ExtraArgs = "",
        [TimeSpan]$RepetitionInterval = [TimeSpan]::Zero,
        [TimeSpan]$RepetitionDuration = [TimeSpan]::Zero,
        [TimeSpan]$ExecutionTimeLimit = (New-TimeSpan -Hours 4)
    )

    # One console only: Task Scheduler runs pwsh.exe with Interactive logon.
    # (cmd.exe + "start /wait" used to leave an empty cmd window + a second titled window.)
    $extra = $ExtraArgs.Trim()
    $psArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""
    if ($extra) { $psArgs = "$psArgs $extra" }

    $action = New-ScheduledTaskAction `
        -Execute $PowerShellExe `
        -Argument $psArgs `
        -WorkingDirectory $PipelineRoot

    # Eastern wall-clock: StartBoundary with NO offset / Z so Task Scheduler keeps
    # local ET time across DST (SynchronizeAcrossTimeZones stays false). A baked
    # -04:00/-05:00 offset is what made 1AM fire at midnight after the fall-back.
    $atTs = [TimeSpan]::Parse($At)
    $trigger = New-ScheduledTaskTrigger -Daily -At $At
    $trigger.StartBoundary = (Get-Date).Date.Add($atTs).ToString("yyyy-MM-ddTHH:mm:ss")
    try {
        if ($null -ne $trigger.PSObject.Properties["SynchronizeAcrossTimeZones"]) {
            $trigger.SynchronizeAcrossTimeZones = $false
        }
    } catch { }
    if ($RepetitionInterval -gt [TimeSpan]::Zero) {
        $dur = if ($RepetitionDuration -gt [TimeSpan]::Zero) { $RepetitionDuration } else { (New-TimeSpan -Hours 12) }
        $trigger.Repetition = (New-ScheduledTaskTrigger -Once -At $At `
            -RepetitionInterval $RepetitionInterval `
            -RepetitionDuration $dur).Repetition
    }
    $settings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit $ExecutionTimeLimit `
        -RestartCount 2 `
        -RestartInterval (New-TimeSpan -Minutes 15) `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -MultipleInstances IgnoreNew

    # Interactive = show UI when user is logged on (required for visible window).
    $principal = New-ScheduledTaskPrincipal `
        -UserId $env:USERNAME `
        -LogonType Interactive `
        -RunLevel Limited

    $running = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($running -and $running.State -eq "Running") {
        Write-Host "  SKIP re-register (currently Running): $TaskName — will pick up new script on next fire" -ForegroundColor Yellow
        return
    }

    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $Description `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null

    Write-Host "  Registered: $TaskName @ $At (visible window)" -ForegroundColor DarkGray
}

Register-PropTask `
    -TaskName "PropOracle - DayAhead 9PM" `
    -Description "INITIAL scrape + live /tickets publish for Eastern tomorrow (soccer/tennis/etc.). MLB often empty until 11PM+. Opens visible PowerShell." `
    -ScriptPath $ScriptDayAhead `
    -At "21:00"

Register-PropTask `
    -TaskName "PropOracle - Grader 3AM" `
    -Description "Overnight A1 historical actuals + grader for yesterday. Split from Daily 1AM so fetch and grade do not share RAM/CPU. Opens visible PowerShell." `
    -ScriptPath $ScriptGrader `
    -At "03:00"

Register-PropTask `
    -TaskName "PropOracle - Daily 5AM" `
    -Description "Juice-window update: all-sport refetch + line snapshot + live payout CDP + publish. Skips grader/A1 when 3AM finished. Opens visible PowerShell." `
    -ScriptPath $Script5 `
    -At "05:00"

Register-PropTask `
    -TaskName "PropOracle - Refresh 1030AM" `
    -Description "PP morning move window (~10:30-11). Fetch/refresh + Force CDP + live site publish." `
    -ScriptPath $ScriptRefresh `
    -At "10:30" `
    -ExtraArgs "-RunLabel 1030AM"

Register-PropTask `
    -TaskName "PropOracle - Refresh 430PM" `
    -Description "Evening lock for 7pm WNBA/MLB boards. Fetch/refresh + Force CDP + live site publish." `
    -ScriptPath $ScriptRefresh `
    -At "16:30" `
    -ExtraArgs "-RunLabel 430PM"

# Light injury refresh: no PrizePicks fetch, no full pipeline. Waits for refresh.lock.
foreach ($inj in @(@{ Name = "PropOracle - Injury Refresh 11PM"; At = "23:00"; Win = "11PM" },
                   @{ Name = "PropOracle - Injury Refresh 6PM"; At = "18:00"; Win = "6PM" })) {
    Register-PropTask `
        -TaskName $inj.Name `
        -Description "Injury collect + sidecars + board attach + Goblin-70/mixer rebuild from on-disk step8 + publish when slips change. No PP fetch." `
        -ScriptPath $ScriptInjuryRefresh `
        -At $inj.At `
        -ExtraArgs "-Window $($inj.Win)" `
        -ExecutionTimeLimit (New-TimeSpan -Hours 1)
}

foreach ($retired in @("PropOracle - MLB Fill 11PM", "PropOracle - MLB Fill 12AM", "PropOracle - Injuries T-60",
                      "PropOracle - Daily 1AM", "PropOracle - Daily 8AM", "PropOracle - Refresh 9AM",
                      "PropOracle - Refresh 945AM", "PropOracle - Refresh 1PM", "PropOracle - Injury Refresh 9AM")) {
    if (Get-ScheduledTask -TaskName $retired -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $retired -Confirm:$false
        Write-Host "  Removed retired task: $retired" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "Scheduler tasks registered (visible PowerShell windows)." -ForegroundColor Green
Write-Host "  - PropOracle - DayAhead 9PM (soccer/tennis/etc. tomorrow card)"
Write-Host "  - PropOracle - Grader 3AM (A1 + yesterday grades; unchanged)"
Write-Host "  - PropOracle - Daily 5AM (juice-window update + live payout CDP + publish)"
Write-Host "  - PropOracle - Refresh 1030AM (PP morning move + ticket patch + publish)"
Write-Host "  - PropOracle - Refresh 430PM (evening 7pm slate + publish)"
Write-Host "  - PropOracle - Injury Refresh 11PM / 6PM (collect + re-attach + ticket rebuild + publish; no PP fetch)"
Write-Host ""
Write-Host "CDP payout scrape: 9PM + 5AM + 10:30 + 4:30 (no standalone 11AM/3PM tasks)." -ForegroundColor Yellow
Write-Host "Injuries: also runs inside 5AM daily + each refresh (10:30 / 4:30). OUT hard-drops from tickets." -ForegroundColor Yellow
Write-Host "Removed: Tennis Early 3AM, Grader 1AM (now Grader 3AM), Payout CDP 11AM/3PM, extra graders 7PM–12AM" -ForegroundColor Yellow
Write-Host ""
Write-Host "Quick checks:"
Write-Host "  Get-ScheduledTask | Where-Object TaskName -like 'PropOracle -*' | Select-Object TaskName, State"
Write-Host "  Get-ScheduledTaskInfo -TaskName 'PropOracle - Daily 5AM' | Select LastRunTime, LastTaskResult, NextRunTime"
Write-Host "  Get-ScheduledTaskInfo -TaskName 'PropOracle - Daily 5AM' | Select LastRunTime, LastTaskResult, NextRunTime"
Write-Host "  Get-ScheduledTaskInfo -TaskName 'PropOracle - Grader 3AM' | Select LastRunTime, LastTaskResult, NextRunTime"
Write-Host ""
Write-Host "Manual catchup (visible window):  pwsh -File scripts\Launch_Daily_1AM_Visible.ps1" -ForegroundColor Cyan
Write-Host "Manual day-ahead (visible window): pwsh -File scripts\Launch_DayAhead_Visible.ps1" -ForegroundColor Cyan
Write-Host "Manual MLB fill (visible window):  pwsh -File scripts\Launch_MlbDayAheadFill_Visible.ps1 -Window 11PM" -ForegroundColor Cyan
Write-Host "Manual payout CDP (after a fetch): pwsh -File scripts\run_payout_cdp.ps1" -ForegroundColor Cyan
Write-Host "Manual FillMissing only:          pwsh -File scripts\run_payout_cdp_update.ps1" -ForegroundColor Cyan
