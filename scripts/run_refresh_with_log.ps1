#requires -Version 5.1
<#
  Line-move refresh (8 AM morning update, 9:00, 9:45, 10:30, 1 PM, 4:30 PM).
  Initial scrape + live publish is 9PM day-ahead; 1AM / 8AM+ are updates.
  Every window: fetch + line timestamps + live publish.
  Payout Force timestamps only when lines moved vs the previous/initial stamp.
  Wait for refresh.lock instead of skipping (9AM/10:30 must still stamp + publish).
  Hold the lock through payout CDP — releasing it after publish let 9AM fetch
  while 8AM still owned PrizePicks Chrome, tickets JSON, and git publish.
#>
param(
    [string]$RunLabel = "945AM"
)

$ErrorActionPreference = "Continue"
try { $Host.UI.RawUI.WindowTitle = "PropOracle - Refresh $RunLabel" } catch { }
$Root = Split-Path $PSScriptRoot -Parent
$LateFetch = Join-Path $Root "scripts\run_nba_late_fetch.ps1"
$Snapshot = Join-Path $Root "scripts\log_prop_snapshot.ps1"
$LockDir = Join-Path $Root "data\cache"
$LockFile = Join-Path $LockDir "refresh.lock"
# Soft TTL: dead/hung holders must not block the rest of the day's cadence.
# Previously a 4h TTL + exit 0 on skip made 10:30 look "successful" while 9AM was hung.
$LockTTLMinutes = 90
# Machine-wide so PropORACLE vs PropORACLE_main_cp cannot fetch in parallel.
$RefreshMutexName = "Global\PropORACLE_RefreshWindow"

if (-not (Test-Path $LateFetch)) {
    Write-Error "Missing late fetch script: $LateFetch"
    exit 1
}
if (-not (Test-Path $Snapshot)) {
    Write-Error "Missing prop snapshot script: $Snapshot"
    exit 1
}

if (-not (Test-Path -LiteralPath $LockDir)) {
    New-Item -ItemType Directory -Path $LockDir -Force | Out-Null
}

function Test-TodaySlateNeedsCatchup {
    $today = (Get-Date).ToString("yyyy-MM-dd")
    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        $etNow = [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
        if ($etNow.Hour -ge 20) {
            $today = $etNow.Date.AddDays(1).ToString("yyyy-MM-dd")
        }
        else {
            $today = $etNow.ToString("yyyy-MM-dd")
        }
    } catch { }
    $combined = Join-Path $Root "outputs\$today\combined_slate_tickets_$today.xlsx"
    if (-not (Test-Path -LiteralPath $combined)) { return $true }
    $statusPath = Join-Path $Root "outputs\$today\pipeline_slate_status.json"
    if (-not (Test-Path -LiteralPath $statusPath)) { return $true }
    try {
        $ss = Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json
        $complete = 0
        $active = @("mlb", "soccer", "tennis", "golf")
        $wnbaResume = "2026-07-28"
        if ($env:WNBA_RESUME_DATE) { $wnbaResume = $env:WNBA_RESUME_DATE.Trim() }
        $wnbaPause = "2026-07-19"
        if ($env:WNBA_PAUSE_START) { $wnbaPause = $env:WNBA_PAUSE_START.Trim() }
        if (-not (($today -ge $wnbaPause) -and ($today -lt $wnbaResume))) {
            $active = @("mlb", "wnba", "soccer", "tennis", "golf")
        }
        foreach ($sk in $active) {
            $st = if ($ss.sports) { "$($ss.sports.$sk)" } else { "" }
            # Golf (and tennis) empty boards are no_slate, not a failed fetch.
            if ($st -eq "complete" -or $st -eq "off_season" -or $st -eq "no_slate") { $complete++ }
        }
        return ($complete -lt $active.Count)
    } catch {
        return $true
    }
}

$LogsDir = Join-Path $Root "logs"
if (-not (Test-Path -LiteralPath $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
}
$RefreshLog = Join-Path $LogsDir ("task_refresh_{0}_{1:yyyy-MM-dd_HHmmss}.log" -f $RunLabel, (Get-Date))
try { Start-Transcript -Path $RefreshLog -Append | Out-Null } catch { }

function Get-RefreshLockInfo {
    if (-not (Test-Path -LiteralPath $LockFile)) { return $null }
    $lockAge = (Get-Date) - (Get-Item -LiteralPath $LockFile).LastWriteTime
    $lockContent = (Get-Content -LiteralPath $LockFile -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $lockContent) { $lockContent = "<unknown owner>" }
    $lockPid = $null
    if ("$lockContent" -match 'PID\s+(\d+)') { $lockPid = [int]$Matches[1] }
    $lockPidAlive = $false
    if ($lockPid) {
        $lockPidAlive = $null -ne (Get-Process -Id $lockPid -ErrorAction SilentlyContinue)
    }
    return @{
        Content = $lockContent
        Pid = $lockPid
        Alive = $lockPidAlive
        AgeMin = [int]$lockAge.TotalMinutes
        Stale = (($lockPid -and -not $lockPidAlive) -or ($lockAge.TotalMinutes -ge $LockTTLMinutes -and -not $lockPidAlive))
    }
}

function Publish-BlockedRefreshWindow {
    param([string]$Reason)
    Write-Host "[REFRESH $RunLabel] $Reason" -ForegroundColor Yellow
    $needsCatchup = Test-TodaySlateNeedsCatchup
    $stampPy = Join-Path $Root "scripts\stamp_fetch_window.py"
    if (Test-Path -LiteralPath $stampPy) {
        & py -3.14 $stampPy --write-stamp --window $RunLabel
    }
    $publish = Join-Path $Root "scripts\Publish-LiveSite.ps1"
    $todayEt = (Get-Date).ToString("yyyy-MM-dd")
    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        $todayEt = [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz).ToString("yyyy-MM-dd")
    } catch { }
    if (Test-Path -LiteralPath $publish) {
        Write-Host "[REFRESH $RunLabel] Publishing current tickets (fetch blocked)..." -ForegroundColor Yellow
        & pwsh -NoProfile -File $publish -RepoRoot $Root -CommitMessage "chore: live tickets/slate $todayEt $RunLabel (blocked)"
    }
    try { Stop-Transcript | Out-Null } catch { }
    if ($needsCatchup) { exit 2 }
    exit 0
}

# Wait for a live owner instead of overlapping CDP/fetch (9AM/10:30 still stamp).
$LockWaitMinutes = 75
$SkipFetchIfWaitedMinutes = 20
$waitStarted = Get-Date
$waitDeadline = $waitStarted.AddMinutes($LockWaitMinutes)
$refreshMutex = $null
$acquiredMutex = $false
try {
    $refreshMutex = [System.Threading.Mutex]::new($false, $RefreshMutexName)
} catch {
    Write-Host "[REFRESH $RunLabel] WARN: named mutex unavailable ($($_.Exception.Message)) — lock file only" -ForegroundColor Yellow
}

while ($true) {
    $info = Get-RefreshLockInfo
    if ($info -and $info.Alive) {
        if ((Get-Date) -ge $waitDeadline) {
            Publish-BlockedRefreshWindow "WAIT TIMEOUT — still running ($($info.Content), age $($info.AgeMin) min)"
        }
        Write-Host "[REFRESH $RunLabel] waiting for lock ($($info.Content), age $($info.AgeMin) min)..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 20
        continue
    }
    if ($info) {
        Write-Host "[REFRESH $RunLabel] Clearing dead/stale lock ($($info.Content))" -ForegroundColor Yellow
        Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
    }
    if ($refreshMutex -and -not $acquiredMutex) {
        $remainingMs = [int][Math]::Max(1000, ($waitDeadline - (Get-Date)).TotalMilliseconds)
        try {
            $acquiredMutex = $refreshMutex.WaitOne($remainingMs)
        } catch [System.Threading.AbandonedMutexException] {
            $acquiredMutex = $true
        } catch {
            Write-Host "[REFRESH $RunLabel] WARN: mutex wait failed ($($_.Exception.Message))" -ForegroundColor Yellow
            $acquiredMutex = $false
            break
        }
        if (-not $acquiredMutex) {
            $info = Get-RefreshLockInfo
            $who = if ($info) { "$($info.Content), age $($info.AgeMin) min" } else { "another refresh window" }
            Publish-BlockedRefreshWindow "WAIT TIMEOUT — mutex still held ($who)"
        }
        $leftover = Get-RefreshLockInfo
        if ($leftover) {
            Write-Host "[REFRESH $RunLabel] Clearing leftover lock after mutex acquire ($($leftover.Content))" -ForegroundColor Yellow
            Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
        }
        break
    }
    break
}

$waitedMinutes = [int]((Get-Date) - $waitStarted).TotalMinutes
$lockContent = "$RunLabel | PID $PID | $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Set-Content -LiteralPath $LockFile -Value $lockContent
Write-Host "[REFRESH $RunLabel] Lock acquired: $lockContent (waited ${waitedMinutes} min)" -ForegroundColor DarkGray
if (-not "$($env:PROPORACLE_BET_WINDOW)".Trim()) {
    $env:PROPORACLE_BET_WINDOW = $RunLabel
}
Write-Host "[REFRESH $RunLabel] Bet window $($env:PROPORACLE_BET_WINDOW) (line stamps + live publish; Force payout if lines moved)" -ForegroundColor DarkGray

function Publish-RefreshWindow {
    param(
        [string]$Date,
        [string]$Label,
        [string]$Suffix = "",
        # late_fetch already built Goblin-70 when lines moved; Publish-LiveSite
        # Ensure-DualCardTickets rebuilds only if the card drifted off dual.
        [switch]$RebuildGoblin70
    )
    $publish = Join-Path $Root "scripts\Publish-LiveSite.ps1"
    if (-not (Test-Path -LiteralPath $publish)) {
        $hit = Get-ChildItem -LiteralPath (Join-Path $Root "scripts") -Filter "Publish*Live*.ps1" -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($hit) { $publish = $hit.FullName }
    }
    if (-not (Test-Path -LiteralPath $publish)) {
        Write-Host "[REFRESH $Label] WARN: Publish-LiveSite.ps1 missing — site may stay on prior board" -ForegroundColor Yellow
        return
    }
    if ($RebuildGoblin70) {
        $g70 = Join-Path $Root "scripts\build_goblin70_tickets.py"
        if (Test-Path -LiteralPath $g70) {
            Write-Host "[REFRESH $Label] Ensuring Goblin-70 dual card before publish..." -ForegroundColor Cyan
            & py -3.14 $g70 --date $Date --write-web
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[REFRESH $Label] WARN: Goblin-70 rebuild exit $LASTEXITCODE" -ForegroundColor Yellow
            }
        }
    }
    $msg = "chore: live tickets/slate $Date $Label"
    if ($Suffix) { $msg = "$msg $Suffix" }
    Write-Host "[REFRESH $Label] Publishing live site JSON to origin/main..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $publish -RepoRoot $Root -CommitMessage $msg
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[REFRESH $Label] LIVE SITE PUBLISH FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
}

$scriptExit = 0
# After 20:00 ET the fetch targets tomorrow's slate (same rule as late_fetch).
$todayEt = (Get-Date).ToString("yyyy-MM-dd")
try {
    $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
    $etNow = [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
    if ($etNow.Hour -ge 20) {
        $todayEt = $etNow.Date.AddDays(1).ToString("yyyy-MM-dd")
    }
    else {
        $todayEt = $etNow.ToString("yyyy-MM-dd")
    }
} catch { }

$skipFetchAfterWait = ($waitedMinutes -ge $SkipFetchIfWaitedMinutes)
try {
    Set-Location $Root
    Write-Host "[REFRESH $RunLabel] Starting $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan

    if ($skipFetchAfterWait) {
        Write-Host "[REFRESH $RunLabel] Previous window just finished (waited ${waitedMinutes} min) — stamp + publish only, skip stacked fetch/payout" -ForegroundColor Yellow
        $stampPy = Join-Path $Root "scripts\stamp_fetch_window.py"
        if (Test-Path -LiteralPath $stampPy) {
            & py -3.14 $stampPy --date $todayEt --window $RunLabel --write-stamp
        }
        Publish-RefreshWindow -Date $todayEt -Label $RunLabel -Suffix "queued"
    }
    else {
        & pwsh -NoProfile -File $Snapshot -Label "$RunLabel PRE" -WriteState
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[REFRESH $RunLabel] PRE snapshot logging failed (continuing)" -ForegroundColor Yellow
        }

        $loggedHelper = Join-Path $PSScriptRoot "Invoke-LoggedPwsh.ps1"
        if (-not (Test-Path -LiteralPath $loggedHelper)) { $loggedHelper = Join-Path $Root "scripts\Invoke-LoggedPwsh.ps1" }
        $childLog = Join-Path $LogsDir ("late_fetch_child_{0}_{1:yyyy-MM-dd_HHmmss}.log" -f $RunLabel, (Get-Date))
        $lateArgs = @("-NoOverwrite", "-RunLabel", $RunLabel, "-SkipPayout")
        if (Test-Path -LiteralPath $loggedHelper) {
            . $loggedHelper
            $refreshExit = Invoke-LoggedPwsh -File $LateFetch -ArgumentList $lateArgs -LogPath $childLog -WorkingDirectory $Root
        } else {
            & pwsh -NoProfile -File $LateFetch @lateArgs
            $refreshExit = $LASTEXITCODE
        }

        & pwsh -NoProfile -File $Snapshot -Label "$RunLabel POST" -CompareToState -WriteState
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[REFRESH $RunLabel] POST snapshot logging failed" -ForegroundColor Yellow
        }

        $stampPy = Join-Path $Root "scripts\stamp_fetch_window.py"
        if (Test-Path -LiteralPath $stampPy) {
            $stampArgs = @("-3.14", $stampPy, "--date", $todayEt, "--window", $RunLabel, "--write-stamp")
            if ($refreshExit -ne 0) { $stampArgs += "--restamp-csvs" }
            & py @stampArgs
        }

        if ($refreshExit -ne 0) {
            Write-Host "[REFRESH $RunLabel] Refresh failed (exit $refreshExit) — still publishing this window" -ForegroundColor Red
            $scriptExit = $refreshExit
        }
        else {
            Write-Host "[REFRESH $RunLabel] Fetch complete" -ForegroundColor Green
        }

        $assertFresh = Join-Path $Root "scripts\Assert-ActiveSportsFresh.ps1"
        if (Test-Path -LiteralPath $assertFresh) {
            $freshJson = Join-Path $Root "logs\LAST_ACTIVE_SPORTS_FRESH.json"
            Write-Host "[REFRESH $RunLabel] Asserting active sports FRESH..." -ForegroundColor Cyan
            & pwsh -NoProfile -File $assertFresh -RepoRoot $Root -Today $todayEt -JsonOut $freshJson
            if ($LASTEXITCODE -ne 0) {
                Write-Host "[REFRESH $RunLabel] ACTIVE SPORTS FRESHNESS GATE FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
                if ($scriptExit -eq 0) { $scriptExit = $LASTEXITCODE }
            }
        }

        Publish-RefreshWindow -Date $todayEt -Label $RunLabel

        # Default Auto: only Force when today's stamp says lines moved / initial.
        # Stale last_line_window.json (wrong date / window=other) must not Force.
        $forcePayout = $false
        $deltaPath = Join-Path $Root "data\cache\last_line_window.json"
        if (Test-Path -LiteralPath $deltaPath) {
            try {
                $delta = Get-Content -LiteralPath $deltaPath -Raw | ConvertFrom-Json
                $stampDate = [string]$delta.date
                $stampWin = [string]$delta.window
                if ($stampDate -eq $todayEt -and $stampWin -and $stampWin -ne "other") {
                    $forcePayout = [bool]$delta.force_payout
                }
                else {
                    Write-Host ("[REFRESH $RunLabel] ignoring stale/other line stamp date={0} window={1} — Auto payout" -f `
                        $stampDate, $stampWin) -ForegroundColor Yellow
                }
                Write-Host ("[REFRESH $RunLabel] line stamp window={0} moved_this={1} from_initial={2} force_payout={3}" -f `
                    $delta.window, $delta.n_moved_this_window, $delta.n_moved_from_initial, $forcePayout) -ForegroundColor DarkGray
            } catch { }
        }
        $livePayScript = Join-Path $Root "scripts\run_live_payout_capture.ps1"
        if (Test-Path -LiteralPath $livePayScript) {
            # Auto: missing live_cdp + slips whose lines/types moved. Force only when stamp says so.
            $rescrapeMode = if ($forcePayout) { "Force" } else { "Auto" }
            Write-Host "[REFRESH $RunLabel] Payout CDP after fetch (RescrapeMode=$rescrapeMode)" -ForegroundColor Cyan
            $dualTickets = Join-Path $Root "ui_runner\templates\tickets_latest.json"
            try {
                $liveSrc = Get-Content -LiteralPath $livePayScript -Raw -ErrorAction SilentlyContinue
                $payArgs = @(
                    "-Date", $todayEt, "-Root", $Root, "-TicketsPath", $dualTickets,
                    "-RebuildRateCard", "-FillMissingTickets"
                )
                if ("$liveSrc" -match '\$Window') { $payArgs += @("-Window", $RunLabel) }
                if ("$liveSrc" -match 'RescrapeMode') {
                    $payArgs += @("-RescrapeMode", $rescrapeMode)
                } elseif ($forcePayout) {
                    $payArgs += "-Force"
                }
                & pwsh -NoProfile -File $livePayScript @payArgs
                Write-Host "[REFRESH $RunLabel] Payout scrape exit $LASTEXITCODE" -ForegroundColor DarkGray
            } catch {
                Write-Host "[REFRESH $RunLabel] WARN: payout scrape failed: $($_.Exception.Message)" -ForegroundColor Yellow
            }
            Publish-RefreshWindow -Date $todayEt -Label $RunLabel -Suffix "payout"
        }
        else {
            Write-Host "[REFRESH $RunLabel] WARN: run_live_payout_capture.ps1 missing — skip payout" -ForegroundColor Yellow
        }
    }
}
finally {
    if (Test-Path -LiteralPath $LockFile) {
        $currentLock = (Get-Content -LiteralPath $LockFile -ErrorAction SilentlyContinue | Select-Object -First 1)
        if ("$currentLock" -like "*PID $PID*") {
            Remove-Item -LiteralPath $LockFile -Force -ErrorAction SilentlyContinue
            Write-Host "[REFRESH $RunLabel] Lock released" -ForegroundColor DarkGray
        }
    }
    if ($acquiredMutex -and $refreshMutex) {
        try { $refreshMutex.ReleaseMutex() } catch { }
        $acquiredMutex = $false
    }
    if ($refreshMutex) {
        try { $refreshMutex.Dispose() } catch { }
        $refreshMutex = $null
    }
}

try { Stop-Transcript | Out-Null } catch { }
exit $scriptExit
