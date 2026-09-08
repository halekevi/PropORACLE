#requires -Version 5.1
<#
.SYNOPSIS
  Scheduled 9:00 PM initial fetch for Eastern tomorrow, then live /tickets publish.

.NOTES
  First scrape of slate D happens the evening of D-1. Most of today's props have
  already started or finished by 9PM ET, so this job publishes tomorrow's dual
  card (Goblin-70 + mixer) to Railway. 1AM / 5AM / 8AM+ on day D are updates.
  Grader 3AM is unchanged (yesterday).

  Registered by scripts\Register_Daily_Task.ps1 as "PropOracle - DayAhead 9PM".
#>
param()

$ErrorActionPreference = "Continue"
try { $Host.UI.RawUI.WindowTitle = "PropOracle - DayAhead 9PM" } catch { }
$Root = Split-Path $PSScriptRoot -Parent
$Daily = Join-Path $Root "scripts\run_daily.ps1"
$Snapshot = Join-Path $Root "scripts\log_prop_snapshot.ps1"

if (-not (Test-Path $Daily)) {
    Write-Error "Missing daily script: $Daily"
    exit 1
}

Set-Location $Root

function Get-MainWorktreeRoot {
    param([string]$RepoRoot = $Root)
    $porcelain = git -C $RepoRoot worktree list --porcelain 2>$null
    if (-not $porcelain) { return $null }
    $wt = $null
    foreach ($line in $porcelain) {
        if ($line -match '^worktree (.+)$') { $wt = $Matches[1].Trim() }
        elseif ($line -match '^branch refs/heads/main$' -and $wt) { return $wt }
        elseif ($line -eq "") { $wt = $null }
    }
    return $null
}

function Get-EasternNow {
    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        return [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
    } catch {
        return Get-Date
    }
}

$branch = (git rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
$mainWt = Get-MainWorktreeRoot
if ($branch -ne "main") {
    if ($mainWt -and (Test-Path -LiteralPath (Join-Path $mainWt "scripts\run_daily.ps1"))) {
        Write-Host "[DAY-AHEAD] On '$branch' - running inside main worktree: $mainWt" -ForegroundColor Yellow
        $Root = $mainWt
        $Daily = Join-Path $Root "scripts\run_daily.ps1"
        $Snapshot = Join-Path $Root "scripts\log_prop_snapshot.ps1"
        Set-Location $Root
    }
    else {
        Write-Host "[DAY-AHEAD] On '$branch' - switching to main..." -ForegroundColor Yellow
        git checkout main 2>&1 | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[DAY-AHEAD] FAILED: cannot checkout main. Abort." -ForegroundColor Red
            exit 1
        }
    }
}

$LogsDir = Join-Path $Root "logs"
if (-not (Test-Path -LiteralPath $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
}
$WrapperLog = Join-Path $LogsDir ("task_dayahead_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))
try { Start-Transcript -Path $WrapperLog -Append | Out-Null } catch { }

$etNow = Get-EasternNow
# Before 8AM ET this is an accidental catchup on today's slate, not tomorrow.
if ($etNow.Hour -lt 8) {
    $SlateDate = $etNow.ToString("yyyy-MM-dd")
} else {
    $SlateDate = $etNow.Date.AddDays(1).ToString("yyyy-MM-dd")
}
$TodayEt = $etNow.ToString("yyyy-MM-dd")
Write-Host "[DAY-AHEAD] Initial fetch for slate $SlateDate (ET now $TodayEt $($etNow.ToString('HH:mm')))" -ForegroundColor Cyan

$EnsurePull = Join-Path $PSScriptRoot "Ensure-CleanPull.ps1"
if (-not (Test-Path -LiteralPath $EnsurePull)) {
    $EnsurePull = Join-Path $Root "scripts\Ensure-CleanPull.ps1"
}
if (Test-Path -LiteralPath $EnsurePull) {
    Write-Host "[DAY-AHEAD] Pulling latest repository (main)..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $EnsurePull -RepoRoot $Root -Label "[DAY-AHEAD]" -StashMessage ("proporacle-dayahead-pre-pull-{0:yyyyMMdd_HHmmss}" -f (Get-Date))
    $pullPrepExit = $LASTEXITCODE
    if ($pullPrepExit -eq 2) {
        & pwsh -NoProfile -File $EnsurePull -RepoRoot $Root -Label "[DAY-AHEAD]" -SkipPull
        $pullPrepExit = $LASTEXITCODE
    }
    if ($pullPrepExit -eq 2) {
        Write-Host "[DAY-AHEAD] FAILED: source-code conflicts block pull." -ForegroundColor Red
        try { Stop-Transcript | Out-Null } catch { }
        exit 128
    }
}

$env:PROPORACLE_BET_WINDOW = "9PM"
# Skip D-payout inside daily: G70 is rebuilt below, then we scrape that dual card
# before Publish-LiveSite so Railway gets live N-correct floors (not pending_live).
$dailyArgs = @(
    "-Date", $SlateDate,
    "-GradeDate", $TodayEt,
    "-SkipGrader",
    "-SkipHistoricalActuals",
    "-SkipLivePayout"
)
Write-Host ("[DAY-AHEAD] Running run_daily.ps1 {0} (G70 + payout CDP + live publish after)" -f ($dailyArgs -join " ")) -ForegroundColor Cyan
$loggedHelper = Join-Path $PSScriptRoot "Invoke-LoggedPwsh.ps1"
if (-not (Test-Path -LiteralPath $loggedHelper)) { $loggedHelper = Join-Path $Root "scripts\Invoke-LoggedPwsh.ps1" }
$childLog = Join-Path $LogsDir ("run_daily_child_dayahead_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))
$dailyExit = 1
if (Test-Path -LiteralPath $loggedHelper) {
    . $loggedHelper
    $dailyExit = Invoke-LoggedPwsh -File $Daily -ArgumentList $dailyArgs -LogPath $childLog -WorkingDirectory $Root
} else {
    & pwsh -NoProfile -File $Daily @dailyArgs
    $dailyExit = $LASTEXITCODE
}

$goblin70 = Join-Path $Root "scripts\build_goblin70_tickets.py"
if (Test-Path -LiteralPath $goblin70) {
    Write-Host "[DAY-AHEAD] Goblin-70 + mixer dual card for $SlateDate..." -ForegroundColor Cyan
    & py -3.14 $goblin70 --date $SlateDate --write-web
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[DAY-AHEAD] WARN: Goblin-70 dual card exit $LASTEXITCODE" -ForegroundColor Yellow
    }
}

# Initial payout scrape on the dual card (full Force if no prior ok flag; else changed+missing).
$livePayScript = Join-Path $Root "scripts\run_live_payout_capture.ps1"
$dualTickets = Join-Path $Root "ui_runner\templates\tickets_latest.json"
if (Test-Path -LiteralPath $livePayScript) {
    Write-Host "[DAY-AHEAD] Live payout CDP after initial fetch (dual card)..." -ForegroundColor Cyan
    try {
        & pwsh -NoProfile -File $livePayScript -Date $SlateDate -Root $Root -TicketsPath $dualTickets `
            -RescrapeMode Auto -Window "9PM" -RebuildRateCard -FillMissingTickets
        Write-Host "[DAY-AHEAD] Payout scrape exit $LASTEXITCODE" -ForegroundColor DarkGray
    } catch {
        Write-Host "[DAY-AHEAD] WARN: payout scrape failed (non-blocking): $($_.Exception.Message)" -ForegroundColor Yellow
    }
} else {
    Write-Host "[DAY-AHEAD] WARN: run_live_payout_capture.ps1 missing — tickets stay pending_live" -ForegroundColor Yellow
}

$newTickets = Join-Path $Root "ui_runner\templates\tickets_latest.json"
$aheadDir = Join-Path $Root "outputs\$SlateDate"
if (-not (Test-Path -LiteralPath $aheadDir)) {
    New-Item -ItemType Directory -Path $aheadDir -Force | Out-Null
}
$aheadTickets = Join-Path $aheadDir "tickets_day_ahead.json"
$uiAhead = Join-Path $Root "ui_runner\data\tickets_day_ahead.json"
if (Test-Path -LiteralPath $newTickets) {
    Copy-Item -LiteralPath $newTickets -Destination $aheadTickets -Force
    $uiAheadDir = Split-Path $uiAhead -Parent
    if (-not (Test-Path -LiteralPath $uiAheadDir)) {
        New-Item -ItemType Directory -Path $uiAheadDir -Force | Out-Null
    }
    Copy-Item -LiteralPath $newTickets -Destination $uiAhead -Force
    Write-Host "[DAY-AHEAD] Archived tomorrow card -> $aheadTickets" -ForegroundColor Cyan
} else {
    Write-Host "[DAY-AHEAD] WARN: no tickets_latest.json after pipeline (exit $dailyExit)" -ForegroundColor Yellow
}

if (Test-Path -LiteralPath $Snapshot) {
    Write-Host "[DAY-AHEAD] Logging fetched prop snapshot..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $Snapshot -Label "DAY-AHEAD 9PM POST" -CompareToState -WriteState
}

$watchlist = Join-Path $Root "scripts\day_ahead_standard_under_watchlist.py"
if (Test-Path -LiteralPath $watchlist) {
    Write-Host "[DAY-AHEAD] Standard UNDER watchlist for $SlateDate..." -ForegroundColor Cyan
    & py -3.14 $watchlist --date $SlateDate --game-date $SlateDate
}

$stampPy = Join-Path $Root "scripts\stamp_fetch_window.py"
if (Test-Path -LiteralPath $stampPy) {
    $stampArgs = @("-3.14", $stampPy, "--date", $SlateDate, "--window", "9PM", "--write-stamp")
    if ($dailyExit -ne 0 -and (Test-Path -LiteralPath $aheadDir)) { $stampArgs += "--restamp-csvs" }
    & py @stampArgs
}

$Health = Join-Path $Root "scripts\Write-DailyRunHealth.ps1"
if (Test-Path -LiteralPath $Health) {
    Write-Host "[DAY-AHEAD] Writing health stamp for $SlateDate..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $Health -RepoRoot $Root -Label "9PM" -ExpectDate $SlateDate
}

$publish = Join-Path $Root "scripts\Publish-LiveSite.ps1"
if (Test-Path -LiteralPath $publish) {
    Write-Host "[DAY-AHEAD] Publishing live site JSON to origin/main..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $publish -RepoRoot $Root -CommitMessage "chore: live tickets/slate $SlateDate 9PM day-ahead"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[DAY-AHEAD] LIVE SITE PUBLISH FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
}
else {
    Write-Host "[DAY-AHEAD] WARN: Publish-LiveSite.ps1 missing" -ForegroundColor Yellow
}

$flagDir = Join-Path $Root "data\cache"
if (-not (Test-Path -LiteralPath $flagDir)) {
    New-Item -ItemType Directory -Path $flagDir -Force | Out-Null
}
$flag = Join-Path $flagDir "day_ahead_ok_$SlateDate.flag"
@(
    "slate=$SlateDate",
    "fetched_et=$($etNow.ToString('o'))",
    "exit=$dailyExit",
    "tickets=$(Test-Path -LiteralPath $aheadTickets)"
) | Set-Content -LiteralPath $flag -Encoding utf8

Write-Host "[DAY-AHEAD] Done slate=$SlateDate daily_exit=$dailyExit (live /tickets published)" -ForegroundColor Green
try { Stop-Transcript | Out-Null } catch { }
exit $dailyExit
