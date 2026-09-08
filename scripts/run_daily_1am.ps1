#requires -Version 5.1
<#
.SYNOPSIS
  Scheduled 1:00 AM update fetch: git pull main, pipeline + publish (no grader).

.NOTES
  Initial scrape + live publish is 9PM day-ahead (soccer/tennis/etc.).
  MLB Fill 11PM / 12AM discover tomorrow's MLB when PrizePicks posts it.
  This job is the overnight **update** + payout CDP republish of that slate.
  Grader + A1 historical actuals run separately at 3AM (unchanged).
  Always -SkipGrader -SkipHistoricalActuals. Live CDP runs after publish (same
  STEP D-payout as 8AM+), so 1AM writes payout_patch / rate cards for that board.
  Empty MLB at 9PM is normal; 11PM/12AM/1AM/5AM keep filling as the board appears.
  Registered by scripts\Register_Daily_Task.ps1 as "PropOracle - Daily 1AM".
#>
param()

$ErrorActionPreference = "Continue"
try { $Host.UI.RawUI.WindowTitle = "PropOracle - Daily 1AM" } catch { }
$Root = Split-Path $PSScriptRoot -Parent
$Daily = Join-Path $Root "scripts\run_daily.ps1"
$Snapshot = Join-Path $Root "scripts\log_prop_snapshot.ps1"

if (-not (Test-Path $Daily)) {
    Write-Error "Missing daily script: $Daily"
    exit 1
}
if (-not (Test-Path $Snapshot)) {
    Write-Error "Missing prop snapshot script: $Snapshot"
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

# Railway tracks origin/main. Prefer running daily on the main worktree when this
# checkout is a feature branch (main may already be locked in PropORACLE_main_cp).
$branch = (git rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
$mainWt = Get-MainWorktreeRoot
if ($branch -ne "main") {
    if ($mainWt -and (Test-Path -LiteralPath (Join-Path $mainWt "scripts\run_daily.ps1"))) {
        Write-Host "[1AM DAILY] On '$branch' - running daily inside main worktree: $mainWt" -ForegroundColor Yellow
        $Root = $mainWt
        $Daily = Join-Path $Root "scripts\run_daily.ps1"
        $Snapshot = Join-Path $Root "scripts\log_prop_snapshot.ps1"
        Set-Location $Root
    }
    else {
        Write-Host "[1AM DAILY] On '$branch' - switching to main for Railway freshness..." -ForegroundColor Yellow
        git checkout main 2>&1 | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[1AM DAILY] FAILED: cannot checkout main (locked by another worktree or WIP). Abort." -ForegroundColor Red
            exit 1
        }
    }
}

$LogsDir = Join-Path $Root "logs"
if (-not (Test-Path -LiteralPath $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
}
$WrapperLog = Join-Path $LogsDir ("task_1am_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))
try { Start-Transcript -Path $WrapperLog -Append | Out-Null } catch { }

Write-Host "[1AM DAILY] Pulling latest repository (main)..." -ForegroundColor Cyan
$EnsurePull = Join-Path $PSScriptRoot "Ensure-CleanPull.ps1"
if (-not (Test-Path -LiteralPath $EnsurePull)) {
    $EnsurePull = Join-Path $Root "scripts\Ensure-CleanPull.ps1"
}
& pwsh -NoProfile -File $EnsurePull -RepoRoot $Root -Label "[1AM DAILY]" -StashMessage ("proporacle-1am-pre-pull-{0:yyyyMMdd_HHmmss}" -f (Get-Date))
$pullPrepExit = $LASTEXITCODE
if ($pullPrepExit -eq 2) {
    Write-Host "[1AM DAILY] Source conflict reported — retrying publish-artifact repair..." -ForegroundColor Yellow
    & pwsh -NoProfile -File $EnsurePull -RepoRoot $Root -Label "[1AM DAILY]" -SkipPull
    $pullPrepExit = $LASTEXITCODE
}
if ($pullPrepExit -eq 2) {
    Write-Host "[1AM DAILY] FAILED: source-code conflicts block pull (resolve manually)." -ForegroundColor Red
    try { Stop-Transcript | Out-Null } catch { }
    exit 128
}
if ($pullPrepExit -ne 0) {
    Write-Host "[1AM DAILY] Pull prep warned (exit $pullPrepExit); continuing with local tree." -ForegroundColor Yellow
}

    $Today = (Get-Date).ToString("yyyy-MM-dd")
# 3AM Grader owns A1 + yesterday's grades so this job stays on fetch + payout CDP.
$dailyArgs = @("-SkipGrader", "-SkipHistoricalActuals")
$env:PROPORACLE_BET_WINDOW = "1AM"
Write-Host ("[1AM DAILY] Running run_daily.ps1 {0} (all-sport fetch + live payout CDP; grader is 3AM)" -f ($dailyArgs -join " ")) -ForegroundColor Cyan
$loggedHelper = Join-Path $PSScriptRoot "Invoke-LoggedPwsh.ps1"
if (-not (Test-Path -LiteralPath $loggedHelper)) { $loggedHelper = Join-Path $Root "scripts\Invoke-LoggedPwsh.ps1" }
$childLog = Join-Path $LogsDir ("run_daily_child_1am_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))
if (Test-Path -LiteralPath $loggedHelper) {
    . $loggedHelper
    $dailyExit = Invoke-LoggedPwsh -File $Daily -ArgumentList $dailyArgs -LogPath $childLog -WorkingDirectory $Root
} else {
    Write-Host "[1AM DAILY] WARN: Invoke-LoggedPwsh.ps1 missing — child output may not be logged" -ForegroundColor Yellow
    & pwsh -NoProfile -File $Daily @dailyArgs
    $dailyExit = $LASTEXITCODE
}

Write-Host "[1AM DAILY] Logging fetched prop snapshot..." -ForegroundColor Cyan
& pwsh -NoProfile -File $Snapshot -Label "1AM DAILY POST" -CompareToState -WriteState
if ($LASTEXITCODE -ne 0) {
    Write-Host "[1AM DAILY] Snapshot logging failed" -ForegroundColor Yellow
}

$Health = Join-Path $Root "scripts\Write-DailyRunHealth.ps1"
if (Test-Path -LiteralPath $Health) {
    Write-Host "[1AM DAILY] Writing health stamp (empty no_slate is OK at 1AM; 5AM/8AM keep updating)..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $Health -RepoRoot $Root -Label "1AM"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[1AM DAILY] Health stamp not green (exit $LASTEXITCODE) — continuing; later windows update" -ForegroundColor Yellow
    }
}
else {
    Write-Host "[1AM DAILY] WARN: Write-DailyRunHealth.ps1 missing" -ForegroundColor Yellow
}

$stampPy = Join-Path $Root "scripts\stamp_fetch_window.py"
if (Test-Path -LiteralPath $stampPy) {
    $stampArgs = @("-3.14", $stampPy, "--date", $Today, "--window", "1AM", "--write-stamp")
    $todayOut = Join-Path $Root "outputs\$Today"
    if ($dailyExit -ne 0 -and (Test-Path -LiteralPath $todayOut)) { $stampArgs += "--restamp-csvs" }
    & py @stampArgs
}

# Ensure dual card + fresh N-correct after the overnight update (run_daily already
# builds G70 + D-payout; re-run G70/payout here so write-back lands before publish).
$goblin70 = Join-Path $Root "scripts\build_goblin70_tickets.py"
if (Test-Path -LiteralPath $goblin70) {
    Write-Host "[1AM DAILY] Goblin-70 + mixer dual card for $Today..." -ForegroundColor Cyan
    & py -3.14 $goblin70 --date $Today --write-web
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[1AM DAILY] WARN: Goblin-70 dual card exit $LASTEXITCODE" -ForegroundColor Yellow
    }
}
$livePayScript = Join-Path $Root "scripts\run_live_payout_capture.ps1"
$dualTickets = Join-Path $Root "ui_runner\templates\tickets_latest.json"
if (Test-Path -LiteralPath $livePayScript) {
    Write-Host "[1AM DAILY] Payout CDP update (missing live + changed props)..." -ForegroundColor Cyan
    try {
        & pwsh -NoProfile -File $livePayScript -Date $Today -Root $Root -TicketsPath $dualTickets `
            -RescrapeMode Auto -Window "1AM" -RebuildRateCard -FillMissingTickets
        Write-Host "[1AM DAILY] Payout scrape exit $LASTEXITCODE" -ForegroundColor DarkGray
    } catch {
        Write-Host "[1AM DAILY] WARN: payout scrape failed (non-blocking): $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

$publish = Join-Path $Root "scripts\Publish-LiveSite.ps1"
if (Test-Path -LiteralPath $publish) {
    Write-Host "[1AM DAILY] Publishing live site JSON to origin/main..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $publish -RepoRoot $Root -CommitMessage "chore: live tickets/slate $Today 1AM"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[1AM DAILY] LIVE SITE PUBLISH FAILED (exit $LASTEXITCODE)" -ForegroundColor Red
    }
}
else {
    Write-Host "[1AM DAILY] WARN: Publish-LiveSite.ps1 missing" -ForegroundColor Yellow
}

if ($dailyExit -ne 0) {
    Write-Host "[1AM DAILY] run_daily failed (exit $dailyExit) — window stamp + publish still attempted" -ForegroundColor Red
    try { Stop-Transcript | Out-Null } catch { }
    exit $dailyExit
}

$AssertFresh = Join-Path $Root "scripts\Assert-ActiveSportsFresh.ps1"
if (Test-Path -LiteralPath $AssertFresh) {
    Write-Host "[1AM DAILY] Asserting active sports FRESH (soft — no_slate expected overnight)..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $AssertFresh -RepoRoot $Root -Today $Today -JsonOut (Join-Path $Root "logs\LAST_ACTIVE_SPORTS_FRESH.json")
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[1AM DAILY] Freshness not all-green (exit $LASTEXITCODE) — not failing 1AM; 8AM re-checks" -ForegroundColor Yellow
    }
}

Write-Host "[1AM DAILY] Complete" -ForegroundColor Green
try { Stop-Transcript | Out-Null } catch { }
exit 0
