#requires -Version 5.1
<#
.SYNOPSIS
  Overnight MLB board discovery for Eastern tomorrow (11PM / midnight fill).

.NOTES
  9PM day-ahead builds soccer/tennis/etc. PrizePicks usually has not posted
  tomorrow's MLB yet at 9PM. This job pulls MLB for slate D starting ~11PM
  and again at midnight; 1AM / 5AM continue filling.

  Soft success when MLB is still empty (not posted). When rows appear:
  MLBOnly pipeline -> Goblin-70 dual card -> payout Auto -> live publish.

  Registered by scripts\Register_Daily_Task.ps1 as:
    PropOracle - MLB Fill 11PM
    PropOracle - MLB Fill 12AM
#>
param(
    [string]$Window = "11PM"
)

$ErrorActionPreference = "Continue"
try { $Host.UI.RawUI.WindowTitle = "PropOracle - MLB Fill $Window" } catch { }
$Root = Split-Path $PSScriptRoot -Parent
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

function Get-CsvDataRowCount {
    param([string]$CsvPath)
    if (-not (Test-Path -LiteralPath $CsvPath)) { return 0 }
    try {
        $n = 0
        Get-Content -LiteralPath $CsvPath | ForEach-Object { $n++ }
        return [Math]::Max(0, $n - 1)
    } catch { return 0 }
}

$branch = (git rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
$mainWt = Get-MainWorktreeRoot
if ($branch -ne "main") {
    if ($mainWt -and (Test-Path -LiteralPath (Join-Path $mainWt "scripts\run_mlb_day_ahead_fill.ps1"))) {
        Write-Host "[MLB FILL $Window] On '$branch' - running inside main worktree: $mainWt" -ForegroundColor Yellow
        $Root = $mainWt
        Set-Location $Root
    }
}

$LogsDir = Join-Path $Root "logs"
if (-not (Test-Path -LiteralPath $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
}
$WrapperLog = Join-Path $LogsDir ("task_mlb_fill_{0}_{1:yyyy-MM-dd_HHmmss}.log" -f ($Window -replace '\s',''), (Get-Date))
try { Start-Transcript -Path $WrapperLog -Append | Out-Null } catch { }

$etNow = Get-EasternNow
# Always target Eastern tomorrow for this overnight MLB discovery window.
$SlateDate = $etNow.Date.AddDays(1).ToString("yyyy-MM-dd")
# After midnight ET (00:xx), "tomorrow" from calendar is already slate day — use today.
if ($etNow.Hour -lt 4) {
    $SlateDate = $etNow.ToString("yyyy-MM-dd")
}

$env:PROPORACLE_BET_WINDOW = "$Window"
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
if (-not "$($env:PROPORACLE_CURL_IMPERSONATE)".Trim()) {
    $env:PROPORACLE_CURL_IMPERSONATE = "chrome131"
}

Write-Host "[MLB FILL $Window] Slate=$SlateDate (ET now $($etNow.ToString('yyyy-MM-dd HH:mm')))" -ForegroundColor Cyan

$EnsurePull = Join-Path $PSScriptRoot "Ensure-CleanPull.ps1"
if (-not (Test-Path -LiteralPath $EnsurePull)) { $EnsurePull = Join-Path $Root "scripts\Ensure-CleanPull.ps1" }
if (Test-Path -LiteralPath $EnsurePull) {
    Write-Host "[MLB FILL $Window] Pulling latest main..." -ForegroundColor DarkGray
    & pwsh -NoProfile -File $EnsurePull -RepoRoot $Root -Label "[MLB FILL $Window]" `
        -StashMessage ("proporacle-mlb-fill-pre-pull-{0:yyyyMMdd_HHmmss}" -f (Get-Date))
}

$SportsRoot = Join-Path $Root "Sports"
$MLBDir = Join-Path $SportsRoot "MLB"
$outDir = Join-Path $Root "outputs\$SlateDate\mlb"
if (-not (Test-Path -LiteralPath $outDir)) {
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
}
$mlbStep1 = Join-Path $outDir "step1_mlb_props.csv"
$CdpUrl = if ($env:PROPORACLE_MLB_CDP_URL) { "$($env:PROPORACLE_MLB_CDP_URL)".Trim() } else { "http://127.0.0.1:9222" }
$CdpReachable = $false
try {
    $probe = Invoke-RestMethod -Uri "$CdpUrl/json/version" -TimeoutSec 2 -ErrorAction Stop
    if ($probe) { $CdpReachable = $true }
} catch { $CdpReachable = $false }

$mlbHttpArgs = @(
    "--date", "$SlateDate",
    "--output", $mlbStep1,
    "--per-page", "250",
    "--max-pages", "10",
    "--max-retries", "4",
    "--api-session-waves", "3",
    "--append"
)

Write-Host "[MLB FILL $Window] Fetching PrizePicks MLB for $SlateDate (cdp=$CdpReachable)..." -ForegroundColor Cyan
Push-Location $MLBDir
try {
    if ($CdpReachable) {
        & py -3.14 -u ".\scripts\step1_fetch_prizepicks_mlb.py" `
            "--cdp" $CdpUrl `
            "--timeout" "120" `
            "--retries" "1" `
            "--retry_delay" "5" `
            "--append" `
            "--date" "$SlateDate" `
            "--output" $mlbStep1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[MLB FILL $Window] CDP failed — HTTP fallback" -ForegroundColor Yellow
            & py -3.14 -u ".\scripts\step1_fetch_prizepicks_mlb.py" @mlbHttpArgs
        }
    } else {
        & py -3.14 -u ".\scripts\step1_fetch_prizepicks_mlb.py" @mlbHttpArgs
    }
} finally {
    Pop-Location
}

$nRows = Get-CsvDataRowCount -CsvPath $mlbStep1
Write-Host "[MLB FILL $Window] step1 rows=$nRows -> $mlbStep1" -ForegroundColor DarkGray

$Snapshot = Join-Path $Root "scripts\log_prop_snapshot.ps1"
if ((Test-Path -LiteralPath $Snapshot) -and $nRows -gt 0) {
    & pwsh -NoProfile -File $Snapshot -Label "MLB FILL $Window POST" -CompareToState -WriteState
}

if ($nRows -le 0) {
    Write-Host "[MLB FILL $Window] MLB not on board yet for $SlateDate — soft OK (1AM/5AM continue filling)" -ForegroundColor Yellow
    try { Stop-Transcript | Out-Null } catch { }
    exit 0
}

# Mirror for pipeline consumers
$mirror = Join-Path $MLBDir "data\outputs\step1_mlb_props.csv"
$mirrorDir = Split-Path $mirror -Parent
if (-not (Test-Path -LiteralPath $mirrorDir)) {
    New-Item -ItemType Directory -Path $mirrorDir -Force | Out-Null
}
Copy-Item -LiteralPath $mlbStep1 -Destination $mirror -Force

$pipe = Join-Path $Root "run_pipeline.ps1"
if (-not (Test-Path -LiteralPath $pipe)) {
    Write-Host "[MLB FILL $Window] WARN: run_pipeline.ps1 missing" -ForegroundColor Yellow
    try { Stop-Transcript | Out-Null } catch { }
    exit 0
}

Write-Host "[MLB FILL $Window] Running MLBOnly pipeline for $SlateDate..." -ForegroundColor Cyan
& pwsh -NoProfile -File $pipe -Date $SlateDate -MLBOnly -SkipLivePayoutCapture -SkipPush
$pipeExit = $LASTEXITCODE
if ($pipeExit -ne 0) {
    Write-Host "[MLB FILL $Window] WARN: MLBOnly pipeline exit $pipeExit" -ForegroundColor Yellow
}

$goblin70 = Join-Path $Root "scripts\build_goblin70_tickets.py"
if (Test-Path -LiteralPath $goblin70) {
    Write-Host "[MLB FILL $Window] Rebuilding Goblin-70 + mixer dual card..." -ForegroundColor Cyan
    & py -3.14 $goblin70 --date $SlateDate --write-web
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[MLB FILL $Window] WARN: Goblin-70 exit $LASTEXITCODE" -ForegroundColor Yellow
    }
}

$livePay = Join-Path $Root "scripts\run_live_payout_capture.ps1"
$dualTickets = Join-Path $Root "ui_runner\templates\tickets_latest.json"
if ((Test-Path -LiteralPath $livePay) -and (Test-Path -LiteralPath $dualTickets)) {
    Write-Host "[MLB FILL $Window] Payout CDP Auto (missing + changed) for $SlateDate..." -ForegroundColor Cyan
    try {
        & pwsh -NoProfile -File $livePay -Date $SlateDate -Root $Root -TicketsPath $dualTickets `
            -RescrapeMode Auto -Window $Window -FillMissingTickets
    } catch {
        Write-Host "[MLB FILL $Window] WARN: payout scrape failed (non-blocking): $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

$publish = Join-Path $Root "scripts\Publish-LiveSite.ps1"
if (Test-Path -LiteralPath $publish) {
    Write-Host "[MLB FILL $Window] Publishing live site..." -ForegroundColor Cyan
    & pwsh -NoProfile -File $publish -RepoRoot $Root `
        -CommitMessage "chore: MLB fill $SlateDate $Window"
}

Write-Host "[MLB FILL $Window] Done slate=$SlateDate rows=$nRows" -ForegroundColor Green
try { Stop-Transcript | Out-Null } catch { }
exit 0
