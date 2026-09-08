#requires -Version 5.1
<#
.SYNOPSIS
  Push live tickets/slate JSON to origin/main (Railway + GitHub raw) from the main worktree.

.NOTES
  Called after 8AM/9:45/10:30/1PM/4:30 refreshes so the site is not left on the 1AM board.
  Mirrors run_pipeline.ps1 Publish-LiveSiteJsonToMain.
#>
param(
    [string]$RepoRoot = "",
    [string]$CommitMessage = ""
)

$ErrorActionPreference = "Continue"
$Root = if ($RepoRoot) { $RepoRoot } else { Split-Path $PSScriptRoot -Parent }

function Get-MainWorktreeRoot {
    param([string]$RepoRoot = $Root)
    $porcelain = git -C $RepoRoot worktree list --porcelain 2>$null
    if (-not $porcelain) { return $RepoRoot }
    $wt = $null
    foreach ($line in $porcelain) {
        if ($line -match '^worktree (.+)$') { $wt = $Matches[1].Trim() }
        elseif ($line -match '^branch refs/heads/main$' -and $wt) { return $wt }
        elseif ($line -eq "") { $wt = $null }
    }
    $br = (git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
    if ($br -eq "main") { return $RepoRoot }
    return $null
}

function Get-TicketsCardKind {
    param([string]$TicketsPath)
    if (-not (Test-Path -LiteralPath $TicketsPath)) { return "missing" }
    try {
        $j = Get-Content -LiteralPath $TicketsPath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch { return "missing" }
    $mode = [string]$j.mode
    if ($mode -match 'goblin70\+graded_main') { return "dual" }
    $hasG70 = $false
    $hasMixer = $false
    foreach ($g in @($j.groups)) {
        $n = [string]$g.group_name
        if ($n -match 'Goblin-70|YOLO Goblin') { $hasG70 = $true }
        else { $hasMixer = $true }
    }
    if ($hasG70 -and $hasMixer) { return "dual" }
    if ($hasG70) { return "goblin70_only" }
    if ($hasMixer) { return "mixer_only" }
    return "empty"
}

function Ensure-DualCardTickets {
    param([string]$RepoRoot)
    $tpl = Join-Path $RepoRoot "ui_runner\templates\tickets_latest.json"
    $rt = Join-Path $RepoRoot "ui_runner\runtime\tickets_latest.json"
    $kind = Get-TicketsCardKind -TicketsPath $tpl
    if ($kind -eq "dual") { return $true }

    Write-Host "[PUBLISH] tickets card is '$kind' — rebuilding Goblin-70 dual card before push" -ForegroundColor Yellow
    $slateDate = (Get-Date).ToString("yyyy-MM-dd")
    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        $et = [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
        # After 8PM ET day-ahead owns tomorrow's slate.
        if ($et.Hour -ge 20) { $slateDate = $et.Date.AddDays(1).ToString("yyyy-MM-dd") }
        else { $slateDate = $et.ToString("yyyy-MM-dd") }
    } catch { }
    if (Test-Path -LiteralPath $tpl) {
        try {
            $j = Get-Content -LiteralPath $tpl -Raw -Encoding utf8 | ConvertFrom-Json
            $d = [string]$j.date
            if ($d -match '^\d{4}-\d{2}-\d{2}') { $slateDate = $d.Substring(0, 10) }
        } catch { }
    }
    $g70 = Join-Path $RepoRoot "scripts\build_goblin70_tickets.py"
    if (-not (Test-Path -LiteralPath $g70)) {
        Write-Host "[PUBLISH] FAILED: build_goblin70_tickets.py missing" -ForegroundColor Red
        return $false
    }
    & py -3.14 $g70 --date $slateDate --write-web
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[PUBLISH] FAILED: Goblin-70 rebuild exit $LASTEXITCODE" -ForegroundColor Red
        return $false
    }
    if ((Test-Path -LiteralPath $tpl) -and (Test-Path -LiteralPath (Split-Path $rt -Parent))) {
        Copy-Item -LiteralPath $tpl -Destination $rt -Force -ErrorAction SilentlyContinue
    }
    $kind2 = Get-TicketsCardKind -TicketsPath $tpl
    if ($kind2 -ne "dual") {
        Write-Host "[PUBLISH] FAILED: after rebuild card is still '$kind2' (need Goblin-70 + mixer)" -ForegroundColor Red
        return $false
    }
    Write-Host "[PUBLISH] dual card OK ($slateDate)" -ForegroundColor Green
    return $true
}

$MainRoot = Get-MainWorktreeRoot
if (-not $MainRoot) {
    Write-Host "[PUBLISH] FAILED: no worktree has main checked out" -ForegroundColor Red
    exit 1
}

Write-Host "[PUBLISH] Live site JSON -> origin/main ($MainRoot)" -ForegroundColor Cyan

# Hard gate: never push mixer-only / Goblin-only to Railway.
if (-not (Ensure-DualCardTickets -RepoRoot $Root)) {
    exit 1
}

$assertPy = Join-Path $Root "scripts\assert_live_publish.py"
if (-not (Test-Path -LiteralPath $assertPy)) {
    # Scheduled jobs run from main_cp; keep a copy of the guard there.
    $srcAssert = Join-Path (Split-Path $PSScriptRoot -Parent) "scripts\assert_live_publish.py"
    if ($srcAssert -ne $assertPy -and (Test-Path -LiteralPath $srcAssert)) {
        Copy-Item -LiteralPath $srcAssert -Destination $assertPy -Force -ErrorAction SilentlyContinue
    }
}
if (Test-Path -LiteralPath $assertPy) {
    Write-Host "[PUBLISH] assert dual-card + runtime/templates sync" -ForegroundColor DarkGray
    & py -3.14 $assertPy --root $Root --fix
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[PUBLISH] FAILED: live JSON guard (Goblin-70+mixer, matching dates)" -ForegroundColor Red
        exit 1
    }
} else {
    $kind = Get-TicketsCardKind -TicketsPath (Join-Path $Root "ui_runner\templates\tickets_latest.json")
    if ($kind -ne "dual") {
        Write-Host "[PUBLISH] FAILED: assert_live_publish.py missing and card is '$kind'" -ForegroundColor Red
        exit 1
    }
    Write-Host "[PUBLISH] dual-card PowerShell guard OK (assert script missing)" -ForegroundColor DarkGray
}

# templates/ = GitHub raw contract (Railway). runtime/ = canonical disk copy.
# mobile/www is not live (Android loads Railway remotely).
$liveRel = @(
    "ui_runner/runtime/tickets_latest.json",
    "ui_runner/templates/tickets_latest.json",
    "ui_runner/runtime/slate_latest.json",
    "ui_runner/templates/slate_latest.json",
    "ui_runner/runtime/slate_display_date.json",
    "ui_runner/templates/slate_display_date.json",
    "ui_runner/runtime/pipeline_status.json",
    "ui_runner/templates/pipeline_status.json",
    "ui_runner/runtime/tickets_winrate_latest.json",
    "ui_runner/templates/tickets_winrate_latest.json",
    "ui_runner/runtime/sport_breakdown.json",
    "ui_runner/templates/sport_breakdown.json",
    "ui_runner/runtime/last_fetch_window.json",
    "ui_runner/templates/last_fetch_window.json",
    "data/reports/bet_windows_latest.json"
)
Get-ChildItem -LiteralPath (Join-Path $Root "ui_runner\runtime") -Filter "slate_sport_*.json" -ErrorAction SilentlyContinue |
    ForEach-Object { $liveRel += ("ui_runner/runtime/" + $_.Name) }
Get-ChildItem -LiteralPath (Join-Path $Root "ui_runner\templates") -Filter "slate_sport_*.json" -ErrorAction SilentlyContinue |
    ForEach-Object { $liveRel += ("ui_runner/templates/" + $_.Name) }

$toPublish = @()
foreach ($rel in $liveRel) {
    $full = Join-Path $Root ($rel -replace "/", "\")
    if (Test-Path -LiteralPath $full) { $toPublish += $rel }
}
if (-not $toPublish.Count) {
    Write-Host "[PUBLISH] No live site JSON found" -ForegroundColor Yellow
    exit 0
}

Push-Location $MainRoot
try {
    git pull --ff-only origin main 2>&1 | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
    foreach ($rel in $toPublish) {
        $src = Join-Path $Root ($rel -replace "/", "\")
        $dst = Join-Path $MainRoot ($rel -replace "/", "\")
        $samePath = $false
        try {
            if ((Test-Path -LiteralPath $src) -and (Test-Path -LiteralPath $dst)) {
                $samePath = (
                    [IO.Path]::GetFullPath($src).TrimEnd('\') -eq
                    [IO.Path]::GetFullPath($dst).TrimEnd('\')
                )
            }
        } catch { $samePath = ($src -eq $dst) }
        if (-not $samePath) {
            $dstDir = Split-Path $dst -Parent
            if (-not (Test-Path -LiteralPath $dstDir)) {
                New-Item -ItemType Directory -Path $dstDir -Force | Out-Null
            }
            Copy-Item -LiteralPath $src -Destination $dst -Force
        }
        git add -- $rel 2>&1 | Out-Null
    }
    $msg = if ($CommitMessage) { $CommitMessage } else { "chore: live tickets/slate $(Get-Date -Format 'yyyy-MM-dd HH:mm')" }
    git commit -m $msg 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $pushOut = git push origin main 2>&1
        foreach ($line in $pushOut) { Write-Host "    $line" -ForegroundColor DarkGray }
        if ($LASTEXITCODE -eq 0) {
            Write-Host "[PUBLISH] OK — origin/main updated" -ForegroundColor Green
            exit 0
        }
        Write-Host "[PUBLISH] FAILED: git push origin main" -ForegroundColor Red
        exit 1
    }
    Write-Host "[PUBLISH] no JSON changes vs main" -ForegroundColor DarkGray
    exit 0
} finally {
    Pop-Location
}
