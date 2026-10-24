#requires -Version 7.2
<#
.SYNOPSIS
  Tuesday post-week NFL refresh: route/WOPR, unit ranks, pace, QB mobility,
  red-zone rates, division O/D, O/D md.

.DESCRIPTION
  Run after Week N grades land. Default Week 2 / season 2026.
  Rebuilds soft-context CSVs then regenerates nfl_od_rankings_current.md
  (and archives week{N+1} unless sealed). Ledger / Odds API stay manual.

.EXAMPLE
  pwsh -File Sports\NFL\scripts\run_tuesday_post_week.ps1 -Season 2026 -Week 2
#>
param(
    [int]$Season = 2026,
    [int]$Week = 2,
    [switch]$SkipRoute,
    [switch]$SkipUnit,
    [switch]$SkipPace,
    [switch]$SkipMobility,
    [switch]$SkipRedZone,
    [switch]$SkipDivision,
    [switch]$SkipOdMd,
    [switch]$ForceArchive
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
Set-Location $RepoRoot
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$NextWeek = $Week + 1

function Invoke-Py {
    param([string]$Label, [string[]]$Args)
    Write-Host ""
    Write-Host "==> $Label" -ForegroundColor Cyan
    Write-Host ("    py -3.14 " + ($Args -join " ")) -ForegroundColor DarkGray
    & py -3.14 @Args
    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $Label (exit $LASTEXITCODE)" -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "OK: $Label" -ForegroundColor Green
}

Write-Host "NFL Tuesday post-week  season=$Season week=$Week -> next=$NextWeek" -ForegroundColor Cyan
Write-Host "Repo: $RepoRoot" -ForegroundColor DarkGray

if (-not $SkipRoute) {
    Invoke-Py "route participation / WOPR" @(
        "Sports\NFL\scripts\build_nfl_route_participation.py",
        "--season", "$Season",
        "--week", "$Week"
    )
}
if (-not $SkipUnit) {
    Invoke-Py "unit line ranks" @(
        "Sports\NFL\scripts\build_nfl_unit_line_ranks.py",
        "--season", "$Season"
    )
}
if (-not $SkipPace) {
    Invoke-Py "pace ranks" @(
        "Sports\NFL\scripts\build_nfl_pace_ranks.py",
        "--season", "$Season"
    )
}
if (-not $SkipMobility) {
    Invoke-Py "QB mobility tiers" @(
        "Sports\NFL\scripts\build_nfl_qb_mobility.py",
        "--seasons", "$($Season-1),$Season"
    )
}
if (-not $SkipRedZone) {
    Invoke-Py "red-zone rates" @(
        "Sports\NFL\scripts\build_nfl_red_zone_rates.py",
        "--seasons", "$($Season-1),$Season"
    )
}
if (-not $SkipDivision) {
    # Prefer cache rebuild; --refresh-games can 403 on ESPN date windows mid-season.
    Invoke-Py "division / team unit ranks" @(
        "Sports\NFL\scripts\build_nfl_division_ranks.py"
    )
}
if (-not $SkipOdMd) {
    $odArgs = @(
        "Sports\NFL\scripts\build_nfl_od_rankings_md.py",
        "--season", "$Season",
        "--week", "$NextWeek"
    )
    if ($ForceArchive) { $odArgs += "--force-archive" }
    Invoke-Py "O/D rankings current.md (+ week$NextWeek archive)" $odArgs
    Invoke-Py "context freshness check" @(
        "-c",
        "from utils.nfl_context_freshness import warn_if_stale; warn_if_stale(max_age_hours=24*10)"
    )
}

Write-Host ""
Write-Host "Next (manual / follow-up):" -ForegroundColor Yellow
Write-Host "  1. Grade Week $Week ledger -> Sports/NFL/data/nfl_prop_gate_ledger_week$Week.*"
Write-Host "  2. Confirm Sports/NFL/data/nfl_od_rankings_current.md (Week $NextWeek leans)"
Write-Host "  3. Fix Odds API quota (remaining must be > 0) before NFL step6b"
Write-Host "  4. Answer ledger questions in nfl_tuesday_week3_checklist.md"
Write-Host ""
Write-Host "DONE refreshes. Live tickets must use nfl_od_rankings_current.md (not sealed week archives)." -ForegroundColor Green
