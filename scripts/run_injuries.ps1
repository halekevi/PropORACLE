#requires -Version 5.1
<#
.SYNOPSIS
  Daily injury collect + board attach + measure + report.

.PARAMETER Date
  Slate date YYYY-MM-DD (default: today ET).

.PARAMETER Window
  Run label (5AM / 1030AM / 430PM / T60) — controls ntfy after 4:30.

.PARAMETER T60
  Only run when the slate's earliest tip is 50–70 minutes away.

.PARAMETER SkipMeasure
  Skip the measure-only effects report.

.PARAMETER SkipNtfy
  Do not push status-change alerts.
#>
param(
    [string]$Date = "",
    [string]$Window = "",
    [switch]$T60,
    [switch]$SkipMeasure,
    [switch]$SkipNtfy
)

$ErrorActionPreference = "Continue"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root

function Get-TodayEt {
    try {
        return [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId(
            (Get-Date), "Eastern Standard Time"
        ).ToString("yyyy-MM-dd")
    } catch {
        return (Get-Date).ToString("yyyy-MM-dd")
    }
}

if (-not $Date) { $Date = Get-TodayEt }

if ($T60) {
    $gate = & py -3.14 (Join-Path $Root "scripts\injury_t60_gate.py") --date $Date
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[injuries] T-60 gate: not in window — skip ($gate)" -ForegroundColor DarkGray
        exit 0
    }
    if (-not $Window) { $Window = "T60" }
    Write-Host "[injuries] T-60 gate open: $gate" -ForegroundColor Cyan
}

Write-Host "[injuries] Collect $Date (window=$Window)..." -ForegroundColor Cyan
& py -3.14 (Join-Path $Root "scripts\collect_injuries_daily.py") --date $Date --root $Root
$collectExit = $LASTEXITCODE

Write-Host "[injuries] Attach board..." -ForegroundColor Cyan
& py -3.14 (Join-Path $Root "scripts\attach_injury_board.py") --date $Date --root $Root

if (-not $SkipMeasure) {
    Write-Host "[injuries] Measure effects (observe)..." -ForegroundColor Cyan
    & py -3.14 (Join-Path $Root "scripts\measure_injury_effects.py") --date $Date --root $Root
}

$reportArgs = @("-3.14", (Join-Path $Root "scripts\report_injuries_daily.py"), "--date", $Date, "--root", $Root)
if ($Window) { $reportArgs += @("--window", $Window) }
$winU = "$Window".ToUpper().Replace(":", "")
if (-not $SkipNtfy -and ($winU -in @("430PM", "T60", "T-60") -or $T60)) {
    $reportArgs += "--ntfy"
}
& py @reportArgs

if ($collectExit -ne 0) { exit $collectExit }
exit 0
