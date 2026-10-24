# Day-ahead PrizePicks Standard UNDER capture (no Combined).
# Fetches WNBA today+tomorrow ET boards and writes the watchlist JSON.
#
#   pwsh -File scripts/run_day_ahead_unders.ps1
#   pwsh -File scripts/run_day_ahead_unders.ps1 -Date 2026-08-16
#
# Output: data/reports/day_ahead_standard_unders_<date>.json
#         data/reports/day_ahead_standard_unders_latest.json
param(
    [string]$Date = "",
    [string]$Cdp = ""
)

$ErrorActionPreference = "Continue"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "run_pipeline.ps1"))) {
    $Root = $PSScriptRoot
    if ((Split-Path -Leaf $Root) -eq "scripts") {
        $Root = Split-Path -Parent $Root
    }
}

$wnbaPs1 = Join-Path $Root "scripts\run_wnba_pipeline.ps1"
if (-not (Test-Path -LiteralPath $wnbaPs1)) {
    Write-Host "Missing $wnbaPs1" -ForegroundColor Red
    exit 1
}

$invoke = @{
    DayAhead = $true
    Step1Only = $true
}
if ($Date) { $invoke["Date"] = $Date }
if ($Cdp) { $invoke["Cdp"] = $Cdp }

Write-Host "Day-ahead Standard UNDER fetch (WNBA step1 + watchlist)" -ForegroundColor Cyan
& $wnbaPs1 @invoke
exit $LASTEXITCODE
