#requires -Version 5.1
<#
.SYNOPSIS
  Lightweight Goblin-70 keep scrub + rebuild-freshness alert (no full rebuild).

.NOTES
  Re-checks published G70 legs against today's board keep gates and drops fails.
  Then asserts LAST_G70_REBUILD.json is within -MaxHours (default 6).

  Registered as "PropOracle - G70 Keep Scrub" every 30 minutes.
#>
param(
    [string]$Date = "",
    [double]$MaxHours = 6,
    [switch]$PublishLive,
    [switch]$SkipPublish
)

$ErrorActionPreference = "Continue"
try { $Host.UI.RawUI.WindowTitle = "PropOracle - G70 Keep Scrub" } catch { }
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root

$LogsDir = Join-Path $Root "logs"
if (-not (Test-Path -LiteralPath $LogsDir)) {
    New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
}
$stamp = Join-Path $LogsDir ("task_g70_keep_scrub_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))
try { Start-Transcript -Path $stamp -Append | Out-Null } catch { }

$scrub = Join-Path $Root "scripts\scrub_goblin70_keep_live.py"
$assert = Join-Path $Root "scripts\assert_g70_freshness.py"
if (-not (Test-Path -LiteralPath $scrub)) {
    Write-Host "[G70 SCRUB] missing scrub_goblin70_keep_live.py" -ForegroundColor Red
    try { Stop-Transcript | Out-Null } catch { }
    exit 1
}

$scrubArgs = @("--write")
if ($Date) { $scrubArgs += @("--date", $Date) }
# Publish when drops happen so Railway clears keep-fails without waiting for full rebuild.
if ($PublishLive -or -not $SkipPublish) {
    $scrubArgs += "--publish-live"
}

Write-Host ("[G70 SCRUB] py scrub_goblin70_keep_live.py {0}" -f ($scrubArgs -join " ")) -ForegroundColor Cyan
& py -3.14 -X utf8 $scrub @scrubArgs
$scrubExit = $LASTEXITCODE
if ($scrubExit -ne 0) {
    Write-Host "[G70 SCRUB] scrub exit $scrubExit" -ForegroundColor Yellow
}

$assertExit = 0
if (Test-Path -LiteralPath $assert) {
    Write-Host ("[G70 SCRUB] assert freshness max_hours={0}" -f $MaxHours) -ForegroundColor Cyan
    & py -3.14 -X utf8 $assert --max-hours $MaxHours
    $assertExit = $LASTEXITCODE
    if ($assertExit -eq 2) {
        Write-Host "[G70 SCRUB] ALERT: G70 rebuild stale — see logs/LAST_G70_STALE.alert" -ForegroundColor Red
    }
}

try { Stop-Transcript | Out-Null } catch { }
# Prefer scrub failure; else surface freshness alert to Task Scheduler.
if ($scrubExit -ne 0) { exit $scrubExit }
exit $assertExit
