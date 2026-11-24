#requires -Version 5.1
<#
.SYNOPSIS
  Light injury refresh (11 PM / 9 AM ET): collect -> board attach -> Goblin-70 + mixer rebuild -> publish.

.DESCRIPTION
  No PrizePicks fetch and no full pipeline. Uses the step8 boards already on disk for the live
  /tickets slate date (tomorrow after the 9 PM day-ahead, today in the morning).

  1. collect_injuries_daily.py  -> data/injuries/injuries_latest.csv + outputs/<date>/injuries_<sport>_<date>.csv
  2. attach_injury_board.py     -> data/injuries/<date>/board_injury_<date>.csv
  3. build_goblin70_tickets.py --write-web (ticket pools drop OUT players from injuries_latest;
     prior live_cdp quotes are re-applied by the scrape patch)
  4. Publish-LiveSite.ps1

  Waits (up to -LockWaitMin) while a full refresh holds data/cache/refresh.lock, then runs.

.PARAMETER Window
  Label for logs/commit (e.g. 11PM, 9AM).

.PARAMETER Date
  Slate date. Default: date in ui_runner/templates/tickets_latest.json.

.PARAMETER NoPublish
  Rebuild locally only.
#>
param(
    [string]$Window = "",
    [string]$Date = "",
    [switch]$NoPublish,
    [int]$LockGraceSec = 90,
    [int]$LockWaitMin = 45
)

$ErrorActionPreference = "Continue"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root
$Py = "py"
$PyArgs = @("-3.14", "-X", "utf8")

$logDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Force $logDir | Out-Null
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
$log = Join-Path $logDir "injury_refresh_$stamp.log"
function Write-Log([string]$msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "HH:mm:ss"), $msg
    Write-Host $line
    Add-Content -LiteralPath $log -Value $line -Encoding UTF8
}

# 9 AM Refresh / 11 PM MLB Fill start at the same minute; let them take the lock first.
$lockFile = Join-Path $Root "data\cache\refresh.lock"
if ($LockGraceSec -gt 0) { Start-Sleep -Seconds $LockGraceSec }
$deadline = (Get-Date).AddMinutes($LockWaitMin)
while ($true) {
    $held = $false
    if (Test-Path -LiteralPath $lockFile) {
        $line = Get-Content -LiteralPath $lockFile -ErrorAction SilentlyContinue | Select-Object -First 1
        if ("$line" -match 'PID\s+(\d+)' -and (Get-Process -Id ([int]$Matches[1]) -ErrorAction SilentlyContinue)) { $held = $true }
    }
    if (-not $held) { break }
    if ((Get-Date) -ge $deadline) {
        Write-Log "SKIP: refresh.lock still held after $LockWaitMin min ($line)"
        exit 0
    }
    Start-Sleep -Seconds 30
}

if (-not $Date) {
    $tl = Join-Path $Root "ui_runner\templates\tickets_latest.json"
    try { $Date = ([string](Get-Content -LiteralPath $tl -Raw -Encoding UTF8 | ConvertFrom-Json).date).Substring(0, 10) } catch { }
}
if (-not $Date) {
    $Date = [System.TimeZoneInfo]::ConvertTimeBySystemTimeZoneId((Get-Date), "Eastern Standard Time").ToString("yyyy-MM-dd")
}
Write-Log "Injury refresh window=$Window date=$Date"

& $Py @PyArgs (Join-Path $Root "scripts\collect_injuries_daily.py") --date $Date --root $Root 2>&1 |
    Tee-Object -FilePath $log -Append | Out-Host
$collectExit = $LASTEXITCODE
if ($collectExit -eq 2) {
    Write-Log "WARN: injury sidecar missing for an in-season sport (alert sent)"
} elseif ($collectExit -ne 0) {
    Write-Log "FAIL: injury collect exit $collectExit - board/tickets left as-is"
    & $Py @PyArgs -c "import sys; sys.path.insert(0, r'$Root'); from utils.pipeline_alerts import emit_pipeline_alert; emit_pipeline_alert('injury-refresh', 'collect failed (exit $collectExit) for $Date window $Window')"
    exit $collectExit
}

& $Py @PyArgs (Join-Path $Root "scripts\attach_injury_board.py") --date $Date --root $Root 2>&1 |
    Tee-Object -FilePath $log -Append | Out-Host
if ($LASTEXITCODE -ne 0) { Write-Log "WARN: attach_injury_board exit $LASTEXITCODE" }

$tickets = Join-Path $Root "ui_runner\templates\tickets_latest.json"
$sigPy = @'
import hashlib, json, sys
try:
    d = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    print(""); raise SystemExit
legs = []
for g in d.get("groups") or []:
    for t in g.get("tickets") or []:
        legs.append([g.get("group_name") or g.get("name"), sorted(
            f"{l.get('player')}|{l.get('prop_type') or l.get('prop')}|{l.get('pick_type')}|{l.get('line')}|{l.get('direction') or l.get('side')}"
            for l in (t.get("legs") or []))])
print(hashlib.sha1(json.dumps([d.get("date"), legs], sort_keys=True).encode()).hexdigest())
'@
function Get-TicketSig { & $Py @PyArgs -c $sigPy $tickets }
$before = Get-TicketSig
& $Py @PyArgs (Join-Path $Root "scripts\build_goblin70_tickets.py") --date $Date --write-web 2>&1 |
    Tee-Object -FilePath $log -Append | Out-Host
$g70Exit = $LASTEXITCODE
if ($g70Exit -ne 0) {
    Write-Log "FAIL: build_goblin70_tickets exit $g70Exit - not publishing"
    & $Py @PyArgs -c "import sys; sys.path.insert(0, r'$Root'); from utils.pipeline_alerts import emit_pipeline_alert; emit_pipeline_alert('injury-refresh', 'Goblin-70 rebuild failed (exit $g70Exit) for $Date window $Window')"
    exit $g70Exit
}
$after = Get-TicketSig
if ($before -and $before -eq $after) {
    Write-Log "Tickets unchanged after injury refresh - nothing to publish"
    exit 0
}
if ($NoPublish) {
    Write-Log "Tickets changed; -NoPublish set"
    exit 0
}

& (Join-Path $Root "scripts\Publish-LiveSite.ps1") -RepoRoot $Root `
    -CommitMessage "chore: injury refresh $Window $Date (Goblin-70 + mixer)" 2>&1 |
    Tee-Object -FilePath $log -Append | Out-Host
$pubExit = $LASTEXITCODE
Write-Log "Publish exit $pubExit"
exit $pubExit
