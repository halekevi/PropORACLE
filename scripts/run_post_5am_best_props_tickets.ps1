#requires -Version 5.1
<#
.SYNOPSIS
  After Daily 5AM finishes: rank best props + rebuild Goblin-70 dual card and publish live.
  Target: tickets ready before 7:00 AM ET.
#>
param(
    [string]$Date = "",
    [string]$RepoRoot = "",
    [datetime]$DeadlineLocal = [datetime]::MinValue,
    [switch]$SkipWait,
    [switch]$SkipPublish
)

$ErrorActionPreference = "Continue"
try { $Host.UI.RawUI.WindowTitle = "PropOracle - Post-5AM Best Props Tickets" } catch { }

if (-not $RepoRoot) {
    $RepoRoot = Split-Path $PSScriptRoot -Parent
    $mainCp = Join-Path (Split-Path $RepoRoot -Parent) "PropORACLE_main_cp"
    if (Test-Path (Join-Path $mainCp "scripts\build_goblin70_tickets.py")) {
        $RepoRoot = $mainCp
    }
}
if (-not $Date) { $Date = (Get-Date).ToString("yyyy-MM-dd") }
if ($DeadlineLocal -eq [datetime]::MinValue) {
    $DeadlineLocal = [datetime]::ParseExact("$Date 06:50:00", "yyyy-MM-dd HH:mm:ss", $null)
}

$LogsDir = Join-Path $RepoRoot "logs"
if (-not (Test-Path $LogsDir)) { New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null }
$RunLog = Join-Path $LogsDir ("post_5am_best_props_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))
try { Start-Transcript -Path $RunLog -Append | Out-Null } catch { }

function Write-Step([string]$msg) {
    Write-Host "[POST-5AM] $msg" -ForegroundColor Cyan
}

function Test-5AmFinished {
    param([string]$Root, [string]$Day)
    $logs = @(Get-ChildItem (Join-Path $Root "logs") -Filter "task_5am_${Day}_*.log" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending)
    if (-not $logs) { return $false }
    $latest = $logs[0]
    # Still growing / wrapper still open → not done
    $ageSec = ((Get-Date) - $latest.LastWriteTime).TotalSeconds
    if ($ageSec -lt 45) { return $false }
    $tail = Get-Content -LiteralPath $latest.FullName -Tail 80 -ErrorAction SilentlyContinue | Out-String
    if ($tail -match 'FAILED|exit 128|Source conflict') {
        Write-Step "5AM log shows failure markers — proceeding anyway with local board"
        return $true
    }
    # Healthy completion signals from wrapper / health stamp
    if ($tail -match 'Writing health stamp|5AM DAILY\] Snapshot|POST-5AM|Invoke-LoggedPwsh.*exit|healthExit') {
        return $true
    }
    # Child daily log finished
    $child = @(Get-ChildItem (Join-Path $Root "logs") -Filter "run_daily_child_5am_${Day}_*.log" -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending)
    if ($child) {
        $ct = Get-Content -LiteralPath $child[0].FullName -Tail 40 -ErrorAction SilentlyContinue | Out-String
        if ($ct -match 'STEP E|Goblin-70 dual card: OK|Publish|DONE|finished') { return $true }
    }
    # Task no longer running and log stale > 3 min after 5:20
    $task = Get-ScheduledTask -TaskName "PropOracle - Daily 5AM" -ErrorAction SilentlyContinue
    if ($task -and $task.State -ne "Running" -and (Get-Date).Hour -ge 5 -and $ageSec -gt 180) {
        return $true
    }
    return $false
}

Write-Step "Repo=$RepoRoot Date=$Date Deadline=$DeadlineLocal SkipWait=$SkipWait"

if (-not $SkipWait) {
    Write-Step "Waiting for Daily 5AM to finish (deadline $DeadlineLocal)..."
    while ((Get-Date) -lt $DeadlineLocal) {
        if (Test-5AmFinished -Root $RepoRoot -Day $Date) {
            Write-Step "5AM finished — starting best-props + tickets"
            break
        }
        # Also wait until at least 5:05 so we don't race a late-starting task
        if ((Get-Date) -lt [datetime]::ParseExact("$Date 05:05:00", "yyyy-MM-dd HH:mm:ss", $null)) {
            Start-Sleep -Seconds 30
            continue
        }
        Start-Sleep -Seconds 45
    }
    if (-not (Test-5AmFinished -Root $RepoRoot -Day $Date)) {
        if ((Get-Date) -ge $DeadlineLocal) {
            Write-Step "Deadline hit without clean 5AM signal — ranking/publishing from current board"
        }
    }
}

$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUTF8 = "1"
Set-Location $RepoRoot

$xlsx = Join-Path $RepoRoot "outputs\$Date\best_props_$Date.xlsx"
New-Item -ItemType Directory -Path (Split-Path $xlsx) -Force | Out-Null

Write-Step "Ranking best props → $xlsx"
& py -3.14 -u -X utf8 (Join-Path $RepoRoot "scripts\rank_best_props_today.py") `
    --date $Date --step8-root $RepoRoot --xlsx $xlsx 2>&1 |
    Tee-Object -FilePath (Join-Path $LogsDir "best_props_${Date}_post5am.log")
$rankExit = $LASTEXITCODE
Write-Step "rank_best_props exit=$rankExit"

$g70 = Join-Path $RepoRoot "scripts\build_goblin70_tickets.py"
$g70Args = @("-3.14", "-u", "-X", "utf8", $g70, "--date", $Date, "--write-web")
if (-not $SkipPublish) { $g70Args += "--publish-live" }
if ($SkipPublish) { Write-Step "Goblin-70 dual card (no publish)" } else { Write-Step "Goblin-70 dual card + publish-live" }
& py @g70Args 2>&1 | Tee-Object -FilePath (Join-Path $LogsDir "g70_post5am_${Date}.log")
$g70Exit = $LASTEXITCODE
Write-Step "build_goblin70 exit=$g70Exit"

# Wake Cursor agent for PP tab / best-prop slip follow-up before 7am
Write-Host ('AGENT_LOOP_TICK_7am_tickets {"prompt":"5AM done. Best props + G70 published for ' + $Date + '. Rank exit=' + $rankExit + ' G70 exit=' + $g70Exit + '. Load PrizePicks Flex/Power tabs from the best L5=5 clears (WNBA/MLB/Soccer/Tennis + CFB). Exclude faces already on open slips. Ready by 7am ET. No submit."}') -ForegroundColor Green

try { Stop-Transcript | Out-Null } catch { }
if ($rankExit -ne 0 -or $g70Exit -ne 0) { exit 2 }
exit 0
