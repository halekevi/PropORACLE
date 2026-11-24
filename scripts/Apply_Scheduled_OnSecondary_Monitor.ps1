#requires -Version 5.1
<#
.SYNOPSIS
  Point PropOracle scheduled tasks at Launch_Scheduled_OnSecondary.ps1 so
  consoles stay visible but open off the primary/gaming monitor.

  Run elevated (UAC):
    pwsh -File scripts\Apply_Scheduled_OnSecondary_Monitor.ps1
#>
$ErrorActionPreference = "Stop"

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).
    IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "Re-launching elevated..." -ForegroundColor Yellow
    $self = $MyInvocation.MyCommand.Path
    Start-Process -FilePath (Get-Command pwsh.exe).Source `
        -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$self`"") `
        -Verb RunAs | Out-Null
    exit 0
}

$pwsh = (Get-Command pwsh.exe -ErrorAction SilentlyContinue).Source
if (-not $pwsh) { $pwsh = "$env:ProgramFiles\PowerShell\7\pwsh.exe" }
$root = Split-Path $PSScriptRoot -Parent
$launcher = Join-Path $root "scripts\Launch_Scheduled_OnSecondary.ps1"
if (-not (Test-Path -LiteralPath $launcher)) {
    throw "Missing launcher: $launcher"
}

# Map task name -> (script relative path, extra args, or py args)
$map = @{
    "PropOracle - Daily 1AM"        = @{ File = "scripts\run_daily_1am.ps1" }
    "PropOracle - Daily 5AM"        = @{ File = "scripts\run_daily_5am.ps1" }
    "PropOracle - Daily 8AM"        = @{ File = "scripts\run_daily_8am.ps1" }
    "PropOracle - DayAhead 9PM"     = @{ File = "scripts\run_daily_day_ahead.ps1" }
    "PropOracle - G70 Keep Scrub"   = @{ File = "scripts\run_g70_keep_scrub.ps1" }
    "PropOracle - Grader 3AM"       = @{ File = "scripts\run_grader_evening.ps1" }
    "PropOracle - Live Freshness"   = @{ PyArgs = "-3.14 -X utf8 scripts\check_live_tickets_fresh.py" }
    "PropOracle - Refresh 1030AM"   = @{ File = "scripts\run_refresh_with_log.ps1"; Extra = "-RunLabel 1030AM" }
    "PropOracle - Refresh 1PM"      = @{ File = "scripts\run_refresh_with_log.ps1"; Extra = "-RunLabel 1PM" }
    "PropOracle - Refresh 430PM"    = @{ File = "scripts\run_refresh_with_log.ps1"; Extra = "-RunLabel 430PM" }
    "PropOracle - Refresh 945AM"    = @{ File = "scripts\run_refresh_with_log.ps1"; Extra = "-RunLabel 945AM" }
    "PropOracle - Refresh 9AM"      = @{ File = "scripts\run_refresh_with_log.ps1"; Extra = "-RunLabel 9AM" }
}

Get-ScheduledTask | Where-Object { $_.TaskName -like "PropOracle*" } | ForEach-Object {
    $task = $_
    $cfg = $map[$task.TaskName]
    if (-not $cfg) {
        Write-Host "SKIP (no map): $($task.TaskName)" -ForegroundColor Yellow
        return
    }
    if ($task.State -eq "Running") {
        Write-Host "DEFER (running): $($task.TaskName)" -ForegroundColor Yellow
        return
    }

    $title = $task.TaskName
    if ($cfg.PyArgs) {
        $args = "-NoProfile -ExecutionPolicy Bypass -File `"$launcher`" -PyArgs `"$($cfg.PyArgs)`" -WindowTitle `"$title`""
    } else {
        $scriptAbs = Join-Path $root $cfg.File
        $args = "-NoProfile -ExecutionPolicy Bypass -File `"$launcher`" -File `"$scriptAbs`" -WindowTitle `"$title`""
        if ($cfg.Extra) {
            $args += " -ExtraArgs `"$($cfg.Extra)`""
        }
    }

    $wd = $root
    $newAction = New-ScheduledTaskAction -Execute $pwsh -Argument $args -WorkingDirectory $wd
    Set-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -Action $newAction | Out-Null
    Write-Host "SECONDARY: $($task.TaskName)" -ForegroundColor Green
}

Write-Host "`nDone. Consoles stay visible but move off the primary/gaming display." -ForegroundColor Cyan
Start-Sleep -Seconds 2
