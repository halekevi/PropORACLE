#requires -Version 5.1
<#
.SYNOPSIS
  Patch PropOracle scheduled tasks to -WindowStyle Hidden so consoles do not
  pop on the gaming display. Keeps Interactive logon (CDP still works).

  Run elevated once:
    pwsh -File scripts\Apply_Hidden_Scheduled_Windows.ps1
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

Get-ScheduledTask | Where-Object { $_.TaskName -like "PropOracle*" -or $_.TaskName -like "PropORACLE*" } | ForEach-Object {
    $task = $_
    $action = $task.Actions | Select-Object -First 1
    if (-not $action) { return }

    $exe = [string]$action.Execute
    $args = [string]$action.Arguments
    $wd = if ($action.WorkingDirectory) { $action.WorkingDirectory } else { $root }

    if ($exe -match "(^|\\)py\.exe$") {
        $newExe = $pwsh
        $newArgs = "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -Command `"Set-Location -LiteralPath '$root'; & '$exe' $args; exit `$LASTEXITCODE`""
    } elseif ($exe -match "pwsh|powershell") {
        $newExe = $exe
        if ($args -match "-WindowStyle\s+Hidden") {
            Write-Host "OK already hidden: $($task.TaskName)" -ForegroundColor DarkGray
            return
        }
        $newArgs = "-WindowStyle Hidden " + ($args -replace "^\s+", "")
    } else {
        Write-Host "SKIP $($task.TaskName) ($exe)" -ForegroundColor Yellow
        return
    }

    if ($task.State -eq "Running") {
        Write-Host "DEFER (running): $($task.TaskName)" -ForegroundColor Yellow
        return
    }

    $newAction = New-ScheduledTaskAction -Execute $newExe -Argument $newArgs -WorkingDirectory $wd
    Set-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -Action $newAction | Out-Null
    Write-Host "HIDDEN: $($task.TaskName)" -ForegroundColor Green
}

Write-Host "`nDone. Next fires will not open a visible console." -ForegroundColor Cyan
Start-Sleep -Seconds 3
