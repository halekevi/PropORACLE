#requires -Version 5.1
<#
.SYNOPSIS
  Kick off the 9PM day-ahead initial fetch in a visible window.
#>
param()

$ErrorActionPreference = "Continue"
$Root = Split-Path $PSScriptRoot -Parent
$Wrapper = Join-Path $PSScriptRoot "run_daily_day_ahead.ps1"

if (-not (Test-Path -LiteralPath $Wrapper)) {
    Write-Error "Missing wrapper: $Wrapper"
    exit 1
}

$pwsh = $null
foreach ($cand in @(
    (Get-Command pwsh.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source),
    "$env:ProgramFiles\PowerShell\7\pwsh.exe",
    (Get-Command powershell.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)
)) {
    if ($cand -and (Test-Path -LiteralPath $cand)) { $pwsh = $cand; break }
}
if (-not $pwsh) {
    Write-Error "No pwsh.exe/powershell.exe found"
    exit 1
}

Write-Host "Launching visible window: $Wrapper" -ForegroundColor Cyan
Start-Process -FilePath $pwsh `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper) `
    -WorkingDirectory $Root `
    -WindowStyle Normal

Write-Host "Started. Tomorrow's slate is scraped and published to live /tickets." -ForegroundColor Green
