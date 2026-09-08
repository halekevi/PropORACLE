#requires -Version 5.1
<#
.SYNOPSIS
  Kick off overnight MLB board discovery (11PM / midnight) in a visible window.
#>
param(
    [ValidateSet("11PM", "12AM")]
    [string]$Window = "11PM"
)

$ErrorActionPreference = "Continue"
$Root = Split-Path $PSScriptRoot -Parent
$Wrapper = Join-Path $PSScriptRoot "run_mlb_day_ahead_fill.ps1"

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

Write-Host "Launching visible window: $Wrapper -Window $Window" -ForegroundColor Cyan
Start-Process -FilePath $pwsh `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper, "-Window", $Window) `
    -WorkingDirectory $Root `
    -WindowStyle Normal

Write-Host "Started. Soft-OK if MLB not posted yet; rebuilds tickets when the board appears." -ForegroundColor Green
