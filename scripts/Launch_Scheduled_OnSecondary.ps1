#requires -Version 5.1
<#
.SYNOPSIS
  Scheduled-task entry: show a visible console on a NON-primary monitor, then
  run the real PropOracle script (or python).

.EXAMPLE
  pwsh -File scripts\Launch_Scheduled_OnSecondary.ps1 -File scripts\run_daily_1am.ps1
  pwsh -File scripts\Launch_Scheduled_OnSecondary.ps1 -PyArgs "-3.14 -X utf8 scripts\check_live_tickets_fresh.py"
#>
param(
    [string]$File = "",
    [string]$ExtraArgs = "",
    [string]$PyArgs = "",
    [string]$WindowTitle = "PropOracle"
)

$ErrorActionPreference = "Continue"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root

try { $Host.UI.RawUI.WindowTitle = $WindowTitle } catch { }

$move = Join-Path $PSScriptRoot "_move_console_off_primary.ps1"
if (Test-Path -LiteralPath $move) {
    . $move
    # Give the console HWND a beat to exist, then park it off the gaming display.
    Start-Sleep -Milliseconds 200
    Move-ConsoleOffPrimaryMonitor
    Start-Sleep -Milliseconds 100
    Move-ConsoleOffPrimaryMonitor
}

if ($PyArgs -and $PyArgs.Trim()) {
    $argList = @($PyArgs.Trim() -split '\s+')
    & py @argList
    exit $LASTEXITCODE
}

if (-not $File) {
    Write-Error "Pass -File <script.ps1> or -PyArgs '...'"
    exit 1
}
$scriptPath = $File
if (-not [System.IO.Path]::IsPathRooted($scriptPath)) {
    $scriptPath = Join-Path $Root $scriptPath
}
if (-not (Test-Path -LiteralPath $scriptPath)) {
    Write-Error "Missing script: $scriptPath"
    exit 1
}

$extra = $ExtraArgs.Trim()
if ($extra) {
    # Split on spaces but keep simple quoted tokens if ever needed later.
    $extraParts = [System.Management.Automation.PSParser]::Tokenize($extra, [ref]$null) |
        Where-Object { $_.Type -eq 'CommandArgument' -or $_.Type -eq 'String' -or $_.Type -eq 'Number' } |
        ForEach-Object { $_.Content }
    if (-not $extraParts) { $extraParts = $extra -split '\s+' }
    & $scriptPath @extraParts
} else {
    & $scriptPath
}
exit $LASTEXITCODE
