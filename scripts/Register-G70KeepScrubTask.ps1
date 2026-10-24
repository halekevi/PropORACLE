#requires -Version 5.1
# One-shot: register PropOracle - G70 Keep Scrub against PropORACLE_main_cp.
$PipelineRoot = "H:\PropORACLE_main_cp"
if (-not (Test-Path -LiteralPath (Join-Path $PipelineRoot "scripts\run_g70_keep_scrub.ps1"))) {
    Write-Error "Missing run_g70_keep_scrub.ps1 under $PipelineRoot"
    exit 1
}
$PowerShellExe = $null
foreach ($cand in @(
    (Get-Command pwsh.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source),
    "$env:ProgramFiles\PowerShell\7\pwsh.exe",
    (Get-Command powershell.exe -ErrorAction SilentlyContinue | Select-Object -ExpandProperty Source)
)) {
    if ($cand -and (Test-Path -LiteralPath $cand)) { $PowerShellExe = $cand; break }
}
if (-not $PowerShellExe) { Write-Error "No pwsh/powershell"; exit 1 }

$TaskName = "PropOracle - G70 Keep Scrub"
$ScriptPath = Join-Path $PipelineRoot "scripts\run_g70_keep_scrub.ps1"
$psArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""
$action = New-ScheduledTaskAction -Execute $PowerShellExe -Argument $psArgs -WorkingDirectory $PipelineRoot
$trigger = New-ScheduledTaskTrigger -Once -At "08:00" -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Hours 16)
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 20) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 15) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask `
    -TaskName $TaskName `
    -Description "Every 30m: re-check published Goblin-70 legs against current keep gates; alert if LAST_G70_REBUILD older than 6h." `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null

$st = Get-ScheduledTask -TaskName $TaskName
$info = Get-ScheduledTaskInfo -TaskName $TaskName
$a = @($st.Actions)[0]
Write-Host ("REGISTERED {0} State={1}" -f $TaskName, $st.State) -ForegroundColor Green
Write-Host ("  WD={0}" -f $a.WorkingDirectory)
Write-Host ("  Args={0}" -f $a.Arguments)
Write-Host ("  Next={0} LastResult={1}" -f $info.NextRunTime, $info.LastTaskResult)
