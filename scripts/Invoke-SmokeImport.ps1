#requires -Version 5.1
<#
.SYNOPSIS
  Run scripts/smoke_import_scheduled.py and exit 2 on failure.

.DESCRIPTION
  Call from run_daily.ps1, run_grader.ps1, run_refresh_with_log.ps1, and
  run_pipeline.ps1 so buried ``from utils.X import y`` failures (role_from_snap)
  abort before fetch/grade. Dot-source or invoke:

    & (Join-Path $Root "scripts\Invoke-SmokeImport.ps1") -Root $Root
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$Root,
    [string]$Label = "smoke-import"
)

$SmokeImport = Join-Path $Root "scripts\smoke_import_scheduled.py"
if (-not (Test-Path -LiteralPath $SmokeImport)) {
    Write-Host "[$Label] WARN: scripts\smoke_import_scheduled.py missing — skipping" -ForegroundColor Yellow
    return
}
Write-Host "[$Label] Checking scheduled entrypoint imports (incl. AST utils scan)..." -ForegroundColor Cyan
& py -3.14 $SmokeImport
if ($LASTEXITCODE -ne 0) {
    Write-Host "[$Label] FAILED — aborting before fetch/grade. Sync utils into this live tree." -ForegroundColor Red
    exit 2
}
