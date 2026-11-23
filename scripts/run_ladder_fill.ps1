#requires -Version 5.1
<#
  Nightly payout ladder fill (scheduled: "PropOracle - Ladder Fill").

  Probes PrizePicks Power N-correct payouts for S/G/D mixes the ladder log is missing
  (scripts/ladder_fill_capture.py). Probe slips are never submitted. Skips and logs
  logs/LAST_LADDER_FILL.json when Chrome CDP (9222) is down or PrizePicks is logged out.
  Keep -MaxSlips low (cap 25): a flagged session stops every CDP capture.

    .\scripts\run_ladder_fill.ps1
    .\scripts\run_ladder_fill.ps1 -DryRun
#>
param(
    [int]$MaxSlips = 25,
    [string]$CdpUrl = "http://127.0.0.1:9222",
    [switch]$DryRun
)

$ErrorActionPreference = "Continue"
$Root = Split-Path $PSScriptRoot -Parent
Set-Location $Root
$LogsDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Path $LogsDir -Force | Out-Null
try { Start-Transcript -Path (Join-Path $LogsDir ("task_ladder_fill_{0:yyyy-MM-dd_HHmmss}.log" -f (Get-Date))) -Append | Out-Null } catch { }

$pyArgs = @("-3.14", "-X", "utf8", (Join-Path $Root "scripts\ladder_fill_capture.py"), "--max-slips", $MaxSlips, "--cdp-url", $CdpUrl)
if ($DryRun) { $pyArgs += "--dry-run" }
& py @pyArgs
$rc = $LASTEXITCODE
Write-Host "[LADDER-FILL] capture exit $rc"

$rel = "ui_runner/data/payout_ladder_live_cdp.json"
$branch = (git rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
if (-not $DryRun -and $branch -eq "main") {
    git diff --quiet -- $rel
    if ($LASTEXITCODE -ne 0) {
        $env:GIT_TERMINAL_PROMPT = "0"
        git add -- $rel
        git commit -m ("data: payout ladder fill probes {0:yyyy-MM-dd}" -f (Get-Date)) -- $rel | Out-Null
        git pull --rebase --autostash 2>&1 | ForEach-Object { Write-Host "    $_" }
        git push 2>&1 | ForEach-Object { Write-Host "    $_" }
        if ($LASTEXITCODE -eq 0) { Write-Host "[LADDER-FILL] ladder log pushed" -ForegroundColor Green }
        else { Write-Host "[LADDER-FILL] WARN: push failed (exit $LASTEXITCODE)" -ForegroundColor Yellow }
    }
    else {
        Write-Host "[LADDER-FILL] no ladder changes to push"
    }
}
elseif (-not $DryRun) {
    Write-Host "[LADDER-FILL] on '$branch' — not pushing (Railway reads main)" -ForegroundColor Yellow
}

try { Stop-Transcript | Out-Null } catch { }
exit $rc
