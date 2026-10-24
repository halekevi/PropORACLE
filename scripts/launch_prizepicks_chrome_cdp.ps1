#requires -Version 5.1
<#
.SYNOPSIS
  Launch Google Chrome with remote debugging for PrizePicks (DataDome bypass).

.DESCRIPTION
  Opens Chrome on port 9222 with the PropOracle PrizePicks profile (~/.pp_browser_profile).
  If CDP is already up, REUSES an existing PrizePicks tab (navigates it) instead of
  opening another tab — scheduled runs used to spam a new tab on every -OpenBoard.

  Log in at app.prizepicks.com, complete any "Press & Hold" challenge, then run pipelines with:
    -Cdp http://127.0.0.1:9222
  or set PROPORACLE_PP_CDP / PRIZEPICKS_CDP to that URL.

.EXAMPLE
  pwsh -NoProfile -File scripts\launch_prizepicks_chrome_cdp.ps1
  pwsh -NoProfile -File scripts\launch_prizepicks_chrome_cdp.ps1 -OpenBoard -LeagueId 3
#>
param(
    [int]$Port = 9222,
    [string]$UserDataDir = "",
    [switch]$OpenBoard,
    [string]$LeagueId = "3",
    # Keep duplicate PrizePicks tabs (default: close extras when reusing CDP).
    [switch]$KeepExtraTabs
)

$ErrorActionPreference = "Stop"

$chromeCandidates = @(
    "${env:ProgramFiles}\Google\Chrome\Application\chrome.exe",
    "${env:ProgramFiles(x86)}\Google\Chrome\Application\chrome.exe",
    "$env:LOCALAPPDATA\Google\Chrome\Application\chrome.exe"
)
$chrome = $chromeCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $chrome) {
    Write-Host "Chrome not found. Install Chrome or pass a full path via -ChromeExe (not implemented)." -ForegroundColor Red
    exit 1
}

$profile = $UserDataDir.Trim()
if (-not $profile) {
    $profile = Join-Path $env:USERPROFILE ".pp_browser_profile"
}

$cdpUrl = "http://127.0.0.1:$Port"
$boardUrl = if ($OpenBoard) {
    "https://app.prizepicks.com/board?league_id=$LeagueId"
} else {
    "https://app.prizepicks.com/"
}

function Test-CdpUp {
    param([string]$Base)
    try {
        $null = Invoke-RestMethod -Uri "$Base/json/version" -TimeoutSec 2
        return $true
    } catch {
        return $false
    }
}

function Send-CdpNavigate {
    param(
        [Parameter(Mandatory = $true)][string]$WebSocketUrl,
        [Parameter(Mandatory = $true)][string]$Url
    )
    # Prefer Python (always present in this repo) over raw WS in Windows PowerShell 5.1.
    $py = Get-Command py -ErrorAction SilentlyContinue
    if (-not $py) { throw "py launcher not found; cannot navigate existing tab" }
    $code = @'
import json, sys
try:
    import websocket
except ImportError:
    sys.exit(2)
ws_url, nav_url = sys.argv[1], sys.argv[2]
ws = websocket.create_connection(ws_url, timeout=8)
try:
    ws.send(json.dumps({"id": 1, "method": "Page.navigate", "params": {"url": nav_url}}))
    # Drain one reply (ignore payload)
    ws.recv()
finally:
    ws.close()
'@
    $tmp = [System.IO.Path]::GetTempFileName() + ".py"
    try {
        Set-Content -LiteralPath $tmp -Value $code -Encoding utf8
        & py -3.14 $tmp $WebSocketUrl $Url
        if ($LASTEXITCODE -eq 2) {
            # fallback: urllib-free websocket via playwright not required — try node-less CDP HTTP activate only
            throw "websocket-client not installed"
        }
        if ($LASTEXITCODE -ne 0) { throw "Page.navigate failed (exit $LASTEXITCODE)" }
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

function Use-ExistingCdpSession {
    param(
        [string]$Base,
        [string]$TargetUrl,
        [bool]$CloseExtras
    )
    $raw = Invoke-RestMethod -Uri "$Base/json" -TimeoutSec 5
    # Real app tabs only — skip blob:, extensions, iframes, fingerprint hosts.
    $pages = @(
        @($raw) | Where-Object {
            $_.type -eq "page" -and
            $_.webSocketDebuggerUrl -and
            ($_.url -like "https://app.prizepicks.com/*")
        }
    )
    $keep = @($pages | Where-Object { $_.url -match "/board" } | Select-Object -First 1)[0]
    if (-not $keep) {
        $keep = @($pages | Select-Object -First 1)[0]
    }

    if (-not $keep) {
        Write-Host "[PP Chrome] CDP up — opening one PrizePicks tab (none found)" -ForegroundColor Cyan
        $encoded = [uri]::EscapeDataString($TargetUrl)
        $null = Invoke-RestMethod -Uri "$Base/json/new?$encoded" -TimeoutSec 8
        return $true
    }

    Write-Host "[PP Chrome] CDP already up — reusing tab: $($keep.url)" -ForegroundColor Green
    try {
        $null = Invoke-WebRequest -Uri "$Base/json/activate/$($keep.id)" -Method PUT -TimeoutSec 3 -UseBasicParsing
    } catch {
        try { $null = Invoke-RestMethod -Uri "$Base/json/activate/$($keep.id)" -TimeoutSec 3 } catch { }
    }

    $wantBoard = $TargetUrl -match "/board"
    $onBoard = $keep.url -match "/board"
    # SPA often strips ?league_id= after load — do not re-navigate just for that.
    if ($wantBoard -and -not $onBoard) {
        try {
            $ws = [string]$keep.webSocketDebuggerUrl
            Send-CdpNavigate -WebSocketUrl $ws -Url $TargetUrl
            Write-Host "[PP Chrome] Navigated existing tab -> $TargetUrl" -ForegroundColor DarkGray
        } catch {
            Write-Host "[PP Chrome] WARN: could not navigate existing tab ($($_.Exception.Message)) — leaving it as-is" -ForegroundColor Yellow
        }
    }

    if ($CloseExtras -and $pages.Count -gt 1) {
        $closed = 0
        $keepId = [string]$keep.id
        # Only collapse duplicate /board tabs — leave lineups / account pages alone.
        $boardDupes = @($pages | Where-Object {
                ([string]$_.id -ne $keepId) -and ($_.url -match "/board")
            })
        foreach ($tab in $boardDupes) {
            try {
                $null = Invoke-RestMethod -Uri "$Base/json/close/$($tab.id)" -TimeoutSec 3
                $closed++
            } catch { }
        }
        if ($closed -gt 0) {
            Write-Host "[PP Chrome] Closed $closed duplicate board tab(s)" -ForegroundColor DarkGray
        }
    }
    return $true
}

# --- main -------------------------------------------------------------------
if (Test-CdpUp -Base $cdpUrl) {
    $null = Use-ExistingCdpSession -Base $cdpUrl -TargetUrl $boardUrl -CloseExtras:(-not $KeepExtraTabs)
    Write-Host "[PP Chrome] CDP ready (reused): $cdpUrl" -ForegroundColor Green
    Write-Host ""
    Write-Host "WNBA board: league_id=3  |  WNBA1H=193  |  WNBA1Q=308  |  NBA board: league_id=7  |  NFL=9  |  NFLP=44 | MLB=2" -ForegroundColor DarkGray
    exit 0
}

# Profile Chrome alive but CDP dead: another Start-Process only opens a new tab
# in that instance and never binds 9222. Kill it, then do a real debug launch.
$stale = @(Get-CimInstance Win32_Process -Filter "Name='chrome.exe'" -ErrorAction SilentlyContinue |
    Where-Object {
        $_.CommandLine -and (
            $_.CommandLine -match 'remote-debugging-port=9222' -or
            $_.CommandLine -match '\.pp_browser_profile' -or
            $_.CommandLine -match [regex]::Escape($profile)
        )
    })
if ($stale.Count -gt 0) {
    Write-Host "[PP Chrome] Profile Chrome running without CDP — restarting (avoids tab spam)" -ForegroundColor Yellow
    foreach ($proc in $stale) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 2
}

$args = @(
    "--remote-debugging-port=$Port",
    # Required for Playwright / websocket-client attach from this machine.
    # Without it, Chrome returns HTTP 403 on DevTools WebSocket handshake and
    # step1 CDP hangs/fails (tickets stay empty on 403 days).
    "--remote-allow-origins=*",
    "--user-data-dir=$profile",
    "--no-first-run",
    "--no-default-browser-check",
    $boardUrl
)

Write-Host "[PP Chrome] Launching debug Chrome on port $Port" -ForegroundColor Cyan
Write-Host "  Profile: $profile" -ForegroundColor DarkGray
Write-Host "  After login / human check, set: `$env:PROPORACLE_PP_CDP = '$cdpUrl'" -ForegroundColor DarkGray
Write-Host ""

Start-Process -FilePath $chrome -ArgumentList $args

Start-Sleep -Seconds 2
try {
    $ver = Invoke-RestMethod -Uri "$cdpUrl/json/version" -TimeoutSec 8
    Write-Host "[PP Chrome] CDP ready: $($ver.Browser)" -ForegroundColor Green
} catch {
    Write-Host "[PP Chrome] Started Chrome; CDP not responding yet — wait a few seconds." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "WNBA board: league_id=3  |  WNBA1H=193  |  WNBA1Q=308  |  NBA board: league_id=7  |  NFL=9  |  NFLP=44 | MLB=2" -ForegroundColor DarkGray
Write-Host "Run pipeline: pwsh -File scripts\run_wnba_pipeline.ps1 -Cdp $cdpUrl -Date YYYY-MM-DD" -ForegroundColor DarkGray
