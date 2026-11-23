# Soft-fail capture of FanDuel / DraftKings / Underdog ladders for a slate date.
# Archives history + refreshes data/reports/book_line_softness_latest.md
# Never blocks Goblin-70 / publish. No Odds API.
#
# Keep ENABLED on every live window until the FD/DK/UD ladder book is complete.
# Then turn off with either:
#   $env:PROPORACLE_BOOK_LINE_CAPTURE = "0"
#   or create data/cache/book_line_capture_off.flag
#
#   .\scripts\run_book_line_capture.ps1 -Date 2026-11-23
#   .\scripts\run_book_line_capture.ps1 -Date 2026-11-23 -SeedOnly

param(
    [Parameter(Mandatory = $true)][string]$Date,
    [string]$Root = "",
    [string]$Window = "",
    [switch]$SeedOnly,
    [string]$Sports = "nba,nfl",
    [string]$DkLeagues = "nba,nfl"
)

$ErrorActionPreference = "Continue"
if (-not $Root) {
    $Root = Split-Path -Parent $PSScriptRoot
}

$cacheDir = Join-Path $Root "data\cache"
New-Item -ItemType Directory -Force -Path $cacheDir | Out-Null
$offFlag = Join-Path $cacheDir "book_line_capture_off.flag"
$envOff = "$env:PROPORACLE_BOOK_LINE_CAPTURE"
if ((Test-Path -LiteralPath $offFlag) -or ($envOff -eq "0") -or ($envOff -eq "false")) {
    Write-Host "[BOOK-LINES] DISABLED (flag or PROPORACLE_BOOK_LINE_CAPTURE=0) — skip $Date" -ForegroundColor Yellow
    exit 0
}

$booksDir = Join-Path $Root "outputs\$Date\books"
New-Item -ItemType Directory -Force -Path $booksDir | Out-Null
$started = Get-Date
$stampPath = Join-Path $cacheDir "last_book_line_capture.json"

function Invoke-SoftPy {
    param([string]$Label, [string[]]$PyArgs)
    Write-Host "[BOOK-LINES] $Label ..." -ForegroundColor Cyan
    try {
        & py -3.14 -u -X utf8 @PyArgs
        $code = $LASTEXITCODE
        if ($code -ne 0) {
            Write-Host "[BOOK-LINES] WARN: $Label exit $code (non-blocking)" -ForegroundColor Yellow
        }
        return $code
    }
    catch {
        Write-Host "[BOOK-LINES] WARN: $Label failed: $($_.Exception.Message)" -ForegroundColor Yellow
        return 1
    }
}

function Write-CaptureStamp {
    param(
        [string]$Status,
        [hashtable]$Extra
    )
    $payload = [ordered]@{
        date         = $Date
        window       = $Window
        status       = $Status
        seed_only    = [bool]$SeedOnly
        finished_at  = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        elapsed_sec  = [int]((Get-Date) - $started).TotalSeconds
        root         = $Root
        books_dir    = $booksDir
        note         = "Keep daily until FD/DK/UD ladders are complete; then set book_line_capture_off.flag"
    }
    if ($Extra) {
        foreach ($k in $Extra.Keys) { $payload[$k] = $Extra[$k] }
    }
    try {
        ($payload | ConvertTo-Json -Depth 4) | Set-Content -LiteralPath $stampPath -Encoding utf8
    } catch {
        Write-Host "[BOOK-LINES] WARN: could not write stamp: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

$fdOk = $false
$udOk = $false
$dkOk = $false

if (-not $SeedOnly) {
    $fdOut = Join-Path $booksDir "fanduel_props_ladder.csv"
    $c = Invoke-SoftPy "FanDuel fetch" @(
        (Join-Path $Root "scripts\fetch_fanduel_player_props.py"),
        "--sports", $Sports,
        "--output", $fdOut,
        "--slate-date", $Date
    )
    $fdOk = (($c -eq 0) -and (Test-Path -LiteralPath $fdOut))

    $udProps = Join-Path $Root "outputs\$Date\underdog_props.csv"
    $udLadder = Join-Path $booksDir "underdog_props_ladder.csv"
    $c = Invoke-SoftPy "Underdog fetch" @(
        (Join-Path $Root "scripts\fetch_underdog_pickem.py"),
        "--sport", "ALL",
        "--output", $udProps,
        "--ladder-output", $udLadder,
        "--slate-date", $Date
    )
    $udOk = (($c -eq 0) -and (Test-Path -LiteralPath $udLadder))

    $dkParts = New-Object System.Collections.Generic.List[string]
    foreach ($lg in ($DkLeagues -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ })) {
        $part = Join-Path $booksDir ("draftkings_props_{0}.csv" -f $lg)
        $code = Invoke-SoftPy "DraftKings $lg" @(
            (Join-Path $Root "scripts\fetch_draftkings_player_props.py"),
            "--league", $lg,
            "-o", $part
        )
        if (($code -eq 0) -and (Test-Path -LiteralPath $part)) {
            $dkParts.Add($part) | Out-Null
        }
    }
    $dkAll = Join-Path $Root "outputs\$Date\draftkings_props_all.csv"
    $dkLadder = Join-Path $booksDir "draftkings_props_ladder.csv"
    if ($dkParts.Count -gt 0) {
        $mergeArgs = @(
            (Join-Path $Root "scripts\merge_draftkings_pickem_csvs.py"),
            "--inputs"
        ) + $dkParts.ToArray() + @("-o", $dkAll)
        Invoke-SoftPy "DraftKings merge" $mergeArgs | Out-Null
        if (Test-Path -LiteralPath $dkAll) {
            Copy-Item -LiteralPath $dkAll -Destination $dkLadder -Force
            $dkOk = $true
        }
    }
}

$archArgs = @(
    (Join-Path $Root "scripts\archive_book_line_history.py"),
    "--date", $Date,
    "--repo-root", $Root
)
if ($SeedOnly) { $archArgs += "--seed-only" }
$archCode = Invoke-SoftPy "Archive history" $archArgs

Invoke-SoftPy "Softness report" @(
    (Join-Path $Root "scripts\report_book_line_softness.py"),
    "--repo-root", $Root
) | Out-Null

$status = if ($SeedOnly) { "seeded" } elseif ($fdOk -or $udOk -or $dkOk) { "ok" } else { "empty" }
Write-CaptureStamp -Status $status -Extra @{
    fanduel_ok   = $fdOk
    underdog_ok  = $udOk
    draftkings_ok = $dkOk
    archive_exit = $archCode
}

Write-Host "[BOOK-LINES] done for $Date status=$status (FD=$fdOk UD=$udOk DK=$dkOk) — stays on until ladders complete" -ForegroundColor Green
exit 0
