# Refresh WNBA1H + WNBA1Q step1-8 for a slate date (no combined/tickets).
#
# PrizePicks league IDs (Sports/WNBA/prizepicks_league_ids.py, CDP /leagues 2026-07-22):
#   WNBA1H=193  WNBA1Q=308  (WNBA4Q=195, WNBA2H=194 — not wired)
#
# Called from scripts\run_wnba_pipeline.ps1 after a full-game step8 (SoftFail).
# Empty / missing period boards skip like NBA 1H/1Q — they do not fail the full WNBA run.
#
# -Date must match PrizePicks filtered_game_dates (America/New_York).
#
# See docs/runbooks/WNBA_PERIOD_SLATE_REFRESH.md
param(
    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [string]$Cdp = "",
    [switch]$PreferBrowser,
    [switch]$SkipFetch,
    [switch]$Force,
    [switch]$IncludeTomorrow,
    [switch]$SoftFail
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Py = "py"
$PyVer = "-3.14"

$WNBA_SEASON_RESUME = "2026-07-28"
if ($env:WNBA_RESUME_DATE) { $WNBA_SEASON_RESUME = $env:WNBA_RESUME_DATE.Trim() }
elseif ($env:PROPORACLE_WNBA_RESUME) { $WNBA_SEASON_RESUME = $env:PROPORACLE_WNBA_RESUME.Trim() }
$WNBA_ALLSTAR_PAUSE_START = "2026-07-19"
if ($env:WNBA_PAUSE_START) { $WNBA_ALLSTAR_PAUSE_START = $env:WNBA_PAUSE_START.Trim() }
elseif ($env:PROPORACLE_WNBA_PAUSE_START) { $WNBA_ALLSTAR_PAUSE_START = $env:PROPORACLE_WNBA_PAUSE_START.Trim() }
if (-not $Force.IsPresent -and ($Date -ge $WNBA_ALLSTAR_PAUSE_START) -and ($Date -lt $WNBA_SEASON_RESUME)) {
    Write-Host "[WNBA period] All-Star pause — skipped until $WNBA_SEASON_RESUME (use -Force)." -ForegroundColor DarkGray
    exit 0
}

$WNBADir = Join-Path $Root "Sports\WNBA"
$OutDir = Join-Path $Root "outputs\$Date"
$NbaApiStep1 = Join-Path $Root "Sports\NBA\scripts\step1_fetch_prizepicks_api.py"

$Cdp = $Cdp.Trim()
if (-not $Cdp) { $Cdp = [string]$env:PROPORACLE_PP_CDP }
if (-not $Cdp) { $Cdp = [string]$env:PRIZEPICKS_CDP }
$Cdp = $Cdp.Trim()
$cdpDefault = if ($Cdp) { $Cdp } else { "http://127.0.0.1:9222" }

foreach ($t in @("wnba1h", "wnba1q")) {
    New-Item -ItemType Directory -Force -Path (Join-Path $OutDir $t) | Out-Null
}

function Get-CsvDataRowCount([string]$CsvPath) {
    if (-not (Test-Path -LiteralPath $CsvPath)) { return 0 }
    try {
        $raw = Import-Csv -LiteralPath $CsvPath
        if ($null -eq $raw) { return 0 }
        if ($raw -is [array]) { return $raw.Count }
        return 1
    } catch {
        return 0
    }
}

function Test-CdpEndpoint {
    param([string]$BaseUrl)
    try {
        $u = ($BaseUrl.TrimEnd("/")) + "/json/version"
        $r = Invoke-WebRequest -Uri $u -UseBasicParsing -TimeoutSec 4
        return ($r.StatusCode -eq 200)
    } catch {
        return $false
    }
}

function Test-PeriodFetchEmpty {
    param([string]$Joined, [int]$ExitCode, [string]$Step1Out)
    if ((Get-CsvDataRowCount -CsvPath $Step1Out) -eq 0) { return $true }
    if ($ExitCode -eq 0) { return $false }
    if ($Joined -match "No projections returned|BOARD_TOO_SMALL|FETCH_FAILED") { return $true }
    return $false
}

function Clear-PeriodStale {
    param([string[]]$Paths)
    foreach ($p in $Paths) {
        Remove-Item -LiteralPath $p -Force -ErrorAction SilentlyContinue
    }
}

function Run-StepLocal {
    param(
        [string]$Label,
        [string]$Script,
        [string[]]$StepArgs
    )
    Write-Host "  --> $Label" -ForegroundColor Yellow
    Push-Location $WNBADir
    try {
        & $Py $PyVer $Script @StepArgs
        if ($LASTEXITCODE -ne 0) {
            throw "$Label failed (exit $LASTEXITCODE)"
        }
    } finally {
        Pop-Location
    }
}

function Invoke-PeriodStep1 {
    param(
        [string]$TagLower,
        [string]$LeagueId,
        [string]$SportTag,
        [string]$Step1Out
    )
    $cdpReachable = Test-CdpEndpoint -BaseUrl $cdpDefault
    $useBrowser = $PreferBrowser -or $Cdp -or $cdpReachable

    $common = @(
        "--league_id", $LeagueId,
        "--game_mode", "pickem",
        "--per_page", "250",
        "--max_pages", "5",
        "--sleep", "2.0",
        "--cooldown_seconds", "90",
        "--max_cooldowns", "3",
        "--jitter_seconds", "10.0",
        "--min_rows", "5",
        "--min_teams", "2",
        "--sport-tag", $SportTag,
        "--output", $Step1Out,
        "--date", $Date
    )
    if ($IncludeTomorrow) {
        $common += @("--include-tomorrow")
    }

    Push-Location $WNBADir
    try {
        if ($useBrowser) {
            $browserArgs = $common + @("--playwright", "--timeout", "120")
            if ($Cdp) {
                $browserArgs += @("--cdp", $Cdp)
            } elseif ($cdpReachable) {
                $browserArgs += @("--cdp", $cdpDefault)
            }
            Write-Host "  --> step1 $TagLower (browser/CDP, league_id=$LeagueId)" -ForegroundColor Yellow
            $out = & $Py $PyVer ".\step1_fetch_prizepicks.py" @browserArgs 2>&1
            $exit = $LASTEXITCODE
            foreach ($line in $out) { Write-Host "        $line" -ForegroundColor DarkGray }
            $joined = ($out | Out-String)
            if (Test-PeriodFetchEmpty -Joined $joined -ExitCode $exit -Step1Out $Step1Out) {
                return "no_slate"
            }
            if ($exit -ne 0) { return "failed" }
            return "ok"
        }

        Write-Host "  --> step1 $TagLower (HTTP, league_id=$LeagueId)" -ForegroundColor Yellow
        $out = & $Py $PyVer ".\step1_fetch_prizepicks.py" @common 2>&1
        $exit = $LASTEXITCODE
        foreach ($line in $out) { Write-Host "        $line" -ForegroundColor DarkGray }
        $joined = ($out | Out-String)
        if ($exit -eq 0 -and (Get-CsvDataRowCount -CsvPath $Step1Out) -gt 0) {
            return "ok"
        }
        if (Test-PeriodFetchEmpty -Joined $joined -ExitCode $exit -Step1Out $Step1Out) {
            if (-not (Test-Path -LiteralPath $NbaApiStep1)) {
                return "no_slate"
            }
        }
        Write-Host "  [WNBA period] HTTP failed - trying NBA API fallback..." -ForegroundColor Yellow
        if (-not (Test-Path -LiteralPath $NbaApiStep1)) {
            if (Test-PeriodFetchEmpty -Joined $joined -ExitCode $exit -Step1Out $Step1Out) {
                return "no_slate"
            }
            return "failed"
        }
        $fbOut = & $Py $PyVer $NbaApiStep1 `
            --league_id $LeagueId --game_mode pickem --per_page 250 --max_pages 5 `
            --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 `
            --replace --output $Step1Out --date $Date 2>&1
        $fbExit = $LASTEXITCODE
        foreach ($line in $fbOut) { Write-Host "        $line" -ForegroundColor DarkGray }
        $fbJoined = ($fbOut | Out-String)
        if (Test-PeriodFetchEmpty -Joined $fbJoined -ExitCode $fbExit -Step1Out $Step1Out) {
            return "no_slate"
        }
        if ($fbExit -ne 0) { return "failed" }
        & $Py $PyVer -c "import pandas as pd; from pathlib import Path; p=Path(r'$Step1Out'); df=pd.read_csv(p); df['sport']='$SportTag'; df.to_csv(p,index=False,encoding='utf-8-sig'); print(f'stamped sport=$SportTag rows={len(df)}')"
        if ((Get-CsvDataRowCount -CsvPath $Step1Out) -eq 0) { return "no_slate" }
        return "ok"
    } finally {
        Pop-Location
    }
}

function Run-WNBAPeriodOnly {
    param(
        [string]$Tag,
        [string]$LeagueId
    )
    $tagLower = $Tag.ToLower()
    $sportTag = $Tag.ToUpper()
    $periodOutDir = Join-Path $OutDir $tagLower
    $step1 = Join-Path $periodOutDir "step1_${tagLower}_props.csv"
    $step2 = Join-Path $periodOutDir "step2_${tagLower}_picktypes.csv"
    $step3 = Join-Path $periodOutDir "step3_${tagLower}_defense.csv"
    $step4 = Join-Path $periodOutDir "step4_${tagLower}_stats.csv"
    $step5 = Join-Path $periodOutDir "step5_${tagLower}_hitrates.csv"
    $step6 = Join-Path $periodOutDir "step6_${tagLower}_context.csv"
    $step7 = Join-Path $periodOutDir "step7_${tagLower}_ranked.xlsx"
    $step8Csv = Join-Path $periodOutDir "step8_${tagLower}_direction.csv"
    $step8Xlsx = Join-Path $periodOutDir "step8_${tagLower}_direction_clean.xlsx"
    $datedOut = Join-Path $OutDir "step8_${tagLower}_direction_clean_${Date}.xlsx"
    $staleAfterStep1 = @($step2, $step3, $step4, $step5, $step6, $step7, $step8Csv, $step8Xlsx, $datedOut)

    Write-Host ""
    Write-Host "[ WNBA PERIOD: $tagLower | league_id=$LeagueId ]" -ForegroundColor Magenta

    if (-not $SkipFetch) {
        $fetchStatus = Invoke-PeriodStep1 -TagLower $tagLower -LeagueId $LeagueId -SportTag $sportTag -Step1Out $step1
        if ($fetchStatus -eq "no_slate") {
            Write-Host "      No live $tagLower board right now — clearing stale period files and skipping." -ForegroundColor DarkGray
            Clear-PeriodStale -Paths $staleAfterStep1
            return "no_slate"
        }
        if ($fetchStatus -ne "ok") {
            Write-Host "      $tagLower step1 FAILED." -ForegroundColor Red
            return "failed"
        }
    } else {
        Write-Host "  --> [SkipFetch] Using existing $step1" -ForegroundColor DarkGray
        if (-not (Test-Path -LiteralPath $step1)) {
            Write-Host "      SkipFetch set but missing $step1 — skipping $tagLower." -ForegroundColor DarkGray
            return "no_slate"
        }
        if ((Get-CsvDataRowCount -CsvPath $step1) -eq 0) {
            Write-Host "      SkipFetch step1 is empty — skipping $tagLower." -ForegroundColor DarkGray
            Clear-PeriodStale -Paths $staleAfterStep1
            return "no_slate"
        }
    }

    try {
        Run-StepLocal "step2" ".\step2_attach_picktypes.py" @("--input", $step1, "--output", $step2)
        Run-StepLocal "step3" ".\step3_attach_defense.py" @(
            "--input", $step2, "--defense", "wnba_defense_summary.csv", "--output", $step3
        )
        # Full-game ESPN rolling stats as period *projection* proxy (same early NBA1H/1Q).
        # Grading uses period actuals via fetch_nba_period_actuals.py --sport WNBA --segment 1H|1Q.
        Run-StepLocal "step4" ".\step4_fetch_player_stats.py" @(
            "--slate", $step3, "--out", $step4, "--season", "2026", "--date", $Date,
            "--days", "35", "--cache", "wnba_espn_cache.csv", "--sleep", "0.8",
            "--retries", "4", "--timeout", "30", "--debug-misses", "wnba_no_espn_debug.csv"
        )
        Run-StepLocal "step4b" ".\scripts\step4b_attach_wnba_context.py" @(
            "--input", $step4, "--output", $step4, "--season", "2025"
        )
        Run-StepLocal "step5" ".\step5_add_line_hit_rates.py" @(
            "--input", $step4, "--output", $step5, "--compute10"
        )
        Run-StepLocal "step6" ".\step6_team_role_context.py" @("--input", $step5, "--output", $step6)
        Run-StepLocal "step7" ".\step7_rank_props.py" @(
            "--input", $step6, "--output", $step7, "--date", $Date
        )
        Run-StepLocal "step8" ".\step8_add_direction_context.py" @(
            "--input", $step7, "--sheet", "ALL", "--output", $step8Csv,
            "--xlsx", $step8Xlsx, "--date", $Date
        )
    } catch {
        Write-Host "      $tagLower pipeline failed: $_" -ForegroundColor Red
        return "failed"
    }

    $legacy = Join-Path $WNBADir "step8_${tagLower}_direction_clean.xlsx"
    if (Test-Path -LiteralPath $step8Xlsx) {
        Copy-Item -LiteralPath $step8Xlsx -Destination $legacy -Force
        Copy-Item -LiteralPath $step8Xlsx -Destination $datedOut -Force -ErrorAction SilentlyContinue
        Write-Host "  OK $tagLower -> $step8Xlsx" -ForegroundColor Green
        return "ok"
    }
    Write-Host "      $tagLower missing step8 workbook after run." -ForegroundColor Red
    return "failed"
}

# League IDs from Sports/WNBA/prizepicks_league_ids.py (verified CDP 2026-07-22)
$hadHardFail = $false
foreach ($pair in @(
    @{ Tag = "wnba1h"; LeagueId = "193" },
    @{ Tag = "wnba1q"; LeagueId = "308" }
)) {
    $status = Run-WNBAPeriodOnly -Tag $pair.Tag -LeagueId $pair.LeagueId
    if ($status -eq "failed") { $hadHardFail = $true }
}

foreach ($tag in @("wnba1h", "wnba1q")) {
    $step8 = Join-Path $OutDir "$tag\step8_${tag}_direction.csv"
    if (-not (Test-Path -LiteralPath $step8)) {
        Write-Host "  --> Matchup edge JSON ($tag) skipped (no step8)" -ForegroundColor DarkGray
        continue
    }
    Write-Host "  --> Matchup edge JSON ($tag)" -ForegroundColor Yellow
    Push-Location $Root
    try {
        $meArgs = @(".\scripts\build_matchup_edge_json.py", "--sport", $tag, "--slate", $step8)
        & $Py $PyVer @meArgs
        if ($LASTEXITCODE -ne 0) {
            Write-Host "      matchup-edge WARN ($tag exit $LASTEXITCODE)" -ForegroundColor Yellow
        } else {
            Write-Host "      OK" -ForegroundColor Green
        }
    } finally {
        Pop-Location
    }
}

Write-Host ""
Write-Host "WNBA period refresh done for $Date (wnba1h + wnba1q)." -ForegroundColor Cyan
if ($hadHardFail -and -not $SoftFail) {
    Write-Host "One or more period boards failed." -ForegroundColor Red
    exit 1
}
if ($hadHardFail) {
    Write-Host "Period board failure treated as non-fatal (SoftFail)." -ForegroundColor Yellow
}
exit 0
