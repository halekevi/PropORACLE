# Refresh NBA1H + NBA1Q step1-8 for a slate date (no combined/tickets).
#
# -Date must match PrizePicks filtered_game_dates (America/New_York). Late playoff
#   games often list as the *next* calendar day vs your local run date - if step1
#   prints "0 rows", re-run with that board date (e.g. -Date 2026-05-30).
#
# Period step4 needs the nba1q table in data/cache/proporacle_ref.db (same DB as
# full-game NBA). If L5 is all NO_DATA, rebuild history first:
#   py -3.14 scripts\build_nba1q_history_db.py
#
# After this script:
#   py -3 scripts/build_matchup_edge_json.py --sport nba1h
#   py -3 scripts/build_matchup_edge_json.py --sport nba1q
#   (one sport per invocation - "nba1h nba1q" is not valid)
#   py -3 scripts/generate_mobile_bundle.py
#
# See docs/runbooks/NBA_PERIOD_SLATE_REFRESH.md
param(
    [string]$Date = (Get-Date -Format "yyyy-MM-dd"),
    [switch]$SkipFetch,
    [switch]$SoftFail,
    [switch]$ForceHistoryRebuild
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Py = "py"
$PyVer = "-3.14"

$NBADir = Join-Path $Root "Sports\NBA"
$OutDir = Join-Path $Root "outputs\$Date"
$SportsRoot = Join-Path $Root "Sports"
try {
    $nbaDt = [datetime]::ParseExact($Date, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
    $nbaStart = if ($nbaDt.Month -ge 10) { $nbaDt.Year } else { $nbaDt.Year - 1 }
} catch {
    $nbaStart = if ((Get-Date).Month -ge 10) { (Get-Date).Year } else { (Get-Date).Year - 1 }
}
$NbaStatsSeason = '{0}-{1:d2}' -f $nbaStart, (($nbaStart + 1) % 100)

foreach ($t in @("nba1h", "nba1q")) {
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
    Push-Location $NBADir
    try {
        $prevUtf8 = $env:PYTHONUTF8
        $env:PYTHONUTF8 = "1"
        & $Py $PyVer $Script @StepArgs
        if ($null -ne $prevUtf8) { $env:PYTHONUTF8 = $prevUtf8 } else { Remove-Item Env:PYTHONUTF8 -ErrorAction SilentlyContinue }
        if ($LASTEXITCODE -ne 0) {
            throw "$Label failed (exit $LASTEXITCODE)"
        }
    } finally {
        Pop-Location
    }
}

# Ensure period L5 history lives in the same DB step4 reads (root cache).
$buildHist = Join-Path $Root "scripts\build_nba1q_history_db.py"
$rootDb = Join-Path $Root "data\cache\proporacle_ref.db"
$checkHist = Join-Path $Root "scripts\_check_nba1q_table.py"
$needHist = $ForceHistoryRebuild.IsPresent
if (-not $needHist -and (Test-Path -LiteralPath $checkHist)) {
    $check = & $Py $PyVer $checkHist $rootDb 2>$null
    if ("$check" -match "missing") { $needHist = $true }
}
if ($needHist -and (Test-Path -LiteralPath $buildHist)) {
    Write-Host "  --> Rebuilding nba1q history into root proporacle_ref.db" -ForegroundColor Yellow
    Push-Location $Root
    try {
        & $Py $PyVer $buildHist
        if ($LASTEXITCODE -ne 0) {
            Write-Host "      build_nba1q_history_db WARN - period L5 may be thin" -ForegroundColor Yellow
        }
    } finally {
        Pop-Location
    }
}

function Run-NBAPeriodOnly {
    param(
        [string]$Tag,
        [string]$LeagueId
    )
    $tagLower = $Tag.ToLower()
    $segment = if ($tagLower -eq "nba1q") { "1Q" } else { "1H" }
    $periodOutDir = Join-Path $OutDir $tagLower
    $step1 = Join-Path $periodOutDir "step1_${tagLower}_props.csv"
    $step2 = Join-Path $periodOutDir "step2_${tagLower}_picktypes.csv"
    $step3 = Join-Path $periodOutDir "step3_${tagLower}_with_defense.csv"
    $step4 = Join-Path $periodOutDir "step4_${tagLower}_with_stats.csv"
    $step5 = Join-Path $periodOutDir "step5_${tagLower}_with_hit_rates.csv"
    $step6 = Join-Path $periodOutDir "step6_${tagLower}_with_team_role_context.csv"
    $step7 = Join-Path $periodOutDir "step7_${tagLower}_ranked_props.xlsx"
    $step8Csv = Join-Path $periodOutDir "step8_${tagLower}_direction.csv"
    $step8Xlsx = Join-Path $periodOutDir "step8_${tagLower}_direction_clean.xlsx"
    $datedOut = Join-Path $OutDir "step8_${tagLower}_direction_clean_${Date}.xlsx"
    $step8Script = Join-Path $SportsRoot "NBA\scripts\step8_add_direction_context.py"
    $staleAfterStep1 = @($step2, $step3, $step4, $step5, $step6, $step7, $step8Csv, $step8Xlsx, $datedOut)

    Write-Host ""
    Write-Host "[ NBA PERIOD: $tagLower | league_id=$LeagueId ]" -ForegroundColor Magenta

    if (-not $SkipFetch) {
        Push-Location $NBADir
        try {
            Write-Host ("  --> step1 {0} HTTP league_id={1}" -f $tagLower, $LeagueId) -ForegroundColor Yellow
            & $Py $PyVer ".\scripts\step1_fetch_prizepicks_api.py" `
                --league_id $LeagueId --game_mode pickem --per_page 250 --max_pages 5 `
                --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 `
                --replace --output $step1 --date $Date
            $exit = $LASTEXITCODE
        } finally {
            Pop-Location
        }
        if ((Get-CsvDataRowCount -CsvPath $step1) -eq 0) {
            Write-Host "      No live board - clearing stale period files and skipping." -ForegroundColor DarkGray
            Clear-PeriodStale -Paths $staleAfterStep1
            return "no_slate"
        }
        if ($exit -ne 0) {
            Write-Host ("      {0} step1 FAILED exit={1}" -f $tagLower, $exit) -ForegroundColor Red
            return "failed"
        }
    } else {
        Write-Host ("  --> [SkipFetch] Using existing {0}" -f $step1) -ForegroundColor DarkGray
        if (-not (Test-Path -LiteralPath $step1)) {
            Write-Host ("      SkipFetch set but missing {0} - skipping." -f $step1) -ForegroundColor DarkGray
            return "no_slate"
        }
        if ((Get-CsvDataRowCount -CsvPath $step1) -eq 0) {
            Write-Host "      SkipFetch step1 is empty - skipping." -ForegroundColor DarkGray
            Clear-PeriodStale -Paths $staleAfterStep1
            return "no_slate"
        }
    }

    try {
        Run-StepLocal "step2" ".\scripts\step2_attach_picktypes.py" @("--input", $step1, "--output", $step2)
        Run-StepLocal "step3" ".\scripts\step3_attach_defense.py" @(
            "--input", $step2, "--defense", "data\cache\defense_team_summary.csv", "--output", $step3
        )
        Run-StepLocal "step4" ".\scripts\step4_attach_player_stats_espn_cache.py" @(
            "--slate", $step3, "--out", $step4, "--date", $Date, "--segment", $segment
        )
        Run-StepLocal "step4b" ".\scripts\step4b_attach_nba_context.py" @(
            "--input", $step4, "--output", $step4, "--season", $NbaStatsSeason
        )
        if ($tagLower -eq "nba1h") {
            # step4e is enrichment-only; do not block period step8 on profile cache I/O.
            try {
                Run-StepLocal "step4e" ".\scripts\step4e_attach_nba1h_context.py" @(
                    "--input", $step4, "--output", $step4
                )
            } catch {
                Write-Host ("      step4e WARN non-fatal: {0}" -f $_) -ForegroundColor Yellow
            }
        }
        Run-StepLocal "step5" ".\scripts\step5_add_line_hit_rates.py" @(
            "--input", $step4, "--output", $step5, "--compute10"
        )
        Run-StepLocal "step6" ".\scripts\step6_team_role_context.py" @("--input", $step5, "--output", $step6)
        Run-StepLocal "step7" ".\scripts\step7_rank_props.py" @("--input", $step6, "--output", $step7)
        Run-StepLocal "step8" $step8Script @(
            "--input", $step7, "--sheet", "ALL", "--output", $step8Csv, "--xlsx", $step8Xlsx, "--date", $Date
        )
    } catch {
        Write-Host ("      {0} pipeline failed: {1}" -f $tagLower, $_) -ForegroundColor Red
        return "failed"
    }

    $legacy = Join-Path $NBADir "step8_${tagLower}_direction_clean.xlsx"
    if (Test-Path -LiteralPath $step8Xlsx) {
        foreach ($dest in @($legacy, $datedOut)) {
            try {
                Copy-Item -LiteralPath $step8Xlsx -Destination $dest -Force -ErrorAction Stop
            } catch {
                Write-Host ("      WARN mirror skipped {0}: {1}" -f $dest, $_) -ForegroundColor Yellow
            }
        }
        Write-Host ("  OK {0} -> {1}" -f $tagLower, $step8Xlsx) -ForegroundColor Green
        return "ok"
    }
    if (Test-Path -LiteralPath $step8Csv) {
        Write-Host ("      {0} step8 CSV present but clean xlsx missing - treating as partial OK" -f $tagLower) -ForegroundColor Yellow
        return "ok"
    }
    Write-Host ("      {0} missing step8 workbook after run." -f $tagLower) -ForegroundColor Red
    return "failed"
}

$hadHardFail = $false
foreach ($pair in @(
    @{ Tag = "nba1h"; LeagueId = "84" },
    @{ Tag = "nba1q"; LeagueId = "192" }
)) {
    $status = Run-NBAPeriodOnly -Tag $pair.Tag -LeagueId $pair.LeagueId
    if ($status -eq "failed") { $hadHardFail = $true }
}

foreach ($tag in @("nba1h", "nba1q")) {
    $step8 = Join-Path $OutDir "$tag\step8_${tag}_direction.csv"
    if (-not (Test-Path -LiteralPath $step8)) {
        Write-Host ("  --> Matchup edge JSON {0} skipped - no step8" -f $tag) -ForegroundColor DarkGray
        continue
    }
    Write-Host ("  --> Matchup edge JSON {0}" -f $tag) -ForegroundColor Yellow
    Push-Location $Root
    try {
        $meScript = ".\scripts\build_matchup_edge_json.py"
        & $Py $PyVer $meScript --sport $tag --slate $step8
        if ($LASTEXITCODE -ne 0) {
            Write-Host ("      matchup-edge WARN {0} exit={1}" -f $tag, $LASTEXITCODE) -ForegroundColor Yellow
        } else {
            Write-Host "      OK" -ForegroundColor Green
        }
    } finally {
        Pop-Location
    }
}

Write-Host ""
Write-Host ("NBA period refresh done for {0} nba1h+nba1q." -f $Date) -ForegroundColor Cyan
if ($hadHardFail -and -not $SoftFail) {
    Write-Host "One or more period boards failed." -ForegroundColor Red
    exit 1
}
if ($hadHardFail) {
    Write-Host "Period board failure treated as non-fatal SoftFail." -ForegroundColor Yellow
}
exit 0
