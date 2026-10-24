# NFL Pipeline — aligned with run_pipeline.ps1 NFL job order
# NFL (PrizePicks 9) and NFLP preseason (44) share this run and the same step8 sheet.
# step1 → … → step7 → step7b edge score → step8
param(
    [string]$Date = "",
    [switch]$SkipFetch
)

$ErrorActionPreference = "Continue"
$ScriptPath = $MyInvocation.MyCommand.Path
if (-not $ScriptPath) { $ScriptPath = $PSCommandPath }
$ScriptDir = Split-Path -Parent $ScriptPath
$Root = Split-Path -Parent $ScriptDir
$NFLDir = Join-Path $Root "Sports\NFL"

if (-not $Date) { $Date = Get-Date -Format "yyyy-MM-dd" }
$OutDir = Join-Path $Root "outputs\$Date\nfl"
$SportOutDir = Join-Path $NFLDir "outputs"
$DataOutDir = Join-Path $NFLDir "data\outputs"
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }
if (-not (Test-Path $SportOutDir)) { New-Item -ItemType Directory -Force -Path $SportOutDir | Out-Null }
if (-not (Test-Path $DataOutDir)) { New-Item -ItemType Directory -Force -Path $DataOutDir | Out-Null }

$env:NFL_PIPELINE_ACTIVE = "1"
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
if (-not ("$env:PROPORACLE_CURL_IMPERSONATE").Trim()) {
    $env:PROPORACLE_CURL_IMPERSONATE = "chrome131"
}
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp 65001 | Out-Null } catch { }

. (Join-Path $Root "scripts\prizepicks_step1_cascade.ps1")

if (Test-Path "$Root\.venv\Scripts\Activate.ps1") {
    & "$Root\.venv\Scripts\Activate.ps1"
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  NFL PIPELINE  |  $Date  |  $OutDir" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# Soft context age banner (does not fail the run).
try {
    & py -3.14 -c "from utils.nfl_context_freshness import warn_if_stale; warn_if_stale(label='NFL pipeline')"
} catch {
    Write-Host "  [warn] context freshness check skipped" -ForegroundColor DarkYellow
}

function Run-Step {
    param(
        [string]$Label,
        [string]$Dir,
        [string]$Script,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$ArgList = @()
    )
    Write-Host "  --> $Label" -ForegroundColor Yellow
    Push-Location $Dir
    try {
        $argArray = @($ArgList | Where-Object { $_ -ne $null -and "$_" -ne "" })
        if ($argArray.Count -gt 0) {
            $output = & py -3.14 $Script @argArray 2>&1
        } else {
            $output = & py -3.14 $Script 2>&1
        }
        $exit = $LASTEXITCODE
        $output | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
        if ($exit -ne 0) {
            Write-Host "      FAILED (exit $exit)" -ForegroundColor Red
            return $false
        }
        Write-Host "      OK" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "      EXCEPTION: $_" -ForegroundColor Red
        return $false
    } finally {
        Pop-Location
    }
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

function Get-NflGameBoardRowCount([string]$CsvPath) {
    # NFL (9) + NFLP (44) count as the daily board. NFLSZN (163) does not.
    if (-not (Test-Path -LiteralPath $CsvPath)) { return 0 }
    try {
        $raw = @(Import-Csv -LiteralPath $CsvPath)
        $daily = @($raw | Where-Object {
            $lg = if ($_.PSObject.Properties.Name -contains "league") { ("$($_.league)").Trim().ToUpper() } else { "" }
            $lid = if ($_.PSObject.Properties.Name -contains "league_id") { ("$($_.league_id)").Trim() } else { "" }
            -not ($lg -eq "NFLSZN" -or $lid -eq "163")
        })
        return $daily.Count
    } catch {
        return 0
    }
}

$s1 = Join-Path $OutDir "step1_pp_props_today.csv"
$s2 = Join-Path $DataOutDir "step2_clean_props.csv"
$s3 = Join-Path $DataOutDir "step3_nfl_with_defense.csv"
$s3dated = Join-Path $OutDir "step3_nfl_with_defense.csv"
$s5 = Join-Path $DataOutDir "step5_nfl_with_stats.csv"
$s6 = Join-Path $DataOutDir "step6_hit_rates.csv"
$s6b = Join-Path $DataOutDir "step6b_with_game_context.csv"
$s7 = Join-Path $OutDir "step7_nfl_ranked.xlsx"
$s8 = Join-Path $OutDir "step8_nfl_direction_clean.xlsx"

$ok = $true

if (-not $SkipFetch) {
    if ($ok) {
        $ok = Invoke-PrizePicksStep1Cascade -SportLabel "NFL" -WorkDir $NFLDir `
            -ScriptRel ".\scripts\step1_fetch_prizepicks_nfl.py" `
            -OutputPath $s1 -PipelineDate $Date `
            -HttpArgs @("--output", $s1, "--date", $Date) `
            -SkipDateHealth
    }
} else {
    Write-Host "  [SkipFetch] Using existing $s1" -ForegroundColor DarkGray
    if (-not (Test-Path $s1)) {
        Write-Host "  ERROR: SkipFetch but missing $s1" -ForegroundColor Red
        $ok = $false
    }
}

$step1Rows = Get-CsvDataRowCount -CsvPath $s1
$gameRows = Get-NflGameBoardRowCount -CsvPath $s1
if ($ok -and $step1Rows -eq 0) {
    Write-Host "[NFL] Off-season - no board for $Date. Exiting."
    exit 0
}
if ($ok -and $gameRows -eq 0) {
    Write-Host "[NFL] No daily slate (NFLSZN-only or empty) — skipping remaining steps."
    exit 0
}
if ($ok -and (Test-Path -LiteralPath $s1)) {
    Copy-Item -LiteralPath $s1 -Destination (Join-Path $DataOutDir "step1_pp_props_today.csv") -Force
}

if ($ok) {
    $ok = Run-Step "NFL Step 2 - Clean Props" $NFLDir ".\scripts\step2_clean_props.py" --input $s1 --output $s2
}
if ($ok) {
    $ok = Run-Step "NFL Refresh Rankings" $Root ".\scripts\refresh_rankings.py" --sport nfl
}
if ($ok) {
    $ok = Run-Step "NFL Step 4 - Defense Rankings" $NFLDir ".\scripts\step4_defense_rankings.py" --output data\defense_rankings.csv
}
if ($ok) {
    $ok = Run-Step "NFL Step 4b - Team Last-5 Form" $NFLDir ".\scripts\step4b_team_last5_games.py" --output data\nfl_team_last5.csv
}
if ($ok) {
    $ok = Run-Step "NFL Step 3 - Merge Defense" $NFLDir ".\scripts\step3_merge_defense_nfl.py" --input $s2 --output $s3 --defense-source auto --team-form data\nfl_team_last5.csv
}
if ($ok -and (Test-Path -LiteralPath $s3)) {
    Copy-Item -LiteralPath $s3 -Destination $s3dated -Force
}
if ($ok) {
    $ok = Run-Step "NFL Step 4c - Snap / Role" $NFLDir ".\scripts\step4c_attach_role_context_nfl.py" --input $s3 --output $s3
}
$okLeaders = Run-Step "NFL Stat Leaders" $Root ".\Sports\NFL\scripts\build_nfl_stat_leaders.py"
if (-not $okLeaders) {
    Write-Host "      [warn] NFL stat leaders skipped" -ForegroundColor DarkYellow
}
$okDiv = Run-Step "NFL Division Ranks" $Root ".\Sports\NFL\scripts\build_nfl_division_ranks.py"
if (-not $okDiv) {
    Write-Host "      [warn] NFL division ranks skipped" -ForegroundColor DarkYellow
}
$okOd = Run-Step "NFL O/D Rankings MD" $Root ".\Sports\NFL\scripts\build_nfl_od_rankings_md.py" --season 2026
if (-not $okOd) {
    Write-Host "      [warn] NFL O/D rankings md skipped" -ForegroundColor DarkYellow
}
$okVeh = Run-Step "NFL Scoring Vehicles" $Root ".\Sports\NFL\scripts\build_nfl_scoring_vehicles.py"
if (-not $okVeh) {
    Write-Host "      [warn] NFL scoring vehicles skipped" -ForegroundColor DarkYellow
}
$okShare = Run-Step "NFL Team Share" $Root ".\scripts\build_team_share_json.py" --sports nfl
if (-not $okShare) {
    Write-Host "      [warn] NFL team share skipped" -ForegroundColor DarkYellow
}
if ($ok) {
    $ok = Run-Step "NFL Step 4d - Injuries" $NFLDir ".\scripts\step4d_attach_injury_nfl.py" --input $s3 --output $s3 --refresh
}
if ($ok -and (Test-Path -LiteralPath $s3)) {
    Copy-Item -LiteralPath $s3 -Destination $s3dated -Force
}
$injOut = Join-Path $OutDir "injuries_nfl_$Date.csv"
$injRoot = Join-Path $Root "outputs\$Date\injuries_nfl_$Date.csv"
if ($ok) {
    $okInj = Run-Step "NFL Injuries CSV" $Root ".\scripts\espn_injuries.py" --sport NFL --date $Date --output $injRoot
    if ($okInj -and (Test-Path -LiteralPath $injRoot)) {
        Copy-Item -LiteralPath $injRoot -Destination $injOut -Force
    } else {
        Write-Host "      [warn] injuries CSV skipped" -ForegroundColor DarkYellow
    }
}
$defCsv = Join-Path $NFLDir "data\defense_rankings.csv"
if ($ok -and (Test-Path -LiteralPath $defCsv)) {
    Copy-Item -LiteralPath $defCsv -Destination (Join-Path $OutDir "defense_rankings.csv") -Force
}
$refDef = Join-Path $Root "data\reference\nfl_team_defense.csv"
if ($ok -and (Test-Path -LiteralPath $refDef)) {
    Copy-Item -LiteralPath $refDef -Destination (Join-Path $OutDir "nfl_team_defense.csv") -Force
}
if ($ok) {
        $ok = Run-Step "NFL Step 5 - Boxscore Stats" $NFLDir ".\scripts\step5_attach_boxscore_stats_nfl.py" --input $s3 --output $s5 --date $Date --cache data\cache\nfl_boxscore_cache.csv --days 400
}
if ($ok) {
    $ok = Run-Step "NFL Step 6 - Hit Rates" $NFLDir ".\scripts\step6_historical_hit_rates.py" --input $s5 --output $s6
}
if ($ok) {
    $ok = Run-Step "NFL Step 6b - Game Context" $NFLDir ".\scripts\step6b_attach_game_context_nfl.py" --input $s6 --output $s6b --date $Date --cache (Join-Path $NFLDir "data\game_context_cache_$Date.csv")
}
if ($ok) {
    $ok = Run-Step "NFL Step 7 - Rank Props" $NFLDir ".\scripts\step7_rank_props_nfl.py" --input $s6b --output $s7
}
if ($ok) {
    Invoke-PyStep7b -SportLabel "NFL" -RepoRoot $Root -Step7Xlsx $s7 -PipelineDate $Date
}
if ($ok) {
    $ok = Run-Step "NFL Step 8 - Direction Context" $NFLDir ".\scripts\step8_add_direction_context_nfl.py" --input $s7 --output $s8 --date $Date
}

if ($ok -and (Test-Path -LiteralPath $s7)) {
    Copy-Item -LiteralPath $s7 -Destination (Join-Path $SportOutDir "step7_nfl_ranked.xlsx") -Force
}
if ($ok -and (Test-Path -LiteralPath $s8)) {
    Copy-Item -LiteralPath $s8 -Destination (Join-Path $SportOutDir "step8_nfl_direction_clean.xlsx") -Force
}

Write-Host ""
if ($ok) {
    Write-Host "  NFL pipeline complete -> $s8" -ForegroundColor Green
    exit 0
}
Write-Host "  NFL pipeline FAILED." -ForegroundColor Red
exit 1
