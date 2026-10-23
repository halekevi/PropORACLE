# Rebuild NBA full-game step2→step8 for a slate date (step1 must already exist).
param(
    [Parameter(Mandatory = $true)][string]$Date,
    [string]$Step1 = ""
)
$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$NBADir = Join-Path $Root "Sports\NBA"
$OutDated = Join-Path $Root "outputs\$Date\nba"
if (-not $Step1) {
    $Step1 = Join-Path $OutDated "step1_pp_props_today.csv"
}
$mainCp = Join-Path (Split-Path $Root -Parent) "PropORACLE_main_cp\outputs\$Date\nba\step1_pp_props_today.csv"
if (-not (Test-Path -LiteralPath $Step1) -and (Test-Path -LiteralPath $mainCp)) {
    $Step1 = $mainCp
}
if (-not (Test-Path -LiteralPath $Step1)) {
    throw "Missing step1: $Step1"
}
New-Item -ItemType Directory -Force -Path $OutDated | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $NBADir "data\outputs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $NBADir "data\inputs") | Out-Null

$legacyIn = Join-Path $NBADir "data\inputs\step1_pp_props_today.csv"
Copy-Item -LiteralPath $Step1 -Destination $legacyIn -Force
$datedStep1 = Join-Path $OutDated "step1_pp_props_today.csv"
if ([IO.Path]::GetFullPath($Step1) -ne [IO.Path]::GetFullPath($datedStep1)) {
    Copy-Item -LiteralPath $Step1 -Destination $datedStep1 -Force
}

function Run-Step {
    param([string]$Label, [string]$Script, [string[]]$StepArgs)
    Write-Host "--> $Label" -ForegroundColor Cyan
    Push-Location $NBADir
    try {
        $prevUtf8 = $env:PYTHONUTF8
        $env:PYTHONUTF8 = "1"
        & py -3.14 $Script @StepArgs
        if ($null -ne $prevUtf8) { $env:PYTHONUTF8 = $prevUtf8 } else { Remove-Item Env:PYTHONUTF8 -ErrorAction SilentlyContinue }
        if ($LASTEXITCODE -ne 0) { throw "$Label failed exit $LASTEXITCODE" }
    } finally { Pop-Location }
}

$s2 = Join-Path $OutDated "step2_with_picktypes.csv"
$s3 = Join-Path $OutDated "step3_with_defense.csv"
$s4 = Join-Path $OutDated "step4_with_stats.csv"
$s5 = Join-Path $OutDated "step5_with_hit_rates.csv"
$s6 = Join-Path $OutDated "step6_with_team_role_context.csv"
$s6a = Join-Path $OutDated "step6a_with_opp_stats.csv"
$s6b = Join-Path $OutDated "step6b_with_game_context.csv"
$s6c = Join-Path $OutDated "step6c_with_schedule_flags.csv"
$s6d = Join-Path $OutDated "step6d_with_h2h.csv"
$s7 = Join-Path $OutDated "step7_ranked_props.xlsx"
$s8csv = Join-Path $OutDated "step8_all_direction.csv"
$s8xlsx = Join-Path $OutDated "step8_all_direction_clean.xlsx"

Run-Step "step2" ".\scripts\step2_attach_picktypes.py" @("--input", $legacyIn, "--output", $s2)
Run-Step "step3" ".\scripts\step3_attach_defense.py" @(
    "--input", $s2, "--defense", "data\cache\defense_team_summary.csv", "--output", $s3
)
Run-Step "step4" ".\scripts\step4_attach_player_stats_espn_cache.py" @(
    "--slate", $s3, "--out", $s4, "--date", $Date
)
try {
    $nbaDt = [datetime]::ParseExact($Date, 'yyyy-MM-dd', [System.Globalization.CultureInfo]::InvariantCulture)
    $nbaStart = if ($nbaDt.Month -ge 10) { $nbaDt.Year } else { $nbaDt.Year - 1 }
} catch {
    $nbaStart = if ((Get-Date).Month -ge 10) { (Get-Date).Year } else { (Get-Date).Year - 1 }
}
$NbaStatsSeason = '{0}-{1:d2}' -f $nbaStart, (($nbaStart + 1) % 100)
Run-Step "step4b" ".\scripts\step4b_attach_nba_context.py" @(
    "--input", $s4, "--output", $s4, "--season", $NbaStatsSeason
)
Run-Step "step5" ".\scripts\step5_add_line_hit_rates.py" @("--input", $s4, "--output", $s5, "--compute10")
Run-Step "step6" ".\scripts\step6_team_role_context.py" @("--input", $s5, "--output", $s6)
# Optional enrichments — continue if a helper is missing
foreach ($pair in @(
    @("step6a", ".\scripts\step6a_attach_opponent_stats_NBA.py", @("--input", $s6, "--output", $s6a)),
    @("step6b", ".\scripts\step6b_attach_game_context.py", @("--input", $s6a, "--output", $s6b)),
    @("step6c", ".\scripts\step6c_schedule_flags.py", @("--input", $s6b, "--output", $s6c)),
    @("step6d", ".\scripts\step6d_attach_h2h_matchups.py", @("--input", $s6c, "--output", $s6d))
)) {
    $label = $pair[0]; $script = $pair[1]; $a = $pair[2]
    $scriptPath = Join-Path $NBADir ($script -replace '^\.\\', '')
    if (-not (Test-Path -LiteralPath $scriptPath)) {
        Write-Host "  skip $label (missing)" -ForegroundColor Yellow
        if ($label -eq "step6a") { Copy-Item $s6 $s6a -Force }
        elseif ($label -eq "step6b") { Copy-Item $s6a $s6b -Force }
        elseif ($label -eq "step6c") { Copy-Item $s6b $s6c -Force }
        elseif ($label -eq "step6d") { Copy-Item $s6c $s6d -Force }
        continue
    }
    try {
        Run-Step $label $script $a
    } catch {
        Write-Host "  WARN $label failed; carrying forward" -ForegroundColor Yellow
        if ($label -eq "step6a") { Copy-Item $s6 $s6a -Force }
        elseif ($label -eq "step6b") { Copy-Item $s6a $s6b -Force }
        elseif ($label -eq "step6c") { Copy-Item $s6b $s6c -Force }
        elseif ($label -eq "step6d") { Copy-Item $s6c $s6d -Force }
    }
}
$rankIn = $s6d
if (-not (Test-Path $rankIn)) { $rankIn = $s6 }
Run-Step "step7" ".\scripts\step7_rank_props.py" @("--input", $rankIn, "--output", $s7)
Run-Step "step8" ".\scripts\step8_add_direction_context.py" @(
    "--input", $s7, "--sheet", "ALL", "--output", $s8csv, "--xlsx", $s8xlsx, "--date", $Date
)

# Mirror into Sports/NBA/data/outputs and main_cp (skip locked Excel targets)
$legacyOut = Join-Path $NBADir "data\outputs"
foreach ($pair in @(
    @($s8csv, (Join-Path $legacyOut "step8_all_direction.csv")),
    @($s8xlsx, (Join-Path $legacyOut "step8_all_direction_clean.xlsx")),
    @($s8xlsx, (Join-Path $NBADir "step8_all_direction_clean.xlsx"))
)) {
    try {
        Copy-Item -LiteralPath $pair[0] -Destination $pair[1] -Force -ErrorAction Stop
    } catch {
        Write-Host "  WARN mirror skipped $($pair[1]): $_" -ForegroundColor Yellow
    }
}
# Root dated snapshot used by the ranker / combined (must match slate Game Date).
$rootDated = Join-Path $Root "outputs\$Date\step8_nba_direction_clean_$Date.xlsx"
Copy-Item -LiteralPath $s8xlsx -Destination $rootDated -Force

$mainCpRoot = Join-Path (Split-Path $Root -Parent) "PropORACLE_main_cp"
$mainCpOut = Join-Path $mainCpRoot "outputs\$Date\nba"
if (Test-Path (Split-Path $mainCpOut -Parent)) {
    New-Item -ItemType Directory -Force -Path $mainCpOut | Out-Null
    Copy-Item $s8csv (Join-Path $mainCpOut "step8_all_direction.csv") -Force
    Copy-Item $s8xlsx (Join-Path $mainCpOut "step8_all_direction_clean.xlsx") -Force
    Copy-Item $s8xlsx (Join-Path $mainCpOut "step8_all_direction_clean_$Date.xlsx") -Force
    Copy-Item $s8xlsx (Join-Path $mainCpRoot "outputs\$Date\step8_nba_direction_clean_$Date.xlsx") -Force
}
Write-Host "OK NBA step8 -> $s8xlsx" -ForegroundColor Green
