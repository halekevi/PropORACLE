# Backfill graded workbooks from on-disk actuals + step8 (no re-fetch).
# Usage: pwsh -File scripts/backfill_grades_date_range.ps1 -Start 2026-10-01 -End 2026-10-22
param(
    [string]$Start = "2026-10-01",
    [string]$End = "2026-10-22",
    [string]$Root = ""
)
$ErrorActionPreference = "Continue"
if (-not $Root) {
    $Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$Root = (Resolve-Path $Root).Path
Set-Location $Root
Write-Host "BACKFILL grades $Start .. $End in $Root"

$SlateGrader = Join-Path $Root "scripts\grading\slate_grader.py"
$NhlGrader = Join-Path $Root "scripts\nhl_grader_advanced.py"
$SoccerGrader = Join-Path $Root "scripts\soccer_grader_advanced.py"
$TennisGrader = Join-Path $Root "Sports\Tennis\scripts\tennis_grader.py"
$JsonBackfill = Join-Path $Root "scripts\backfill_graded_props_json.py"
$Sanity = Join-Path $Root "scripts\grader_sanity_alerts.py"

function Resolve-FirstExisting([string[]]$Candidates) {
    foreach ($c in $Candidates) {
        if ($c -and (Test-Path -LiteralPath $c)) { return $c }
    }
    return $null
}

$cur = [datetime]::ParseExact($Start, "yyyy-MM-dd", $null)
$endDt = [datetime]::ParseExact($End, "yyyy-MM-dd", $null)
while ($cur -le $endDt) {
    $Date = $cur.ToString("yyyy-MM-dd")
    $DateDir = Join-Path $Root "outputs\$Date"
    Write-Host "`n======== $Date ========" -ForegroundColor Cyan
    if (-not (Test-Path $DateDir)) {
        Write-Host "  no outputs dir — skip"
        $cur = $cur.AddDays(1)
        continue
    }

    # NBA
    $nbaAct = Join-Path $DateDir "actuals_nba_$Date.csv"
    $nbaSlate = Resolve-FirstExisting @(
        (Join-Path $DateDir "step8_nba_direction_clean_$Date.xlsx"),
        (Join-Path $DateDir "nba\step8_nba_direction_clean.xlsx"),
        (Join-Path $Root "Sports\NBA\step8_all_direction_clean.xlsx")
    )
    $nbaOut = Join-Path $DateDir "graded_nba_$Date.xlsx"
    if ((Test-Path $nbaAct) -and $nbaSlate) {
        Write-Host "  Grade NBA <- $(Split-Path $nbaSlate -Leaf)"
        & py -3.14 -X utf8 $SlateGrader --sport NBA --slate $nbaSlate --actuals $nbaAct --output $nbaOut --date $Date
    } else {
        Write-Host "  Skip NBA (act=$([bool](Test-Path $nbaAct)) slate=$([bool]$nbaSlate))"
    }

    # CFB
    $cfbAct = Join-Path $DateDir "actuals_cfb_$Date.csv"
    $cfbSlate = Resolve-FirstExisting @(
        (Join-Path $DateDir "step8_cfb_direction_clean_$Date.xlsx"),
        (Join-Path $DateDir "cfb\step8_cfb_direction_clean.xlsx"),
        (Join-Path $Root "Sports\CFB\step8_cfb_direction_clean.xlsx")
    )
    $cfbOut = Join-Path $DateDir "graded_cfb_$Date.xlsx"
    if ((Test-Path $cfbAct) -and $cfbSlate) {
        Write-Host "  Grade CFB <- $(Split-Path $cfbSlate -Leaf)"
        & py -3.14 -X utf8 $SlateGrader --sport CFB --slate $cfbSlate --actuals $cfbAct --output $cfbOut --date $Date
    } else {
        Write-Host "  Skip CFB (act=$([bool](Test-Path $cfbAct)) slate=$([bool]$cfbSlate))"
    }

    # NHL
    $nhlAct = Join-Path $DateDir "actuals_nhl_$Date.csv"
    $nhlSlate = Resolve-FirstExisting @(
        (Join-Path $DateDir "step8_nhl_direction_clean_$Date.xlsx"),
        (Join-Path $DateDir "nhl\step8_nhl_direction_clean.xlsx"),
        (Join-Path $Root "Sports\NHL\step8_nhl_direction_clean.xlsx")
    )
    if ((Test-Path $nhlAct) -and $nhlSlate -and (Test-Path $NhlGrader)) {
        Write-Host "  Grade NHL <- $(Split-Path $nhlSlate -Leaf)"
        & py -3.14 -X utf8 $NhlGrader --date $Date --actuals $nhlAct --slate $nhlSlate --output-dir $DateDir
    } else {
        Write-Host "  Skip NHL"
    }

    # Soccer
    $socAct = Join-Path $DateDir "actuals_soccer_$Date.csv"
    $socSlate = Resolve-FirstExisting @(
        (Join-Path $DateDir "step8_soccer_direction_clean_$Date.xlsx"),
        (Join-Path $DateDir "soccer\step8_soccer_direction_clean.xlsx"),
        (Join-Path $Root "Sports\Soccer\step8_soccer_direction_clean.xlsx")
    )
    if ((Test-Path $socAct) -and $socSlate -and (Test-Path $SoccerGrader)) {
        Write-Host "  Grade Soccer <- $(Split-Path $socSlate -Leaf)"
        & py -3.14 -X utf8 $SoccerGrader --date $Date --actuals $socAct --slate $socSlate --output-dir $DateDir
    } else {
        Write-Host "  Skip Soccer"
    }

    # Tennis
    $tenSlate = Resolve-FirstExisting @(
        (Join-Path $DateDir "step8_tennis_direction_clean_$Date.xlsx"),
        (Join-Path $DateDir "tennis\step8_tennis_direction_clean.xlsx"),
        (Join-Path $Root "Sports\Tennis\step8_tennis_direction_clean.xlsx")
    )
    $tenOut = Join-Path $DateDir "graded_tennis_$Date.xlsx"
    if ($tenSlate -and (Test-Path $TennisGrader)) {
        Write-Host "  Grade Tennis <- $(Split-Path $tenSlate -Leaf)"
        & py -3.14 -X utf8 $TennisGrader --date $Date --slate $tenSlate --output $tenOut --days-back 2 --days-forward 1
    } else {
        Write-Host "  Skip Tennis"
    }

    if (Test-Path $JsonBackfill) {
        Write-Host "  Rebuild graded_props JSON"
        & py -3.14 -X utf8 $JsonBackfill --date $Date --json-only
    }
    if (Test-Path $Sanity) {
        & py -3.14 -X utf8 $Sanity --date $Date --root $Root --no-notify
    }

    $cur = $cur.AddDays(1)
}
Write-Host "`nBACKFILL DONE $Start .. $End" -ForegroundColor Green
