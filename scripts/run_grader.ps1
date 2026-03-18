param(
    [string]$Date = (Get-Date -Format "yyyy-MM-dd")
)

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$DateDir = Join-Path $Root "outputs\$Date"

$TicketsFile = Join-Path $DateDir "combined_slate_tickets_$Date.xlsx"
$NBAActuals  = Join-Path $DateDir "actuals_nba_$Date.csv"
$CBBActuals  = Join-Path $DateDir "actuals_cbb_$Date.csv"

function Run-Py {
    param (
        [string]$Name,
        [string]$WorkingDir,
        [string]$ScriptPath,
        [array]$Args
    )

    Write-Host "`n=== Running $Name ===" -ForegroundColor Cyan

    if (-not (Test-Path $ScriptPath)) {
        Write-Host "  Script not found: $ScriptPath" -ForegroundColor Yellow
        return
    }

    Push-Location $WorkingDir

    try {
        # Try Python 3.14 launcher first
        $cmd = @("py", "-3.14", $ScriptPath) + $Args
        & $cmd[0] $cmd[1] $cmd[2..($cmd.Length-1)]

        if ($LASTEXITCODE -ne 0) {
            Write-Host "  py launcher failed, trying python..." -ForegroundColor Yellow
            $cmd = @("python", $ScriptPath) + $Args
            & $cmd[0] $cmd[1..($cmd.Length-1)]
        }
    }
    catch {
        Write-Host "  ERROR running ${Name}: $_" -ForegroundColor Red
    }

    Pop-Location
}

Write-Host "`n=====================================" -ForegroundColor Green
Write-Host "   SLATE IQ GRADER RUNNER" -ForegroundColor Green
Write-Host "   Date: $Date"
Write-Host "=====================================`n" -ForegroundColor Green

# =============================
# Resolve Combined Ticket Grader Path
# =============================
$CombinedTicketGrader = Join-Path $Root "combined_ticket_grader.py"

if (-not (Test-Path $CombinedTicketGrader)) {
    $CombinedTicketGrader = Join-Path $Root "scripts\combined_ticket_grader.py"
}

if (-not (Test-Path $CombinedTicketGrader)) {
    $CombinedTicketGrader = Join-Path $Root "scripts\grading\combined_ticket_grader.py"
}

# =============================
# Run Combined Ticket Grader
# =============================
if (-not (Test-Path $TicketsFile)) {
    Write-Host "Tickets file not found: $TicketsFile" -ForegroundColor Yellow
}
elseif (-not (Test-Path $NBAActuals)) {
    Write-Host "NBA actuals not found: $NBAActuals" -ForegroundColor Yellow
}
elseif (-not (Test-Path $CBBActuals)) {
    Write-Host "CBB actuals not found: $CBBActuals" -ForegroundColor Yellow
}
elseif (-not (Test-Path $CombinedTicketGrader)) {
    Write-Host "Combined ticket grader script not found!" -ForegroundColor Red
}
else {
    Run-Py "Combined Ticket Grader" $Root $CombinedTicketGrader @(
        "--tickets", $TicketsFile,
        "--nba_actuals", $NBAActuals,
        "--cbb_actuals", $CBBActuals,
        "--out", (Join-Path $DateDir "combined_tickets_graded_$Date.xlsx")
    )
}

Write-Host "`n✅ DONE." -ForegroundColor Green