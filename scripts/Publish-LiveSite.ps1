#requires -Version 5.1
<#
.SYNOPSIS
  Push live tickets/slate JSON to origin/main (Railway + GitHub raw) from the main worktree.

.NOTES
  Called after 8AM/9:45/10:30/1PM/4:30 refreshes so the site is not left on the 1AM board.
  Mirrors run_pipeline.ps1 Publish-LiveSiteJsonToMain.
#>
param(
    [string]$RepoRoot = "",
    [string]$CommitMessage = "",
    # Refuse (not just alert) when publish-path code is dirty / not on main.
    # Strict is now the default; disable with env PROPORACLE_PUBLISH_STRICT_TREE=0.
    [switch]$StrictTree,
    # Emergency bypass for the pre-publish tree check (alert is still written).
    [switch]$SkipTreeCheck
)

$ErrorActionPreference = "Continue"
# Scheduled publish must never hang on interactive git prompts (gc prune / stale worktree).
$env:GIT_TERMINAL_PROMPT = "0"
$env:GIT_ASK_YESNO = "false"
$Root = if ($RepoRoot) { $RepoRoot } else { Split-Path $PSScriptRoot -Parent }

# ---------------------------------------------------------------------------
# Failure alerting. 2026-10-20 -> 10-21: seven publishes failed (Goblin-70 build died on a
# missing module) and the only trace was a red line inside a task log.
#   logs\publish_alerts.log        append-only, one line per FAIL / WARN
#   logs\LAST_PUBLISH_STATUS.json  ok flag, reason, last_ok_at (point a watcher at this)
#   env PROPORACLE_ALERT_WEBHOOK   optional Discord/Slack/ntfy-style URL, JSON {"content","text"}
# ---------------------------------------------------------------------------
function Write-PublishAlert {
    param([string]$Level, [string]$Stage, [string]$Reason)
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    $logsDir = Join-Path $Root "logs"
    try {
        if (-not (Test-Path -LiteralPath $logsDir)) { New-Item -ItemType Directory -Path $logsDir -Force | Out-Null }
        Add-Content -LiteralPath (Join-Path $logsDir "publish_alerts.log") -Value ("{0} [{1}] {2}: {3}" -f $ts, $Level, $Stage, $Reason) -Encoding utf8
    } catch { }
    if ($Level -eq "FAIL") {
        $statusPath = Join-Path $logsDir "LAST_PUBLISH_STATUS.json"
        $lastOk = $null
        try {
            if (Test-Path -LiteralPath $statusPath) {
                $prev = Get-Content -LiteralPath $statusPath -Raw -Encoding utf8 | ConvertFrom-Json
                $lastOk = [string]$prev.last_ok_at
            }
        } catch { }
        try {
            ([ordered]@{ ok = $false; at = $ts; stage = $Stage; reason = $Reason; last_ok_at = $lastOk } |
                ConvertTo-Json) | Set-Content -LiteralPath $statusPath -Encoding utf8
        } catch { }
        # Task Scheduler launches do not always see a freshly setx'd variable, so fall back to
        # the persisted User-scope value.
        $hook = [string]$env:PROPORACLE_ALERT_WEBHOOK
        if (-not $hook) { $hook = [string][Environment]::GetEnvironmentVariable("PROPORACLE_ALERT_WEBHOOK", "User") }
        if ($hook) {
            try {
                $text = "PropORACLE LIVE PUBLISH FAILED ($Stage): $Reason"
                if ($hook -match '^https?://ntfy\.') {
                    # ntfy shows the raw body, so send plain text (JSON would be displayed verbatim).
                    Invoke-RestMethod -Uri $hook -Method Post -ContentType "text/plain; charset=utf-8" `
                        -Headers @{ Title = "PropORACLE publish FAILED"; Priority = "high"; Tags = "rotating_light" } `
                        -Body ([System.Text.Encoding]::UTF8.GetBytes($text)) -TimeoutSec 10 | Out-Null
                } else {
                    Invoke-RestMethod -Uri $hook -Method Post -ContentType "application/json" `
                        -Body (@{ content = $text; text = $text } | ConvertTo-Json) -TimeoutSec 10 | Out-Null
                }
            } catch { }
        }
    }
}

function Write-PublishOk {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    try {
        $logsDir = Join-Path $Root "logs"
        if (-not (Test-Path -LiteralPath $logsDir)) { New-Item -ItemType Directory -Path $logsDir -Force | Out-Null }
        ([ordered]@{ ok = $true; at = $ts; stage = "published"; reason = ""; last_ok_at = $ts } |
            ConvertTo-Json) | Set-Content -LiteralPath (Join-Path $logsDir "LAST_PUBLISH_STATUS.json") -Encoding utf8
    } catch { }
}

function Fail-Publish {
    param([string]$Stage, [string]$Reason)
    Write-Host ('[PUBLISH] FAILED ({0}): {1}' -f $Stage, $Reason) -ForegroundColor Red
    Write-PublishAlert -Level "FAIL" -Stage $Stage -Reason $Reason
    exit 1
}

function Get-MainWorktreeRoot {
    param([string]$RepoRoot = $Root)
    $porcelain = git -C $RepoRoot worktree list --porcelain 2>$null
    if (-not $porcelain) { return $RepoRoot }
    $wt = $null
    foreach ($line in $porcelain) {
        if ($line -match '^worktree (.+)$') { $wt = $Matches[1].Trim() }
        elseif ($line -match '^branch refs/heads/main$' -and $wt) { return $wt }
        elseif ($line -eq "") { $wt = $null }
    }
    $br = (git -C $RepoRoot rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
    if ($br -eq "main") { return $RepoRoot }
    return $null
}

function Get-TicketsCardKind {
    param([string]$TicketsPath)
    if (-not (Test-Path -LiteralPath $TicketsPath)) { return "missing" }
    try {
        $j = Get-Content -LiteralPath $TicketsPath -Raw -Encoding utf8 | ConvertFrom-Json
    } catch { return "missing" }
    $mode = [string]$j.mode
    if ($mode -match 'goblin70\+graded_main') { return "dual" }
    $hasG70 = $false
    $hasMixer = $false
    foreach ($g in @($j.groups)) {
        $n = [string]$g.group_name
        if ($n -match 'Goblin-70|YOLO Goblin') { $hasG70 = $true }
        else { $hasMixer = $true }
    }
    if ($hasG70 -and $hasMixer) { return "dual" }
    if ($hasG70) { return "goblin70_only" }
    if ($hasMixer) { return "mixer_only" }
    return "empty"
}

function Ensure-DualCardTickets {
    param([string]$RepoRoot)
    $tpl = Join-Path $RepoRoot "ui_runner\templates\tickets_latest.json"
    $rt = Join-Path $RepoRoot "ui_runner\runtime\tickets_latest.json"
    $kind = Get-TicketsCardKind -TicketsPath $tpl
    if ($kind -eq "dual") { return $true }

    Write-Host ('[PUBLISH] tickets card is ''{0}'' - rebuilding Goblin-70 dual card before push' -f $kind) -ForegroundColor Yellow
    $slateDate = (Get-Date).ToString("yyyy-MM-dd")
    try {
        $tz = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
        $et = [System.TimeZoneInfo]::ConvertTimeFromUtc((Get-Date).ToUniversalTime(), $tz)
        # After 8PM ET day-ahead owns tomorrow's slate.
        if ($et.Hour -ge 20) { $slateDate = $et.Date.AddDays(1).ToString("yyyy-MM-dd") }
        else { $slateDate = $et.ToString("yyyy-MM-dd") }
    } catch { }
    if (Test-Path -LiteralPath $tpl) {
        try {
            $j = Get-Content -LiteralPath $tpl -Raw -Encoding utf8 | ConvertFrom-Json
            $d = [string]$j.date
            if ($d -match '^\d{4}-\d{2}-\d{2}') { $slateDate = $d.Substring(0, 10) }
        } catch { }
    }
    $g70 = Join-Path $RepoRoot "scripts\build_goblin70_tickets.py"
    if (-not (Test-Path -LiteralPath $g70)) {
        Write-Host '[PUBLISH] FAILED: build_goblin70_tickets.py missing' -ForegroundColor Red
        return $false
    }
    $logsDir = Join-Path $RepoRoot "logs"
    if (-not (Test-Path -LiteralPath $logsDir)) {
        New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
    }
    $loggedHelper = Join-Path $RepoRoot "scripts\Invoke-LoggedPwsh.ps1"
    $g70Log = Join-Path $logsDir ("g70_writeweb_publish_{0}_{1:yyyyMMdd_HHmmss}.log" -f $slateDate, (Get-Date))
    $g70Exit = 1
    if (Test-Path -LiteralPath $loggedHelper) {
        . $loggedHelper
        $g70Exit = Invoke-G70WriteWebStandalone -ScriptPath $g70 -Date $slateDate -LogPath $g70Log -WorkingDirectory $RepoRoot
    }
    else {
        Write-Host '[PUBLISH] WARN: Invoke-LoggedPwsh.ps1 missing — falling back to Tee-Object for --write-web' -ForegroundColor Yellow
        & py -3.14 -u -X utf8 $g70 --date $slateDate --write-web 2>&1 | Tee-Object -FilePath $g70Log
        $g70Exit = $LASTEXITCODE
        Write-Host ("write-web exit code: {0}" -f $g70Exit)
    }
    if ($g70Exit -ne 0) {
        Write-Host ('[PUBLISH] FAILED: Goblin-70 rebuild exit {0}' -f $g70Exit) -ForegroundColor Red
        $tail = ""
        try { $tail = ((Get-Content -LiteralPath $g70Log -Tail 3 -ErrorAction Stop) -join ' | ').Trim() } catch { }
        $script:G70FailReason = ('Goblin-70 rebuild exit {0} for {1}; log {2}; tail: {3}' -f $g70Exit, $slateDate, $g70Log, $tail)
        return $false
    }
    if ((Test-Path -LiteralPath $tpl) -and (Test-Path -LiteralPath (Split-Path $rt -Parent))) {
        Copy-Item -LiteralPath $tpl -Destination $rt -Force -ErrorAction SilentlyContinue
    }
    $kind2 = Get-TicketsCardKind -TicketsPath $tpl
    if ($kind2 -ne "dual") {
        Write-Host ('[PUBLISH] FAILED: after rebuild card is still ''{0}'' (need Goblin-70 + mixer)' -f $kind2) -ForegroundColor Red
        return $false
    }
    Write-Host ('[PUBLISH] dual card OK ({0})' -f $slateDate) -ForegroundColor Green
    return $true
}

$MainRoot = Get-MainWorktreeRoot
if (-not $MainRoot) {
    Fail-Publish "worktree" "no worktree has main checked out"
}

Write-Host ('[PUBLISH] Live site JSON -> origin/main ({0})' -f $MainRoot) -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# Pre-publish tree check. The checkout is always dirty with data, so only the code the
# publish path runs is checked (scripts\check_publish_tree.py: import closure of the
# Goblin-70 builder + live guard). Missing local imports / syntax errors always refuse
# (the build would die anyway). Modified/untracked publish-path code and a non-main
# build checkout alert as WARN, and refuse under -StrictTree / PROPORACLE_PUBLISH_STRICT_TREE=1.
# ---------------------------------------------------------------------------
# Strict is the DEFAULT (decided 2026-10-21): a publish with modified/untracked publish-path code
# fails loudly instead of building a card from code main does not have. Opt out only on purpose:
# PROPORACLE_PUBLISH_STRICT_TREE=0 (or "false"/"no"), which downgrades dirty/branch findings to WARN.
$strictEnv = "$($env:PROPORACLE_PUBLISH_STRICT_TREE)".Trim().ToLowerInvariant()
$strict = $StrictTree.IsPresent -or ($strictEnv -notin @("0", "false", "no", "off"))
if ($SkipTreeCheck) {
    Write-PublishAlert -Level "WARN" -Stage "tree-check" -Reason "skipped via -SkipTreeCheck"
} else {
    $treePy = Join-Path $Root "scripts\check_publish_tree.py"
    if (Test-Path -LiteralPath $treePy) {
        $treeRaw = & py -3.14 -X utf8 $treePy --root $Root --json 2>&1 | Out-String
        $tree = $null
        try { $tree = $treeRaw | ConvertFrom-Json } catch { }
        if ($null -eq $tree) {
            Write-PublishAlert -Level "WARN" -Stage "tree-check" -Reason ("check_publish_tree.py gave no JSON: " + $treeRaw.Trim())
        } else {
            $missing = @($tree.missing)
            $dirty = @($tree.dirty)
            if ($missing.Count -gt 0) {
                Fail-Publish "tree-check" ("publish-path code has unresolved imports / syntax errors (build would fail): " + ($missing -join "; "))
            }
            if ($dirty.Count -gt 0) {
                $why = ("{0} publish-path code file(s) differ from git: {1}" -f $dirty.Count, (($dirty | ForEach-Object { $_.Trim() }) -join "; "))
                if ($strict) { Fail-Publish "tree-check" $why }
                Write-Host ('[PUBLISH] WARN: ' + $why) -ForegroundColor Yellow
                Write-PublishAlert -Level "WARN" -Stage "tree-check" -Reason $why
            }
        }
    }
    $buildBranch = (git -C $Root rev-parse --abbrev-ref HEAD 2>$null | Out-String).Trim()
    if ($buildBranch -and $buildBranch -ne "main") {
        $why = ("building from checkout '{0}' on branch '{1}', not main (its code is what the card is built with)" -f $Root, $buildBranch)
        if ($strict) { Fail-Publish "branch-check" $why }
        Write-Host ('[PUBLISH] WARN: ' + $why) -ForegroundColor Yellow
        Write-PublishAlert -Level "WARN" -Stage "branch-check" -Reason $why
    }
}

# Hard gate: never push mixer-only / Goblin-only to Railway.
if (-not (Ensure-DualCardTickets -RepoRoot $Root)) {
    $r = if ($script:G70FailReason) { $script:G70FailReason } else { "Goblin-70 dual card could not be built (see [PUBLISH] lines above)" }
    Fail-Publish "dual-card" $r
}

$assertPy = Join-Path $Root "scripts\assert_live_publish.py"
if (-not (Test-Path -LiteralPath $assertPy)) {
    # Scheduled jobs run from main_cp; keep a copy of the guard there.
    $srcAssert = Join-Path (Split-Path $PSScriptRoot -Parent) "scripts\assert_live_publish.py"
    if ($srcAssert -ne $assertPy -and (Test-Path -LiteralPath $srcAssert)) {
        Copy-Item -LiteralPath $srcAssert -Destination $assertPy -Force -ErrorAction SilentlyContinue
    }
}
if (Test-Path -LiteralPath $assertPy) {
    Write-Host '[PUBLISH] assert dual-card + runtime/templates sync' -ForegroundColor DarkGray
    & py -3.14 $assertPy --root $Root --fix
    if ($LASTEXITCODE -ne 0) {
        Fail-Publish "assert-live" "live JSON guard failed (Goblin-70+mixer, matching dates); see assert_live_publish.py output above"
    }
} else {
    $kind = Get-TicketsCardKind -TicketsPath (Join-Path $Root "ui_runner\templates\tickets_latest.json")
    if ($kind -ne "dual") {
        Fail-Publish "assert-live" ("assert_live_publish.py missing and card is '{0}'" -f $kind)
    }
    Write-Host '[PUBLISH] dual-card PowerShell guard OK (assert script missing)' -ForegroundColor DarkGray
}

# templates/ = GitHub raw contract (Railway). runtime/ = canonical disk copy.
# mobile/www is not live (Android loads Railway remotely).
$liveRel = @(
    "ui_runner/runtime/tickets_latest.json",
    "ui_runner/templates/tickets_latest.json",
    "ui_runner/runtime/slate_latest.json",
    "ui_runner/templates/slate_latest.json",
    "ui_runner/runtime/slate_display_date.json",
    "ui_runner/templates/slate_display_date.json",
    "ui_runner/runtime/pipeline_status.json",
    "ui_runner/templates/pipeline_status.json",
    "ui_runner/runtime/tickets_winrate_latest.json",
    "ui_runner/templates/tickets_winrate_latest.json",
    "ui_runner/runtime/sport_breakdown.json",
    "ui_runner/templates/sport_breakdown.json",
    "ui_runner/runtime/last_fetch_window.json",
    "ui_runner/templates/last_fetch_window.json",
    "data/reports/bet_windows_latest.json"
)
Get-ChildItem -LiteralPath (Join-Path $Root "ui_runner\runtime") -Filter "slate_sport_*.json" -ErrorAction SilentlyContinue |
    ForEach-Object { $liveRel += ("ui_runner/runtime/" + $_.Name) }
Get-ChildItem -LiteralPath (Join-Path $Root "ui_runner\templates") -Filter "slate_sport_*.json" -ErrorAction SilentlyContinue |
    ForEach-Object { $liveRel += ("ui_runner/templates/" + $_.Name) }

$toPublish = @()
foreach ($rel in $liveRel) {
    $full = Join-Path $Root ($rel -replace "/", "\")
    if (Test-Path -LiteralPath $full) { $toPublish += $rel }
}
if (-not $toPublish.Count) {
    Write-Host '[PUBLISH] No live site JSON found' -ForegroundColor Yellow
    exit 0
}

Push-Location $MainRoot
try {
    git pull --ff-only origin main 2>&1 | ForEach-Object { Write-Host "    $_" -ForegroundColor DarkGray }
    foreach ($rel in $toPublish) {
        $src = Join-Path $Root ($rel -replace "/", "\")
        $dst = Join-Path $MainRoot ($rel -replace "/", "\")
        $samePath = $false
        try {
            if ((Test-Path -LiteralPath $src) -and (Test-Path -LiteralPath $dst)) {
                $samePath = (
                    [IO.Path]::GetFullPath($src).TrimEnd('\') -eq
                    [IO.Path]::GetFullPath($dst).TrimEnd('\')
                )
            }
        } catch { $samePath = ($src -eq $dst) }
        if (-not $samePath) {
            $dstDir = Split-Path $dst -Parent
            if (-not (Test-Path -LiteralPath $dstDir)) {
                New-Item -ItemType Directory -Path $dstDir -Force | Out-Null
            }
            Copy-Item -LiteralPath $src -Destination $dst -Force
        }
        git add -- $rel 2>&1 | Out-Null
    }
    $msg = if ($CommitMessage) { $CommitMessage } else { "chore: live tickets/slate $(Get-Date -Format 'yyyy-MM-dd HH:mm')" }
    # Pathspec: commit ONLY the live JSON staged above. A bare commit sweeps in anything another
    # process has staged in this checkout (that is how Invoke-LoggedPwsh.ps1 got deleted on 2026-09-10).
    git commit -m $msg -- $toPublish 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        $pushOut = git push origin main 2>&1
        foreach ($line in $pushOut) { Write-Host "    $line" -ForegroundColor DarkGray }
        if ($LASTEXITCODE -eq 0) {
            Write-Host '[PUBLISH] OK - origin/main updated' -ForegroundColor Green
            Write-PublishOk
            exit 0
        }
        Fail-Publish "git-push" ("git push origin main failed: " + (($pushOut | Out-String).Trim()))
    }
    Write-Host '[PUBLISH] no JSON changes vs main' -ForegroundColor DarkGray
    Write-PublishOk
    exit 0
} finally {
    Pop-Location
}
