#requires -Version 5.1
<#
.SYNOPSIS
  Push graded_props_<date>.json exports to the data-only `graded-data` branch (no worktree checkout).

.DESCRIPTION
  Replaces the old `git add -f ui_runner/templates/graded_props_<date>.json` on main. Uses git
  plumbing against a temporary index so the 1.1 GB branch is never checked out:

    origin/graded-data tree  +  blob(s) for the given dates  +  refreshed index.json
    -> commit-tree -> update-ref -> push origin graded-data

  Reads the exports from <RepoRoot>\ui_runner\templates (where the grader writes them). Idempotent:
  a date whose blob already matches is skipped; nothing is pushed if nothing changed.

.EXAMPLE
  .\scripts\Publish-GradedData.ps1 -Date 2026-10-23
  .\scripts\Publish-GradedData.ps1 -Date 2026-10-22,2026-10-23 -RepoRoot H:\PropORACLE_main_cp
  .\scripts\Publish-GradedData.ps1 -All        # every graded_props_*.json on disk (regrade sweep)
#>
param(
    [string[]]$Date = @(),
    [switch]$All,
    [string]$RepoRoot = "",
    [string]$Branch = "graded-data",
    [string]$Remote = "origin"
)

$ErrorActionPreference = "Stop"
$env:GIT_TERMINAL_PROMPT = "0"
$Root = if ($RepoRoot) { $RepoRoot } else { Split-Path $PSScriptRoot -Parent }
$Tpl = Join-Path $Root "ui_runner\templates"

function Write-Status([string]$msg, [string]$color = "Gray") { Write-Host ("[graded-data] " + $msg) -ForegroundColor $color }

$files = @()
if ($All) {
    $files = Get-ChildItem -LiteralPath $Tpl -Filter "graded_props_*.json" | Where-Object { $_.Name -match '^graded_props_\d{4}-\d{2}-\d{2}\.json$' }
} else {
    foreach ($d in $Date) {
        if ($d -notmatch '^\d{4}-\d{2}-\d{2}$') { Write-Status "skip '$d' (not YYYY-MM-DD)" "Yellow"; continue }
        $p = Join-Path $Tpl ("graded_props_{0}.json" -f $d)
        if (Test-Path -LiteralPath $p) { $files += Get-Item -LiteralPath $p } else { Write-Status "no export for $d at $p" "Yellow" }
    }
}
if (-not $files.Count) { Write-Status "nothing to publish"; exit 0 }

Push-Location $Root
$tmpIndex = Join-Path $env:TEMP ("graded_data_index_{0}.idx" -f [guid]::NewGuid().ToString("N"))
$tmpJson = Join-Path $env:TEMP ("graded_data_index_{0}.json" -f [guid]::NewGuid().ToString("N"))
try {
    git fetch -q $Remote $Branch 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "git fetch $Remote $Branch failed" }
    $parent = (git rev-parse --verify ("{0}/{1}^{{commit}}" -f $Remote, $Branch) 2>$null | Out-String).Trim()
    if (-not $parent) { throw "$Remote/$Branch not found - create the branch first (see graded-data README)" }

    $env:GIT_INDEX_FILE = $tmpIndex
    git read-tree $parent 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "git read-tree $parent failed" }

    $changed = @()
    foreach ($f in $files) {
        $rel = "graded_props/" + $f.Name
        $existing = (git rev-parse --verify ("{0}:{1}" -f $parent, $rel) 2>$null | Out-String).Trim()
        $blob = (git hash-object -w -- $f.FullName | Out-String).Trim()
        if (-not $blob) { throw "hash-object failed for $($f.FullName)" }
        if ($existing -eq $blob) { continue }
        git update-index --add --cacheinfo ("100644,{0},{1}" -f $blob, $rel) 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "update-index failed for $rel" }
        $changed += $f.Name
    }
    if (-not $changed.Count) {
        Write-Status ("{0} file(s) already current on {1}/{2}" -f $files.Count, $Remote, $Branch)
        exit 0
    }

    # Refresh index.json from the staged tree.
    $dates = git ls-files -- "graded_props/" | ForEach-Object {
        if ($_ -match 'graded_props_(\d{4}-\d{2}-\d{2})\.json$') { $Matches[1] }
    } | Sort-Object -Unique
    $index = [ordered]@{
        generated_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss UTC")
        layout       = "graded_props/graded_props_<YYYY-MM-DD>.json"
        raw_base     = "https://raw.githubusercontent.com/halekevi/PropORACLE/$Branch/graded_props"
        dates        = @($dates)
        n            = @($dates).Count
    }
    ($index | ConvertTo-Json -Depth 4) | Set-Content -LiteralPath $tmpJson -Encoding utf8
    $idxBlob = (git hash-object -w -- $tmpJson | Out-String).Trim()
    git update-index --add --cacheinfo ("100644,{0},index.json" -f $idxBlob) 2>&1 | Out-Null

    $tree = (git write-tree | Out-String).Trim()
    if (-not $tree) { throw "write-tree failed" }
    $msg = "graded-data: {0} ({1} file(s))" -f (($changed | ForEach-Object { $_ -replace '^graded_props_|\.json$', '' }) -join ", "), $changed.Count
    $commit = (git commit-tree $tree -p $parent -m $msg | Out-String).Trim()
    if (-not $commit) { throw "commit-tree failed" }
    Remove-Item Env:\GIT_INDEX_FILE -ErrorAction SilentlyContinue

    git update-ref ("refs/heads/{0}" -f $Branch) $commit $parent 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        # Local branch ref may not exist / may have diverged; push the commit object directly.
        $pushSpec = "{0}:refs/heads/{1}" -f $commit, $Branch
    } else {
        $pushSpec = "refs/heads/{0}:refs/heads/{0}" -f $Branch
    }
    $out = git push $Remote $pushSpec 2>&1
    if ($LASTEXITCODE -ne 0) { throw ("git push failed: " + ($out | Out-String).Trim()) }
    Write-Status ("pushed {0} -> {1}/{2}: {3}" -f $commit.Substring(0, 9), $Remote, $Branch, $msg) "Green"
    exit 0
} catch {
    Write-Status ("FAILED: " + $_.Exception.Message) "Red"
    try {
        $logs = Join-Path $Root "logs"
        if (-not (Test-Path -LiteralPath $logs)) { New-Item -ItemType Directory -Path $logs -Force | Out-Null }
        Add-Content -LiteralPath (Join-Path $logs "publish_alerts.log") -Value ("{0} [WARN] graded-data: {1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $_.Exception.Message) -Encoding utf8
    } catch { }
    exit 1
} finally {
    Remove-Item Env:\GIT_INDEX_FILE -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tmpIndex -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $tmpJson -ErrorAction SilentlyContinue
    Pop-Location
}
