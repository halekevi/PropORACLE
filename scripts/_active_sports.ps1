# Active / inactive boards from config/active_sports.json, which scripts/sync_active_sports.py
# keeps in step with the ESPN season calendar (off the day after the championship, back the day
# before the first preseason or regular-season game). PROPORACLE_FORCE_ALL_SPORTS=1 turns every
# board on. If the config cannot be read, callers fall back to their own date gates.
$script:ActiveSportsInactive = $null

function Read-ActiveSportsConfig {
    param([string]$RepoRoot)
    $cfgPath = Join-Path $RepoRoot "config\active_sports.json"
    try {
        $cfg = Get-Content -LiteralPath $cfgPath -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop
        $script:ActiveSportsInactive = @($cfg.inactive | ForEach-Object { "$($_.key)".Trim().ToUpperInvariant() } | Where-Object { $_ })
    }
    catch {
        $script:ActiveSportsInactive = $null
    }
}

function Test-ActiveSportsConfigLoaded {
    return ($null -ne $script:ActiveSportsInactive)
}

function Test-SportOn {
    param([string]$Key, [bool]$Fallback = $true)
    if ("$($env:PROPORACLE_FORCE_ALL_SPORTS)" -eq "1") { return $true }
    if ($null -eq $script:ActiveSportsInactive) { return $Fallback }
    return -not ($script:ActiveSportsInactive -contains $Key.Trim().ToUpperInvariant())
}
