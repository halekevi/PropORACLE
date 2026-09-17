# NFL route / target participation

Player-level volume context for receiving props (snap %, WOPR, target share).

## Why

Secondary matchup alone does not tell you whether a receiver will run enough
routes / see enough targets to clear a line. A 60% snap WR and a 90% snap WR
are different props regardless of CB rank.

## Artifacts

| File | Contents |
|------|----------|
| `nfl_route_participation.csv` | Per-player L3 + season aggregates |
| `nfl_route_participation_games.csv` | Per player-game rows |
| `nfl_route_participation_meta.json` | Build stamp + source notes |

## Build (Tuesday after grades)

```powershell
py -3.14 Sports/NFL/scripts/build_nfl_route_participation.py --season 2026 --week 2
```

Sources (free nflverse, no auth):

- `snap_counts_{season}.csv.gz` — offense snap %
- `stats_player_week_{season}.csv.gz` — `target_share`, `air_yards_share`, `wopr`
- `pbp_participation_{season}.csv` — true route / man-zone when published
  (2026 often missing early; builder falls back to snap % as `route_pct` proxy)

WOPR = `1.5 * target_share + 0.7 * air_yards_share` (nflverse definition).

## Name join

Uses `utils.nfl_player_names.norm_nfl_player_name`:

- `DJ Moore` <-> `D.J. Moore`
- `James Cook III` <-> `James Cook`
- periods / apostrophes / hyphens stripped

Snap cache (`build_nfl_snap_pct_cache.py`) and step4c use the same normalizer so
`Snap L3` actually fills on step8.

## Soft attach (live)

`utils.nfl_route_participation_gate.attach_participation()` joins onto step3:

- `snap_pct_L3` / `snap_pct_season` (fills gaps if step4c missed)
- `wopr_L3`, `target_share_L3`, `route_pct_L3`, ...

step8 surfaces: **Snap L3**, **WOPR L3**, **Tgt Share L3**, **Route % L3**.

## Hard gates (off by default)

`PARTICIPATION_HARD_GATES_ENABLED = False` in `utils/nfl_route_participation_gate.py`.

When flipped after Week 2+ ledger:

| Rule | Effect |
|------|--------|
| `snap_pct < 40%` | Suppress all props for that player |
| `wopr < 0.50` on receiving_yards OVER | Block |
| `target_share < 0.20` on receptions OVER | Block |
| Low WOPR + Elite secondary UNDER | Stacked lean (never blocks) |
| Unknown participation | Pass through |

Hook is already called from `utils.nfl_keep_gates.nfl_standard_ticket_eligible`
(no-op while the flag is False).

## Soft badges

`participation_soft_signals(row)` -> `SnapLow` / `WOPRHigh` / `LowWOPR+EliteSec` etc.
Use to **suppress** weak OVERs early season; do not open gated props from high
WOPR alone.
