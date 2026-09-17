# NFL Pipeline

## Status: Live for NFL + NFLP (preseason)

PrizePicks **NFL (9)** and **NFLP (44)** use this same pipeline and the same
step8 workbook. Preseason is not a second sport — it is the NFL board with
`League=NFLP`. Defense ranks are the last completed regular season (2025
until 2026 Week 1 has a full table) and refresh on every pipeline run.

Target artifact: `outputs/<date>/nfl/step8_nfl_direction_clean.xlsx`
(also copied to `Sports/NFL/outputs/`). Columns include Def Tier, Def Rank,
Def Axis, Opp Pass/Rush/FG Def Rank, League, Off Identity (Pass/Rush/FG team),
and player share % of team (rush yards/attempts, rec yards/targets, pass yards).

## Key Differences from NBA/MLB

- Snap count % replaces minutes for role certainty
- Weather affects passing props (wind > 15mph = reduce)
- Game script matters: blowouts = RB heavy, passing game shrinks
- Bye weeks: no props posted for teams on bye
- Thursday/Monday games: smaller PrizePicks boards

## Phase 1 (Now): Scaffold

## Phase 2 — snap % cache (nflverse)

Public nflverse release CSVs (no auth) feed `Sports/NFL/data/nfl_snap_pct_cache.json`
(step4c also accepts a repo-root `data/nfl_snap_pct_cache.json` fallback):

```powershell
py -3.14 Sports\NFL\scripts\build_nfl_snap_pct_cache.py
# optional: --season 2025
```

Then step4c attaches `snap_pct_L3` / `snap_pct_season` → `minutes_tier` soft.
Before Sept, the builder prefers the prior completed season (2026 file is 404 until nflverse publishes).

## Phase 2 — weather / script / injuries / depth (wired)

Pipeline order after step4c:

- **step4d** ESPN injury report (`injury_status`, rank penalty; OUT/IR veto)
- **step6b** Odds API totals/spreads (`ODDS_API_KEY`) + ESPN scoreboard + Open-Meteo wind for outdoor games
- **depth charts** `build_nfl_depth_chart_cache.py` → `depth_slot` / `expected_snaps` on step4c
- **stat leaders / bottoms** from the 2025 boxscore cache (role floors, not zeros) → `nfl_stat_leaders.csv` + Matchup Edge team top/bottom 5 and league boards
- **Week 1 prop gate ledger** (`nfl_prop_gate_ledger_week1.csv`) — Standard UNDER-heavy; rebuild with `Sports/NFL/scripts/backfill_nfl_week1_prop_gates.py`
- **Standard tickets** prefer UNDER in this order until a Goblin sample exists:
  rec yards U → receptions U → longest reception U → sacks U → kick pts U →
  rush yards (bidirectional). **Pass yards gated out** both sides until Gate70
  sample builds (Week 1 U 42% / O 15%).

```powershell
py -3.14 scripts\pull_rosters.py --sport nfl
py -3.14 Sports\NFL\scripts\build_nfl_player_id_map.py
py -3.14 Sports\NFL\scripts\build_nfl_depth_chart_cache.py
py -3.14 scripts\build_team_share_json.py --sports nfl
py -3.14 Sports\NFL\scripts\build_nfl_stat_leaders.py
py -3.14 Sports\NFL\scripts\build_nfl_division_ranks.py
py -3.14 Sports\NFL\scripts\build_nfl_scoring_vehicles.py
py -3.14 Sports\NFL\scripts\backfill_nfl_box_volume.py
py -3.14 scripts\build_team_share_json.py --sports nfl
py -3.14 scripts\build_matchup_edge_json.py --sport nfl
```

Refresh those before Week 1. Game-day inactives still post ~90 min before kick — step4d is the IR/OUT list, not the inactive sheet.

## Phase 3 (June-July): Historical backfill 2024-2025

## Phase 4 (August): Preseason live testing

## Phase 5 (September): Production activation

### Running the full pipeline (dated outputs)

```powershell
$env:NFL_PIPELINE_ACTIVE = "1"
.\run_pipeline.ps1 -Sport NFL -SkipFetch -Date 2026-05-18
# or
.\scripts\run_nfl_pipeline.ps1 -Date 2026-05-18 -OutDir outputs\2026-05-18\nfl -SkipFetch
```

Target artifact: `outputs/<date>/nfl/step8_nfl_direction_clean.xlsx`

PrizePicks `league_id` for NFL is **9**. Step1 does not require `NFL_PIPELINE_ACTIVE`; steps 2+ require `NFL_PIPELINE_ACTIVE=1` (set automatically by the runners above).

First-time step1 fetch needs a Playwright browser profile (see `scripts/capture_entries.py`).

### Outputs (paths relative to `NFL/`)

| Step | Output |
|------|--------|
| 1 | `data/outputs/step1_pp_props_today.csv` |
| 2 | `data/outputs/step2_clean_props.csv` |
| 4 | `data/defense_rankings.csv` |
| 4b | `data/nfl_team_last5.csv` — each team’s last **5** completed regular-season games (ESPN scoreboards; PF/PA, W-L, opponents) |
| 4c | snap % + depth slot on step3 CSV |
| 4d | ESPN injuries on step3 CSV; `outputs/<date>/injuries_nfl_<date>.csv` |
| leaders | `data/nfl_stat_leaders.csv` — team/league ranks; step7/8 Team Rank, League Rank, Leader Slice |
| division | `data/nfl_team_unit_ranks.csv` — league + division O/D ranks; `nfl_divisional_tightness.json` |
| unit lines | `data/nfl_ol_ranks.csv` / `nfl_dl_ranks.csv` / `nfl_secondary_ranks.csv` / `nfl_box_ranks.csv` — free nflverse PFR+NGS+FTN; see `nfl_unit_rank_prop_map.md` + `nfl_free_context_build_order.md` |
| pace | `data/nfl_pace_ranks.csv` — plays/game, sec/play, no-huddle from pbp (`build_nfl_pace_ranks.py`) |
| route / WOPR | `data/nfl_route_participation.csv` — snap% + WOPR + target share (`build_nfl_route_participation.py`; see `nfl_route_participation_readme.md`) |
| vehicles | `data/nfl_scoring_vehicles.csv` — PASS/RUN/FG/BALANCED identity; `nfl_game_script_leans.json` |
| share | `data/nfl_player_prop_share.csv` + `nfl_team_share.json` — player % of team per prop |
| 6 | `data/outputs/step6_hit_rates.csv` |
| 6b | `data/outputs/step6b_with_game_context.csv` — totals, spread, wind, weather_flag |
| 8 (target) | `outputs/step8_nfl_direction_clean.xlsx` — same layout as **NHL** (`NHL/outputs/step8_…`), **not** a flat file under repo `outputs/`. Matches `NFL_SLATE` in `ui_runner/app.py`. |

### Web / `slate_latest.json`

- Combined `write_slate_json` emits the **`nfl`** key (lowercase) alongside other sports; `ui_runner` normalizes any legacy mixed-case keys when loading slate JSON.
- Ticket legs may show **`sport`: `"NFL"`** (uppercase); the home page maps that to panel key `nfl` and CSS `sp-nfl` via `.toLowerCase()`.
