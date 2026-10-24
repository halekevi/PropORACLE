# NFL free context layers — build order

Maximize **free** signal before any paid feed (PFF / TruMedia).

## Already wired in PropORACLE

| Layer | Where | Cost |
|-------|-------|------|
| Implied totals / spread / game total | `step6b_attach_game_context_nfl.py` + Odds API | Free tier key |
| Weather (wind/temp/precip, dome skip) | `utils/nfl_espn_context.py` Open-Meteo | Free, no key |
| Snap % / role | `build_nfl_snap_pct_cache.py` nflverse | Free |
| Pass/rush team O/D ranks | `nfl_team_unit_ranks.csv` | Free (ESPN + box) |
| Coarse def tiers | `defense_rankings.csv` + `nfl_prop_defense` | Free |
| Game script leans | `nfl_game_script_leans.json` | Free |
| Depth / injuries | step4c / step4d | Free (ESPN) |

## New this pass — unit lines (OL / DL / secondary / box)

| Artifact | Builder | Source |
|----------|---------|--------|
| `nfl_ol_ranks.csv` | `build_nfl_unit_line_ranks.py` | nflverse PFR adv + team stats |
| `nfl_dl_ranks.csv` | same | PFR def + opp rush YBC |
| `nfl_secondary_ranks.csv` | same | PFR def coverage + NGS separation |
| `nfl_box_ranks.csv` | same | PFR blitz + FTN×pbp box |
| `nfl_pace_ranks.csv` | `build_nfl_pace_ranks.py` | nflverse pbp tempo |
| `nfl_unit_rank_prop_map.md` | docs | prop → column map |

Refresh:

```powershell
py -3.14 Sports/NFL/scripts/build_nfl_unit_line_ranks.py --season 2026
py -3.14 Sports/NFL/scripts/build_nfl_pace_ranks.py --season 2026
py -3.14 Sports/NFL/scripts/build_nfl_route_participation.py --season 2026 --week 2
py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py --season 2026
```

## Still free, next to wire

| Priority | Layer | Source | Effort |
|----------|-------|--------|--------|
| 1 | Attach unit ranks onto step3/7 props | **done** step3 | — |
| 2 | FTN×pbp box + contested scheme lean | **done in builder** | — |
| 3 | Pace (plays/game, sec/play) | **done** `build_nfl_pace_ranks.py` | — |
| 4 | QB mobility (Pocket/Mixed/Mobile) | **done** `build_nfl_qb_mobility.py` → step3 soft | — |
| 5 | Red-zone team/player rates | **done** `build_nfl_red_zone_rates.py` → step3 soft | — |
| 6 | True Zone/Man charting (if free source appears) | — | M |
| 7 | Referee crew tendencies | nflverse `officials` | S |
| 8 | Route participation | **done** `build_nfl_route_participation.py` | — |
| 9 | Home/away prop splits | graded ledger | M |
| 10 | Kicker distance accuracy / punt return rate | PFR + PBP | M |

Refresh (Tuesday)::

```powershell
py -3.14 Sports/NFL/scripts/build_nfl_unit_line_ranks.py --season 2026
py -3.14 Sports/NFL/scripts/build_nfl_pace_ranks.py --season 2026
py -3.14 Sports/NFL/scripts/build_nfl_route_participation.py --season 2026 --week 2
py -3.14 Sports/NFL/scripts/build_nfl_qb_mobility.py --seasons 2025,2026
py -3.14 Sports/NFL/scripts/build_nfl_red_zone_rates.py --seasons 2025,2026
py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py --season 2026
```

## Paid — only if free stack plateaus

PFF grades, TruMedia alignment splits, proprietary pressure models.

## Week slate minimum

1. Keep step6b odds + weather on every slate (ESPN fallback when Odds API credits=0)
2. Refresh snap cache Mondays
3. Run unit-line + pace + route + QB mobility + red-zone builders after each week nflverse drop
4. Do not gate tickets on unit/pace/mobility/RZ until unique-game ledger validates
