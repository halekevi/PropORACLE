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
| `nfl_box_ranks.csv` | same | PFR blitz + run-stop |
| `nfl_unit_rank_prop_map.md` | docs | prop → column map |

Refresh:

```powershell
py -3.14 Sports/NFL/scripts/build_nfl_unit_line_ranks.py --season 2026
```

## Still free, next to wire

| Priority | Layer | Source | Effort |
|----------|-------|--------|--------|
| 1 | Attach unit ranks onto step3/7 props | join CSVs | S |
| 2 | FTN box size by defense (pbp possession join) | `ftn_charting` + pbp | M |
| 3 | Coverage scheme Zone/Man/Mixed | NGS / FTN | M |
| 4 | Pace (plays/game, sec/play) | nflverse pbp / team stats | S |
| 5 | Referee crew tendencies | nflverse `officials` | S |
| 6 | Route participation | nflverse pbp_participation | M |
| 7 | Home/away prop splits | graded ledger | M |

## Paid — only if free stack plateaus

PFF grades, TruMedia alignment splits, proprietary pressure models.

## Week slate minimum

1. Keep step6b odds + weather on every slate  
2. Refresh snap cache Mondays  
3. Run `build_nfl_unit_line_ranks.py` after each week’s nflverse PFR drop  
4. Do not gate tickets on unit ranks until Week 2+ unique ledger validates
