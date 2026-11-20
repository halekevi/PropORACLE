# NBA master graded props

Built: 2026-11-18T20:42:55-05:00
File: `master_graded_2026-11-18_README.md`

## Row identity
One row per `(player, prop, line, pick_type, side, game_date)`.
Goblin/Demon: OVER only. Standard: OVER and UNDER.

## Leakage rule
All L5/L10/L20/season/minutes/starter features use only box scores with `game_date` **strictly before** the prop's `game_date`.

## Columns
| column | meaning |
|---|---|
| game_date | slate / game date (ET) |
| player, team, opp_team | board identity |
| prop, pick_type, side, line | market face |
| projection_id | PrizePicks projection id (one Goblin/Demon rung) |
| first_line, last_line, n_fetches, line_move | line_history joined on projection_id only |
| actual, hit, push, void, void_reason, result | grade |
| l5_hits / l5_n (and L10/L20) | directional clears of **this** line in last N decided games |
| l5_avg, l10_avg, gap, gap_sd | mean vs line; gap = l5_avg - line; gap_sd = sd(values - line) on L5 |
| season_hits, season_n, season_hit_rate | same-season games before prop day |
| minutes_l5_avg, starter_l5_rate | role proxies |
| spread, total, favorite, home_away | from game_lines.db (Favorite derived from home spread) |
| opp_team | board opp, else backfilled from box home/away |
| opp_def_rank, opp_def_tier, opp_pts_allowed_l10 | day-as-of L10 pts allowed; rank 1–30 NBA only |
| team_pace_l10, pace_flag | day-as-of L10 pace proxy; Fast/Avg/Slow vs same-day tertiles |
| line_moved | 1 if abs(line_move)>0 (null line_move → 0) |
| source, season_tag | provenance |

## Counts
- total rows: **315,911**

### by season_tag × pick_type
- 2025-26_late / Demon: 17,687
- 2025-26_late / Goblin: 57,517
- 2025-26_late / Standard: 33,631
- 2026-27 / Demon: 88,777
- 2026-27 / Goblin: 55,913
- 2026-27 / Standard: 62,386

### by side
- OVER: 269,972
- UNDER: 45,939

## Notes / missing features
- NBA-only first pass.
- pace_flag = Fast/Avg/Slow from team L10 pace_proxy (team_pts+opp_pts) vs same-day tertiles; games before prop day only.
- opp_def_rank / opp_pts_allowed_l10 = day-as-of L10 points allowed by opponent before prop day (1=stingiest, max 30 NBA teams).
- Team-game pairing is (game_date, home, away); exhibition/foreign codes excluded from ranks.
- opp_team backfilled from box home/away when board opp was blank/dash.
- line_move first/last are per projection_id (one Goblin/Demon rung); never across rungs. Unmatched → null.
- line_history exists mainly for 2026-27; export-era dates usually have null line_move (no PP fetch history).
- props with no box-stat map skipped in feature windows: 3087 rows still kept with null L-windows.
- Injury board status not attached this pass (void/DNP from missing minutes only on season boards).
- Static nba_defense_by_stat.csv is no longer used for opp_def_rank.
