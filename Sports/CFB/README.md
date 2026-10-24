# College Football (CFB) Pipeline

Mirrors the **CBB** layout: PrizePicks fetch → normalize → defense → ESPN IDs → boxscore stats → rank (step6 xlsx). Outputs land under `outputs/<date>/cfb/` and are picked up automatically by `combined_slate_tickets.py`.

## PrizePicks

- **League ID:** `15` (CFB full game) and **`184` (CFB1H)**
- Full-game step 1: `scripts/pipeline/step1_pp_cfb_scraper.py --board cfb`
- First-half step 1: `scripts/pipeline/step1_pp_cfb_scraper.py --board cfb1h`

CFB1H is a **separate** pipeline. Outputs land under `outputs/<date>/cfb1h/` and L5 is **Q1+Q2 ESPN PBP only** — never full-game box yards. Combined / Goblin-70 / the ranker keep `CFB1H` distinct from `CFB`.

## Run

From repo root:

```powershell
.\run_pipeline.ps1 -CFBOnly
.\run_pipeline.ps1 -CFBOnly -SkipFetch   # reuse existing step1 CSV
```

`-CFBOnly` and the parallel CFB job both fetch CFB then CFB1H. An empty 1H board skips remaining 1H steps and does not fail full-game CFB. Full parallel run includes CFB when the slate date is in **Aug–Jan** (off-season skipped Feb–Jul).

## Outputs

| Step | Full game (`cfb/`) | First half (`cfb1h/`) |
|------|--------------------|------------------------|
| 1 | `step1_cfb.csv` | `step1_cfb1h.csv` |
| 2 | `step2_cfb.csv` | `step2_cfb1h.csv` |
| 3 | `step3_with_unit_rankings_cfb.csv` | `step3_with_unit_rankings_cfb1h.csv` |
| 4 | `step3_cfb.csv` | `step3_cfb1h.csv` |
| 5 | `step5b_cfb.csv` (full-game boxes) | `step5b_cfb1h.csv` (1H PBP) |
| 6 | `step6_ranked_cfb.xlsx` | `step6_ranked_cfb1h.xlsx` |
| 8 | `step8_cfb_direction_clean.xlsx` | `step8_cfb1h_direction_clean.xlsx` |

## Data files

- `data/reference/ncaa_football_athletes_master.csv` — bootstrap ESPN athlete map (grows over time)
- `data/reference/cfb_def_rankings.csv` — team defense ranks (placeholder from CBB template until CFB-specific table is built)
- `data/cache/cfb_boxscore_cache.csv` — ESPN college-football game logs (populate via backfill + step5b)
- `data/cache/cfb1h_boxscore_cache.csv` — first-half PBP rows only (never copy full-game yards)
- `data/reference/cfb_player_volume.csv` — per-player rush att / pass att / rec target averages and team share (from CAR, C/ATT, and PBP targets). Rebuild: `py -3.14 Sports/CFB/scripts/pipeline/build_cfb_player_volume.py --refetch-missing` fills every cached team, not just today's slate.

## Rolling stats (L5 / L10) — backfill prior season

CFB players only play ~12 games per year, so week 1–4 slates need **last season’s games** in the cache for full L5/L10. Step 5b `--cache-only` attaches from that tape (player TDs, stacked yards, longest, attempts, kick) without waiting for 2026 boxscores. Do **not** fall back to receptions/attempts as a fake longest.

**One-time (or preseason) backfill** from repo root:

```powershell
# 2025 regular season + bowls
python scripts/backfill_cfb_espn_range.py --preset full-2025 --season 2025

# Prior year (for early fall 2026 slates)
python scripts/backfill_cfb_espn_range.py --preset full-2024 --season 2024
```

Other presets: `regular-2025`, `bowls-2025`. Custom range: `--from 2025-08-23 --to 2026-01-19 --season 2025`.

Then run the pipeline (`-CFBOnly` or full parallel). Step 5 scans **200 days** back from `--date` and reads the shared cache.

## Regular season unit rankings (national)

Pass/rush/receiving **offense** and **defense allowed**, plus TDs, FG, PAT, kick
points, INTs, sacks, tackles, and TFL, are ranked **nationally (FBS-wide)** and
**within each conference**. Quintile tiers (Elite → Weak) use the pool size:
136 for national, conference size (e.g. 18 Big Ten) for conference ranks.
Gold / D still uses **national** opponent D. Conference rank is extra context
(`team_pass_off_rank_conf`, `opp_pass_def_rank_conf`, …).

Printed O/D board (NFL-shaped, percentile tiers, slate-only rows):
`data/cfb_od_rankings_week{N}.md` from
`py -3.14 Sports/CFB/scripts/build_cfb_unit_line_ranks.py`.
OL/DL/secondary numbers are ranks of existing `cfb_line_secondary.csv` indices
(empty until `CFBD_API_KEY`). Do not treat DL tier as the Sacks UNDER explanation.

That markdown is the **live weekly ranking board** (step8 pairings). HIT/MISS,
margin×D, and the Pass Yards veto cell live on `graded_props_*.json`. When the
CFBD composite fills, join it to the graded tape to audit — do not read
percentile tiers as hit rates. See `data/cfb_unit_rank_prop_map.md`.

| Unit | Rank 1 means |
|------|----------------|
| Pass / rush / rec / points / TD / FG / PAT / kick pts **offense** | Most per game |
| Pass / rush / points / TD / FG / PAT / kick pts **defense** | Fewest allowed per game |
| INT thrown / sacks taken / offensive plays | Fewest (ball security / OL / low volume) |
| INT forced (defense) | Fewest picks (harder QB INT overs) |
| Sacks / tackles / TFL **produced** | Most per game (context; sack *props* use opponent sacks-taken) |
| Rush rate / TOP | Run share and time of possession (clock-kill script) |

**Clock / time of possession.** Rankings store `off_rush_rate` (rush att / pass+rush att)
and `off_top_sec_pg` (ESPN `possessionTimeSeconds` / games) plus `off_clock_tier`
(Run-heavy clock → Pass-heavy). Step 3 attaches `team_` / `opp_` copies. Step 6
multiplies the rank score: run-heavy teams suppress their own pass/rec **overs**
and boost rush **overs**; an opponent that sits on the ball cuts *your* snaps
(pass/rec/rush overs down). Next `build_cfb_unit_rankings.py` refresh fills TOP;
rush rate is derived from attempts already on the CSV.

**Build / refresh** (ESPN regular-season team stats):

```powershell
cd Sports\CFB
py -3.14 scripts\build_cfb_unit_rankings.py --season 2025
```

Output: `data/reference/cfb_team_unit_rankings.csv`

The pipeline runs this automatically as **CFB Step 3a**, then **Step 3b** attaches
ranks/tiers and prop-aware `OVERALL_DEF_RANK` / `def_tier`:

- Pass / rec yards → opponent pass D
- Rush yards → opponent rush D
- Pass / rush / rec TDs → matching TD D allowed
- Player TDs → total TD D allowed
- FG / PAT / kick points → matching kick D allowed
- INT → opponent INT-forced rank (1 = fewest)
- Sacks → opponent sacks-taken (OL)
- Tackles → opponent offensive plays

## Postseason (playoffs / CFP)

CFB does not use March Madness, but the pipeline treats **College Football Playoff + bowl season** like CBB tournament context:

- Metadata: `utils/cfb_playoff_metadata.py` — edit `CFB_CFP_2026` when the 12-team bracket is set (seeds + round: `CFP_BYE`, `CFP_FIRST`, `CFP_QUARTER`, `CFP_SEMI`, `CFP_CHAMP`)
- Step 6 sets `is_playoff_game`, `playoff_round`, and slightly dampens yard/TD props in playoff games (tighter scripts)
- Combined slate / UI: `is_tournament_game` mirrors `is_playoff_game` for parity with CBB; columns `team_seed`, `opp_seed`, `playoff_round`

## Notes

- Gap catalog (what a complete per-prop record requires vs what exists): `Sports/CFB/GAP_CATALOG.md`
- ESPN sport path: `football/college-football`
- Combined slate keys: `cfb` and `cfb1h` in `slate_latest.json` (labels `CFB`, `CFB1H`)
- CFB1H cache: `data/cache/cfb1h_boxscore_cache.csv` (SQLite sport key `cfb1h`). First fill seeds event ids from the full-game cache, then re-parses ESPN summaries for periods 1–2 only.
- Replace `cfb_def_rankings.csv` with real FBS defensive efficiency when available for better opp_def tiers
