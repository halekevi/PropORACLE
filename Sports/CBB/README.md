# College Basketball (CBB) + Women's CBB (WCBB)

PrizePicks boards: **NCAABB Mens** and **NCAABB Women**.

## Season window (2026-27)

| | Date |
|---|---|
| 2025-26 last expected slate | Men's 2026-04-06 · Women's 2026-04-05 |
| Off-season | 2026-04-07 through 2026-10-31 |
| **Resume / DI tipoff** | **2026-11-01** |

Full parallel runs skip CBB/WCBB until resume. Force a dry run:

```powershell
.\run_pipeline.ps1 -CBBOnly -ForceCBB
```

Override resume: `$env:PROPORACLE_CBB_RESUME = "2026-10-28"`.

## PrizePicks league IDs

See `prizepicks_league_ids.py`. Step 1 resolves live `/leagues` names, then falls back:

| Board | Fallback id | PP names seen / expected |
|---|---|---|
| CBB (mens) | 20 | CBB, NCAAB, NCAABB Mens |
| WCBB (women) | 176 | NCAABB Women, WCBB (tab dark off-season) |
| CBB1H / CBB2H | 290 / 155 | empty until later in the season |

## Run

```powershell
.\run_pipeline.ps1 -CBBOnly                  # both boards when in season
.\run_pipeline.ps1 -CBBOnly -SkipFetch
```

Outputs:

| | Path |
|---|---|
| Mens | `outputs/<date>/cbb/step6_ranked_cbb.xlsx` and `step8_cbb_direction_clean.xlsx` |
| Women | `outputs/<date>/wcbb/step6_ranked_wcbb.xlsx` and `step8_wcbb_direction_clean.xlsx` |

Combined picks those up automatically (`MAIN` resumes 2026-11-01).

## October checklist (before tipoff)

1. **Women's ESPN master** (do not reuse the men's file):

```powershell
cd Sports\CBB
py -3.14 scripts\utilities\build_espn_athletes_master.py --gender women --out data\reference\ncaa_wbb_athletes_master.csv
```

2. **Boxscore caches** — week-1 L5 needs last season. Mens cache: `cbb_boxscore_cache.csv`. Women: `data/cache/wcbb_boxscore_cache.csv`.
3. **Defense ranks** — refresh `data/reference/cbb_def_rankings.csv` (women still share this file until a WCBB table exists).
4. **Torvik** — mens only (`docs/TORVIK_RATINGS.md`). WCBB is not wired.
5. When PrizePicks posts the women's tab, confirm the live id (176 is last year's fallback).

## Best props

`scripts/rank_best_props_today.py` prints CBB and WCBB sections when the slate date is in season **or** a step8 file exists. Combined still ranks from step6. Step7b does not score CBB (`ml_prob` stays blank).
