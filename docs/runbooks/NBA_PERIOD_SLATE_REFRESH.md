# NBA 1H / 1Q slate refresh (playoffs & late tips)

When the matchup panel shows the wrong opponent (e.g. SAS vs MIN instead of OKC), the usual cause is **stale `team`/`opp` on published slate JSON**, not defense CSVs. Matchup edge reads `opp` from the slate via `tonight_matchups()`.

## Date: use PrizePicks board date (ET)

Step1 filters by `--date` in **America/New_York**. Late playoff slates often tip on the **next calendar day** vs when you run the pipeline. If step1 reports `survived=0`, check `filtered_game_dates` in the log and re-run with that date.

## Period history DB (required for step4+)

Period L5 lives in the `nba1q` table of the **canonical** ref DB:

`data/cache/proporacle_ref.db`

(Same file full-game NBA step4 uses for the `nba` table.)

If period boards stop at step3, or step4 prints all `NO_DATA`, rebuild/migrate history:

```powershell
py -3.14 scripts\build_nba1q_history_db.py
```

That script also copies any legacy `nba1q` rows from `Sports/NBA/data/cache/proporacle_ref.db` into the root DB once.

## Period-only pipeline (no full NBA)

```powershell
# Full fetch + step2→step8 for both boards
powershell -File scripts\_run_nba_period_refresh.ps1 -Date 2026-10-22

# Resume from existing step1 (after full-game step4+ is healthy)
powershell -File scripts\_run_nba_period_refresh.ps1 -Date 2026-10-22 -SkipFetch -ForceHistoryRebuild

# SoftFail: one board failing does not exit 1
powershell -File scripts\_run_nba_period_refresh.ps1 -Date 2026-10-22 -SoftFail
```

Writes under `outputs/<date>/nba1h/` and `outputs/<date>/nba1q/`, and copies step8 clean xlsx to `Sports/NBA/`.

Daily `run_pipeline.ps1` runs the same period path after full-game NBA; period failures are **soft** (full-game NBASuccess stays true).

## Matchup edge + mobile (separate sport flags)

```bash
py -3 scripts/build_matchup_edge_json.py --sport nba1h
py -3 scripts/build_matchup_edge_json.py --sport nba1q
```

`build_matchup_edge_json.py` accepts **one** `--sport` per run (`nba1h nba1q` together is invalid).

`_resolve_slate_path` prefers `outputs/<date>/<sport>/step8_*` (newest date folder) before `slate_sport_*.json`.

## Publish UI slate

Either full combined write:

```bash
py -3 scripts/combined_slate_tickets.py --write-web --date <YYYY-MM-DD> ...
```

Or regenerate mobile bundle after updating `ui_runner/templates/slate_latest.json`:

```bash
py -3 scripts/generate_mobile_bundle.py
```

## Sanity checks

```bash
py -3 -c "import json; d=json.load(open('ui_runner/templates/nba1h_matchup_edge.json')); print(d['matchups'].get('SAS'))"

py -3 -c "import json; r=json.load(open('mobile/www/slate_sport_nba1h.json'))['rows']; print({(x['team'],x['opp']) for x in r if x['team']=='SAS'})"
```

Both should show **OKC**, not MIN.
