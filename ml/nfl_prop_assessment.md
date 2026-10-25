# NFL prop assessment

Working note next to this folder’s `README.md` and `nba_prop_assessment.md`.
How to judge every PrizePicks NFL face, what is already attached, and what
is worth building. Nothing here is a keep gate. NFL Goblin-70 stays
**shadow / hold**. The only pre-registered experiment is Standard UNDER
tackles (`ml/prereg/nfl_standard_under_2026-10-24.md`).

Sunday 2026-10-25: 1PM games are underway. What’s left is the 4:05 / 4:25
window and SNF. Treat everything except the tackles shadow as shadow or
very small.

## 1. Where NFL stands

Historical NFL results are **not a prior**. Face labels were defaulted
until 2026-10-23, so keep gates were measured on Demon and Goblin rungs
graded as Standard. On real faces, Standard UNDER receiving yards was
~44% and receptions ~60%, on small samples. Sacks were held out of the
shadow after the first real board.

About 40% of the NFL board still isn’t measurable: unsupported props
(completions, field goals, period props) and mapping failures. The
Boston stub-box case was a data-quality miss, not a gate miss.

step8 role / snap was empty because the inputs were stale or missing:

- `nfl_route_participation.csv` had been frozen at **week 2** (2026-09-22)
- `nfl_snap_pct_cache.json` was missing, so step4c wrote `UNKNOWN`
- Rebuilt 2026-10-25 through **week 7** (488 skill / 1,862 snap-cache players).
  `pbp_participation` still 404 — route % is still a snap proxy.

An L5 in Week 7–8 is most of the season. L5 = 5 means “every game so
far,” not a trend. Work is **building inputs**, not tuning gates.

## 2. Factors behind every NFL prop

| Layer | Factors | Why |
|---|---|---|
| Opportunity | Snap share, routes run, target share, carry share, red-zone share | NFL minutes. Volume drives almost every yardage prop |
| Game script | Spread and total | Bigger than any other sport. Trailing teams pass; leading teams run |
| Injuries / inactives | Wed–Fri practice reports, inactives at T−90, QB and O-line changes | A backup QB changes the whole offense |
| Matchup | Defense vs position, coverage, pass-rush vs pass-block | One Weak/Elite label is coarse |
| Environment | Wind, precip, dome / outdoor, home / away | Wind hits passing yards, long receptions, kicking |
| Efficiency | Yards per target or carry, catch rate, aDOT | Noisy. Shrink heavily |
| Market | PP vs sharp, line movement | Often the strongest outside signal |
| Data quality | Stub boxes, byes, stat mapping, face labels | Today’s Boston case |

Logical order on a slate:

1. Drop unsupported / invalid-attach props. No cell, no fallback floor.
2. Require a real `pick_type` (empty defaults are not Standard).
3. Prefer volume that exists (snap + target / carry share) over L5=5 alone.
4. Veto OVERs into a game script that removes the volume (blowout, sit).
5. Price any idea at the **N-correct** quote. NFL Standards stay 2–3 pick.

## 3. What moves each prop

| Prop | Drivers |
|---|---|
| Passing yards / attempts | Spread, total, pass rate over expected, opp pass D, wind |
| Rushing yards / attempts | Carry share, leading-team run script, opp run D, O-line |
| Receptions / receiving yards | Target share, routes, QB quality. Receptions more stable |
| Longest rec / rush | One-play. aDOT and big-play rate. High variance |
| Tackles | Def snaps, opp run rate and plays, position (LBs) |
| Sacks | Pass-rush win vs pass pro, dropbacks. Low lines, high variance. Shadow **held** |
| Kicking points | Team implied total, dome / wind |
| TDs, INTs | Binary. Mostly avoid |

## 4. What is already attached

| Piece | Where | Status |
|---|---|---|
| Face / attach guards | `utils/stat_attach_guard.py`, ledger `face_status` | Analysis drops invalid |
| Tackles Standard UNDER shadow | `ml/prereg/nfl_standard_under_2026-10-24.md` | Logging only. Sacks held |
| Snap % cache | `build_nfl_snap_pct_cache.py` → step4c | Rebuilt through week 7; `through_week` required |
| Route / target / WOPR | `build_nfl_route_participation.py` → `attach_participation` | Week 7; week-freshness is a hard fail |
| Pre-kick volume log | `utils/nfl_volume_snapshot.py` | Same as NBA minutes; lock per game at kick |
| Volume-week assert | `utils/nfl_volume_freshness.py` | Step3 / step4c / pipeline fail if behind |
| Weather | Open-Meteo via `utils/nfl_espn_context.py` | Attach wind to pass / long rec / kick |
| Spreads / totals | step6b Odds API | Blank when quota is 0 |
| Unit / pace / RZ / QB mobility | Tuesday pack | Soft only. Do not gate |

## 5. Gaps, in build order

1. **nflverse snap share + target share (in)** — rebuild the two caches
   through the latest published week and join them onto the board.
   This is the NFL minutes layer. Hard gates stay off until n is honest.
2. **Volume × efficiency × game-script projection** — snaps × routes ×
   target (or carry) share, shrunk Y/target, then P(over) on every rung.
   Calibration test is pre-registered in
   `ml/prereg/nfl_volume_projection_2026-10-25.md` (train 2021–2024,
   test 2025, decile + Brier, pass vs base-rate Brier / ECE). **Do not
   run the model until that file is the spec.** No PrizePicks lines in
   that test. Only the later edge-vs-PP experiment needs forward faces.
3. **Odds API top-up** — restores script. Bigger here than NBA.
4. **Inactives at T−90** — same shape as NBA T−30, a few minutes earlier
   relative to kickoff. Practice reports midweek.
5. **Unsupported props** — completions, FGs, period props need ESPN
   stat columns.
6. **Sharp player-prop lines** — after script is back.

Do **not** retune keep gates on the contaminated book. Do **not** treat
L5=5 as a trend.

## 6. How the ML fits

```
volume (snaps × routes × tgt/carry share)
        ×  shrunk efficiency
        ×  game script
        →  distribution
        →  P(over) on every Goblin / Standard / Demon rung
        →  EV at the scraped N-correct quote
```

Shortcut: nflverse has years of player stats. Backtest whether
“70% to exceed 50 yards” happens 70% of the time, today, with no PP
line. That is the calibration problem. Edge vs PrizePicks is a later
forward test on real faces only.

Guardrails stay the same as `README.md`: walk-forward by date, Wilson,
real quotes, no 4–6 leg Standard slips.

## Today

Tackles shadow is logging. Everything else on the Sunday list is
unproven Goblin OVER or a Standard built without current role data —
shadow or very small. 1PM games are live; do not ticket those.

Volume caches are no longer a Tuesday reminder. `assert_nfl_volume_week_fresh`
fails NFL step3 / step4c / `run_nfl_pipeline.ps1` when snap or route
`through_week` is behind the last completed NFL week (Sunday of week N
is still N−1 until Tuesday). Rebuild is the fix, not a skip.

```powershell
py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py --season 2026
py -3.14 Sports/NFL/scripts/build_nfl_route_participation.py --season 2026
```

`--week 0` (default) uses every published week. Pre-kick snap / target /
WOPR values land in `Sports/NFL/data/cache/nfl_volume_snapshots.jsonl`
the same way NBA minutes snapshot. Train later on those rows only.
Route % stays a snap proxy until `pbp_participation` exists; the route
builder switches automatically when that file returns.
