# NFL prop assessment

Working note next to this folder’s `README.md` and `nba_prop_assessment.md`.
How to judge every PrizePicks NFL face, what is already attached, and what
is worth building. Nothing here is a keep gate. NFL Goblin-70 stays
**shadow / hold**. Pre-registered experiments: Standard UNDER tackles
(`ml/prereg/nfl_standard_under_2026-10-24.md`) and Demon volume
(`ml/prereg/nfl_demon_volume_2026-10-25.md`, rush-attempt lead backs and
reception target leaders when favored-or-close) and Demon receptions
L5-floor (`ml/prereg/nfl_demon_rec_floor_2026-10-25.md`, top-2 target
share, line under L5 min). Neither is placed. The 2026-10-25 6-leg
Power that finished 5/6 paid nothing; Flex would have paid 5/6.

Sunday 2026-10-25: a 6-leg Power finished 5/6 (Skattebo rush-att Demon
and Tucker rec Demon hit; Washington Standard rec-yards missed). 5/6
Power pays nothing; the same slip as Flex would have paid. Washington
was the thin leg: Standard yards near the median, no cushion, and the
morning rebuild had already dropped his solo line. Pre-submit: flag any
leg whose line sits near L5/projection with no floor. Volume Demons
(attempts, receptions, targets) can sit under a workhorse’s usual floor;
yards cannot. The frozen leader gate would not have logged Tucker — that
is why the rec-floor track exists.

Two observe-only backtests sit next to those tracks
(`ml/prereg/volume_floor_backtest_2026-10-25.md`). They must not retune
them. A is line-based on face-verified NBA/MLB (and whatever else
clears). B is line-free NFL floor persistence from nflverse. Neither
produces honest EV. A better cut becomes a new track.

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
| Demon volume observe | `ml/prereg/nfl_demon_volume_2026-10-25.md` | Rush att / rec Demon OVER; lead back / target leader; favored-or-close; line under recent. Frozen. Small stake, not a lock |
| Demon rec-floor observe | `ml/prereg/nfl_demon_rec_floor_2026-10-25.md` | Receptions Demon OVER; top-2 + share ≥15pp; line below L5 min. Tucker-type, not the leader gate |
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
   Demon volume observe (frozen) cannot log a leg without `spread`;
   missing lines fail `spread_unknown`. Account action, not a gate edit.
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

Tackles shadow is logging. Demon volume is pre-registered the same way:
lead back / target leader with no committee or injury, favored or close
(spread ≤ +3.5), and the live Demon still under recent volume. Observe
only; a matching face is a small-stake candidate until the log has a
few weeks, not a lock. First slate after 2026-10-25. Everything else on
the Sunday list is unproven Goblin OVER or a Standard built without
current role data — shadow or very small. 1PM games are live; do not
ticket those.

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

## 7. Category reference (full board)

Nothing in this section is a keep gate. Niche-market edge is a
**hypothesis to pre-register**, not a conclusion. Popular props (pass
yards, receiving yards, anytime TDs) get the most market attention.

Skip Fantasy Score (already banned) and every `(Combo)` two-player
prop. The data cannot assess those well.

### Volume (best place to focus)

Rush Attempts, Receptions, Rec Targets, Pass Attempts, Pass Completions,
Tackles+Ast.

What decides them: role and opportunity (snap share, routes, target
share, carry share), game script (spread and total), pass rate over
expected, and for tackles the opponent’s run rate and plays per game
plus defensive snap share.

Data: snaps / routes / target and carry share from nflverse (now
attached), spreads and totals (Odds API), defensive snap counts.

Pipeline: rush attempts and receptions are supported. Pass attempts and
completions are unsupported (no `PASS_CMP` / `PASS_ATT` columns).
Targets still need nflverse targets joined to the board.

Volume is the steadiest part of football. Skattebo Demon rush attempts
is the example. That track is frozen at
`ml/prereg/nfl_demon_volume_2026-10-25.md` (observe only).

### Yardage

Pass Yards, Rush Yards, Receiving Yards, Rush+Rec Yds, Pass+Rush Yds.

Volume × efficiency. Efficiency is noisy — shrink it heavily toward a
position average.

Data: air yards and aDOT, opponent defense by position (EPA allowed),
pressure rate vs pass blocking, wind.

Supported, but these are the most-watched props and likely the most
efficiently priced.

### Kicking

Kicking Points, FG Made, PAT Made.

Team implied total, red-zone efficiency (field goals from stalled
drives), dome or wind, kicker range, coach aggression on fourth down.

Kicking points supported. FG made is unsupported.

### Derived and first / longest

Completion %, Passer Rating, Rush Yards Per Carry, Yards on First
Reception, Rush Yards in First 5 Attempts, Consecutive Pass Completions,
Longest Rush / Reception / Completion.

Individual-play markets. High variance. Today’s mapping failures (YPC
built from total rush yards, “longest reception” from catch counts).

nflverse play-by-play computes every one of these exactly, for grading
and for history. Do not sibling-map them.

### Quarter and half thresholds

Quarters with 1+ Reception / 5+ Rush Yds / 10+ Rec Yds / 25+ Pass Yds;
Halves with 25+ Rush / 25+ Rec / 100+ Pass Yds.

How consistently a player gets volume across periods, plus game script
(stars sit in blowouts). Same nflverse play-by-play, quarter-tagged.

Interesting because “quarters with 1+ reception” for a target leader is
a distribution question that is hard to eyeball and may be priced
loosely. Hypothesis only.

### Touchdowns, interceptions, sacks

Anytime TDs, Rush TDs, Pass TDs, INT, Sacks, Sacks Taken.

Low-count, high-variance. TDs: red-zone and goal-line share plus
implied team total. INTs: QB interception rate and opponent pressure.
Sacks: pass-rush win rate vs pass blocking and opponent dropbacks.

Mostly avoid for tickets. Sacks return to the Standard UNDER shadow
once the stat attaches.

### Punting

Punts, Punts Inside 20, Punt Touchbacks.

Roughly the inverse of offensive success: weak offense, strong defense,
low implied total, bad weather. Needs play-by-play, team efficiency,
implied totals. Niche and probably lightly modeled. Hypothesis only.

## 8. Biggest unlock: nflverse play-by-play

About 40% of the 2026-10-25 board was unmeasurable (1,630 unsupported
plus 466 mapping failures). Play-by-play fixes most of that in one
integration:

- Grade derived, first-N, longest, quarter, and half props correctly
- Build honest history for them
- Model them, because each is a function of plays

`pbp_participation` is still 404; route % is a snap proxy until that
file returns. Play-by-play attach is a separate ingest from the weekly
stat tables already on the board.

## 9. ML layer (per family)

```
volume model (snaps, routes, targets, carries, plays;
             tackles opportunity for defenders)
        ×  efficiency model (shrunk toward position averages)
        ×  game-script adjustment (spread + total)
        →  distribution
             Poisson / negbin for counts
             quantile LightGBM for yards
             quarter-level counts for threshold props
        →  calibrate on 2021–2025 nflverse
           (existing prereg: ECE ≤ 0.05 per family,
            ml/prereg/nfl_volume_projection_2026-10-25.md)
        →  compare to the real N-correct quote
        →  discover_gates on the residuals
```

Do not run the volume projection until that prereg is the spec.

Long-term: a drive-level game simulator (a few thousand play-by-play
paths per game). Yards, attempts, longest, first reception, quarters,
kicking, and punts then fall out of the same simulation. Larger
project; what serious NFL modelers use. Not a near-term ticket path.

## 10. Where language models help

Not by picking props. A language model has no edge at guessing
outcomes. Use it to turn unstructured information into timestamped
features, which is where the market moves:

| Job | Output |
|---|---|
| News to features | Injury reports, Wed–Fri practice, beat posts, pressers → `expected_role_change`, `snap_limit`, `new_starter`, each with a pre-kick timestamp |
| Role-change detection | Trades, depth-chart promotions, backup RB taking over |
| Daily data-quality reviewer | Automated pass over the best-props list; flag all-zero starter history, YPC with an 82-yard average |
| Factor cards | Plain-language projection notes for human judgment |

Treat AI features like any other input. Log them with a pre-kickoff
timestamp and test them in a shadow first. A summary written after the
news broke can leak the answer.

## 11. Build order (after current volume attach)

1. **nflverse play-by-play attach** — correct grading and history for
   every derived and period prop.
2. **Volume models** for attempts, receptions, targets, and tackles,
   plus spreads from Odds API or ESPN. Odds API top-up is still the
   account blocker for script on the live board.
3. **Pre-register a niche-market test** — punts, kicking points,
   quarter thresholds. Write the spec before any run.
4. **AI news-to-features**, with timestamps, shadow only.
5. **Drive-level simulator** as the long-term upgrade.

Current locks that do not move for this list: Demon volume observe is
frozen; tackles Standard UNDER is logging; volume-week freshness is a
hard fail; no Demons on the general ticket path.
