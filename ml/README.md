# Gate discovery (`ml/`)

Readable rules, scored at the **real scraped quote**, not at hit rate.

PrizePicks already prices Goblins knowing they hit often. A gate that lifts
Goblin hit rate from 75% to 80% still loses if Power-3 pays 1.65x (needs
~84.6%/leg). Edge is wherever your hit rate beats what the quote implies.

This package is **not** the ticket-probability model in `docs/ml/` and
`data/ml/`. Nothing here is auto-wired into `*_keep_gates.py`.

NBA Goblin-70 stays **shadow / hold** until a slice clears the Wilson lower
bound at a scraped quote on dates the search has not seen.

NBA factor stack and quote gaps live in [`nba_prop_assessment.md`](nba_prop_assessment.md).
Oct 25 closed the NBA scout candidate. Next scout **2026-11-20** (pre-tip
`slate_minutes` only). Standard stay **3-pick Power at 6x or 2-pick at 3x — no 4–6 leg slips**.

NFL stands in [`nfl_prop_assessment.md`](nfl_prop_assessment.md)
(full-board category reference in §7). Historical
keep cells are not a prior (face labels defaulted until 10-23). First build
is nflverse snap + target share. Volume-week freshness is a hard fail.
Pre-kick snapshots go in `Sports/NFL/data/cache/nfl_volume_snapshots.jsonl`.
The projection calibration test is pre-registered at
`ml/prereg/nfl_volume_projection_2026-10-25.md` (train 2021–2024, test
2025) and has not been run. Tackles Standard UNDER is one
pre-registered shadow. Demon rush-attempt / reception volume is frozen
(`ml/prereg/nfl_demon_volume_2026-10-25.md`; favored-or-close ≤ +3.5).
A second track logs top-2 Demon receptions under the L5 floor
(`ml/prereg/nfl_demon_rec_floor_2026-10-25.md`). Observe only. The
leader gate needs a board spread before it can log.

Every other sport uses the same stack
([`prop_skeleton.md`](prop_skeleton.md)): opportunity × rate × context
→ distribution → calibrate → N-correct quote. Thin adapter per sport.

| Note | Status | First step |
|---|---|---|
| [`nba_prop_assessment.md`](nba_prop_assessment.md) | Scout closed Oct 25. Next 2026-11-20 | Pre-tip minutes only |
| [`nfl_prop_assessment.md`](nfl_prop_assessment.md) | Volume through week 7. G70 shadow | Calibration prereg; do not run yet |
| [`nhl_prop_assessment.md`](nhl_prop_assessment.md) | In season. Mixer held. No keep file | Daily Faceoff goalies, totals, ESPN vs NHL API |
| [`soccer_prop_assessment.md`](soccer_prop_assessment.md) | Keep cells unverified | Rerun step4; re-measure Shots / SOT |
| [`tennis_prop_assessment.md`](tennis_prop_assessment.md) | Goblin verified. Total Games held | Replace stale Sackmann; ML → Total Games |
| [`cfb_prop_assessment.md`](cfb_prop_assessment.md) | Weekly. CFBD key unset | Set `CFBD_API_KEY`; red-zone for Player TDs |
| [`cbb_prop_assessment.md`](cbb_prop_assessment.md) | Archived dry-pass checks OK; tickets held | Opening-night board must pass the same four checks |
| [`mlb_prop_assessment.md`](mlb_prop_assessment.md) | Series, then offseason. Keep gates live | Winter Statcast projection + calibration |
| [`wnba_prop_assessment.md`](wnba_prop_assessment.md) | Off until May | Port NBA minutes / `proj_mu` |
| [`golf_prop_assessment.md`](golf_prop_assessment.md) | Light. Course-blind | Later (DataGolf) |
| [`period_prop_assessment.md`](period_prop_assessment.md) | All held | After full-game layers |

Priority this week: Odds API / ESPN spreads (Demon shadow) → CBB
archived-slate dry pass (tickets held) → soccer step4 re-measure
Shots / SOT → nflverse play-by-play. Everything else waits.

## Tools

| Tool | Role |
|---|---|
| **pysubgroup** | Subgroup discovery. Combinations like `L5=5 AND minutes_tier=HIGH AND def_tier∈{Weak,Below}` ranked by lift and size. Closest off-the-shelf match to `scan_gate_thresholds`. |
| **imodels** | RuleFit, SkopeRules, FIGS, Bayesian Rule Lists. FIGS gives small trees that read like MLB keep gates. |
| **optbinning** | Single-feature cut points with monotonic constraints. “Is BA ≥ .275 the right number, or .262?” |
| **sklearn + SHAP** | Depth-3 tree (or LightGBM with monotone constraints) to generate candidates. SHAP dependence shows where an effect switches on. |

Install extras (local only — do **not** add to Railway `requirements.txt`):

```powershell
py -3.14 -m pip install -r ml/requirements.txt
```

`scripts/discover_gates_sport.py` already does walk-forward OOS + Wilson + atom
combinations. These tools widen the search past single thresholds.

## Guardrails

1. Walk-forward splits **by date only**, never random.
2. Minimum n per slice, judged by the **Wilson lower bound**.
3. Correction for testing hundreds of slices (**Benjamini–Hochberg**).
4. Final check: the gate clears breakeven at **real quotes** on dates it has not seen.
5. Flag split imbalance (most of a cell sitting on one side of the global cut).

Default Goblin quotes (N-correct, not 1st-place):

| Shape | Quote | Per-leg breakeven |
|---|---|---|
| Power-2 | 1.40x | 84.5% |
| Power-3 | 1.65x | 84.6% |
| Power-3 observe | 2.00x | 79.4% |
| Power-3 catalog (retired) | 2.50x | 73.7% |

Standard Power-2 ~3x / Power-3 ~6x. Flex consolation is not priced (no live
Flex Goblin card).

## Find the data you’re missing

Start with **confident misses** (passed the gate, still missed) vs hits on the
same gate. Minutes drops, blowouts, late scratches, back-to-backs, line
movement. `miss_attribution` first; pysubgroup on misses-only next.

Known fill gaps, by sport:

| Sport | Source |
|---|---|
| NBA | `nba_api` (tracking, rotations, minutes, usage); official NBA injury-report PDF (late scratches); Rotowire / similar for projected lineups |
| NHL | MoneyPuck (xG, lines, goalies); Natural Stat Trick (already used); Daily Faceoff (starting goalies — `goalie_name` is 0% fill); Odds API (`game_total` is 0% fill) |
| MLB | `pybaseball` Statcast: exit velocity, pitch mix, platoon |
| Soccer | FBref, Understat, StatsBomb open data (xG, minutes, confirmed lineups). Starter status was the biggest soccer finding. |
| Tennis | tennis-data.co.uk or Sofascore — replace the 151-day-stale Sackmann feed (aces / DF L5 empty) |
| Market | Pinnacle + `line_history` once `_mkt_key` is fixed. Closing-line movement is often the strongest outside variable and is not recorded. |

## Run

All sports in `SPORT_CFG` (CFB, CFB1H, Soccer, Tennis, WNBA, WNBA1H, NHL, NBA,
NBA1H, MLB, NFL, Golf, CBB), each book (Goblin OVER / Standard OVER / Standard UNDER):

```powershell
py -3.14 -m ml.discover_gates
# pysubgroup is off by default (beam search hung on empty slices). Opt-in:
py -3.14 -m ml.discover_gates --sport NBA --book "Goblin OVER" --pysubgroup
```

One sport / one book:

```powershell
py -3.14 -m ml.discover_gates --sport NBA --book "Goblin OVER"
```

NBA-only shim (same path):

```powershell
py -3.14 scripts\discover_nba_goblin_quote_gates.py
```

Writes `data/reports/ml_discover_gates/summary.md` plus per-sport JSON.

NBA scout vs generic gate at scraped quotes (walk-forward box + cell priors;
passes `lookup=` / `index=` so legs are not all held):

```powershell
py -3.14 -m ml.nba_scout_quotes
```

Writes `data/reports/ml_discover_gates/nba_scout_quotes.md`. 2026-02-19 →
2026-10-22 (89 slates): GATE 67.0% n=533, scout top-N proj_edge 76.3% n=523,
proj_ev_edge 78.0%. Scout beats the gate by ~10 points and is the only sleeve
positive at 2.5x, but nothing clears 1.65x (84.6%) or 2.00x (79.4%) — not on
point estimate, not on Wilson. Scout Rebounds is the strongest cell
(81.1% n=169, Wilson 0.745) and still misses 2.00x.

### Standard UNDER candidates (four checks)

The only non-artifact signal in the all-sports pass was Standard UNDER
(2-pick Power 3x → 57.7%/leg). Before believing a cell:

```powershell
py -3.14 -m ml.standard_under_checks            # NHL L10>=8, NFL generic / L10>=8 (+cover)
py -3.14 -m ml.nba_scout_quotes --book "Standard UNDER"
```

Writes `standard_under_checks.md`. Each candidate gets: prop × line (0.5-line
share), quote reality (flat 3x/6x; literal `Standard` label vs blank/dash
default; line vs board Standard line), walk-forward by month with an
expanding "would the prior record have said play" column, regular-season vs
Apr 15–Jun 30 split, and same-game vs different-game pair joint hit rate
against a **date-matched** p² (a hot month with many legs is not correlation).

2026-10-24 result: **NHL `L10>=8` fails** — 50% 0.5-line, March 81.7% then
51–55% in the playoff window, forward-only 53.4% n=485, Oct 66.7% n=42. **NFL
`L10>=8` passes all four** on 2026-09-09 → 10-22: 80.9% n=472, 9% 0.5-line,
non-0.5 79.7% Wilson 0.756, OOS 80.4% n=199, forward-only 83.2% n=285,
same-game lift 1.01 after date matching. Six weeks of one season, heavy on
sacks (1.5 UNDER 100% n=44) and tackles; passcompletions UNDER is 41%. It
goes to the forward shadow under the 10-11 promotion bar, not to tickets.
NBA Standard UNDER scout is 55.8% n=249 at top-3/day — at breakeven, nothing.

Do **not** paste a winning slice into a keep file from this output. A Wilson
clear with `train_n` near 0, or a `line_0.5` counting-stat sleeve, is an
artifact until a later slate replicates it.
