# Pre-registration: NFL Standard UNDER shadow

Frozen 2026-10-24. Machine copy: `nfl_standard_under_2026-10-24.json`.
Logger: `utils/nfl_std_under_shadow.py` (track `nfl_std_under_shadow`), called
from `scripts/build_goblin70_tickets.py` after the WNBA Standard UNDER watch.
Nothing here is placed or shown on `/tickets`.

## What is being tested

Whether NFL Standard UNDER on sacks and tackles, gated on `L10>=8 + cover>=1`,
beats the per-leg breakeven of a real 2-leg Power quote.

## Why the backtest is not the prior

The 2026-09-09 → 10-22 cell (`L10>=8 + cover>=1` 82.9% n=375; `L10>=8`
84.6% n=136 on the first cut) was built on labels, not faces:

- NFL step1 on the live tree did not persist `pick_type` before 2026-10-23.
  Step7 defaulted blank to `Standard`, so Goblin and Demon rungs graded as
  Standard faces.
- 484 of 632 graded "Standard UNDER" sacks / tackles legs sat on a ladder
  (two or more lines for the same player-prop-date). Sacks 1.5 UNDER was
  100% n=82, all ladder. Those are Demon / Goblin rungs where UNDER was never
  offered.
- The first real board (10-23) had sacks / tackles at Demon 99 / Goblin 23 /
  Standard 7. The true Standard face population is small.

The shadow starts from zero on boards whose step1 stored a mixed `pick_type`.

## Spec

| Item | Value |
|---|---|
| Sport | NFL, regular season only (`season_class == regular`) |
| Props in | `sacks`, `tackles` (PrizePicks "Tackles + Ast" canons to `tackles`) |
| Props out | `passcompletions` (explicit); everything else not listed is out |

## Amendment 2026-10-25 — sacks held

Frozen 2026-10-24 listed `sacks` and `tackles`. Sunday attach showed sacks
history as all-zero / `UNSUPPORTED_PROP` for starters (sibling or missing
column, not a real sack line). `utils/nfl_std_under_shadow.py` now logs
**tackles only**.

**Sacks excluded until stat attach verified; re-admitted when sacks history
is non-zero for starters.** This is a dated amendment, not a new
pre-registration. The original `props_in` stays `sacks` + `tackles` so the
shadow can take sacks back without rewriting the 10-24 spec. Until then the
logged set is a subset of what was registered.

Logger reject reasons: `prop_excluded` for `sacks`. Re-admit by restoring
`sacks` to `PROPS_IN` after a board where starter sack L5 is non-zero and
`stat_status` is OK. Do not silently leave them out after attach is fixed.
| Face | literal `Standard` in step1 `pick_type`; board must have ≥1 Goblin/Demon row (`step1_has_pick_type`) |
| Side | UNDER only |
| Gate | `l10_under >= 8` AND directional cover `line − L5 avg >= 1` |
| Timing | logged before kickoff only; `game_date` (ET) must be after 2026-10-24 |
| Dedupe | one row per player-prop-line; pairs disjoint by player |
| Shape | 2-leg Power, flat 3x, per-leg breakeven 57.7%; pairs priced p² (date-matched pair test returned 1.01 on the label data) |
| Stake | flat $20, paper |

## Two arms

Slip rule 4 (frozen 10-11): no 0.5 / 2.5 / 4.5 lines on role players, applies
to NFL in both directions, excludes SUPPORT role. It was built for OVER
fragility; for UNDER a low line may be the edge. Both arms share one leg log.

| Arm | Legs |
|---|---|
| `rule4_applied` | drop lines in {0.5, 2.5, 4.5} |
| `rule4_waived` | keep them |

NFL step8 has no usable role field (`Usage Role` blank, `Minutes Tier`
UNKNOWN, `Snap L3` blank on every Standard row), so the SUPPORT-role exemption
cannot be evaluated. The applied arm is **line-only** and treats every player
as a role player. Each leg carries `rule4_role_source = unavailable_line_only`.
If step8 later fills role / snaps, the arm can be re-cut from the log without
re-registering.

## Bar (the 10-11 bar)

- ≥ 100 decided legs (HIT / MISS) with `game_date` after 2026-10-24.
- Wilson 95% lower bound ≥ breakeven of a **real scraped** 2-leg Standard
  quote (`utils.scraped_quote.is_real_scraped_quote`: `live_cdp` /
  `n_correct_live` with `captured_at`). Breakeven = quote^(-1/2).
- With no real scrape on file, `summarize()` reports against flat 3x and marks
  `bar_met = false` with `needs a real scraped quote`.
- Legs whose graded face is not Standard, or whose line never appears on the
  graded board, are `NOT_ON_BOARD` and do not count either way.
- The bar is evaluated per arm. Meeting it in one arm does not promote the
  other.

## What happens if it clears

Nothing automatic. Report in `scripts/report_track_comparison.py`; the user
decides whether Standard UNDER becomes a placed track. No keep gate, no
ticket wiring, no `/tickets` change from this file.

## Plumbing that had to exist first

- Live tree NFL step1 now writes `pick_type`, `standard_line`, `line_score`
  and refuses a ≥100-row board that is 100% Standard
  (`assert_pick_type_persisted`, `--allow-standard-only` to override).
- Step7 / step8 already pass `pick_type` and `Line` through when present
  (10-23 step8: Demon 3428 / Goblin 1719 / Standard 1160).
- `grade_pending()` confirms each leg against `graded_props_<game_date>.json`
  by player + canon prop + line and requires the graded face to be Standard.
