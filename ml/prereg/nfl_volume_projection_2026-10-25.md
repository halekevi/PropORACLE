# Pre-registration: NFL volume projection calibration

Frozen 2026-10-25. Machine copy: `nfl_volume_projection_2026-10-25.json`.

This is written **before** the model is trained or scored. Same discipline
as the NBA scout pre-registration: the test, the split, and the pass bar
are locked first. A later notebook that beats a softer bar does not count.

Nothing here is a keep gate. Nothing is ticketed. PrizePicks lines are
out of this test entirely.

## What is being tested

Whether a volume × efficiency × script projection, built only from
information available before each game, is calibrated on a held-out NFL
season.

The question is not “does this beat a PrizePicks line.” It is: when the
model says 70% to clear 50 receiving yards, does that happen about 70%
of the time.

## Split (locked)

| Item | Value |
|---|---|
| Train | Regular-season player-games, 2021–2024 |
| Test | Regular-season player-games, 2025 |
| Walk-forward check | Inside 2025, fit through week *W−1*, score week *W* (weeks 5–18) |
| Leakage | Features for game *G* use only games strictly before *G* |
| Snapshots | When a pre-kick snapshot exists, use it. Post-kick rebuilds are invalid training rows |
| Preseason / playoffs | Out |

2025 is complete enough to score and is not the live 2026 board.

## Features in

- Snap share L3 / season (nflverse `snap_counts`)
- Target share L3 / season and WOPR (nflverse `stats_player_week`)
- Route % — snap-share proxy today; switch to true routes when
  `pbp_participation` or `stats_player_week.routes` exists
- Carry share for RB rush props
- Game script: closing spread and total when present; missing script is
  a missing feature, not a zero
- Position, home/away

## Features out

- Any PrizePicks line, pick type, or face
- Same-game box stats
- Next-game snap / target updates
- Injury designations posted after kickoff

## Targets (per prop family)

Predict P(actual > threshold) on a fixed threshold grid, then the mean
and a distribution. Thresholds are nflverse actuals, not market lines.

| Family | Thresholds |
|---|---|
| receiving_yards | 15, 25, 40, 60, 80 |
| receptions | 2, 4, 6 |
| rushing_yards | 20, 40, 60 |
| rush_attempts | 8, 12, 18 |
| passing_yards | 180, 225, 270 |
| pass_attempts | 25, 32, 38 |

A player-game is in a family only when the player recorded that skill
role that season (WR/TE for receiving; RB for rushing; QB for passing).
Zero-snap games stay in as zeros if the player was active.

## Metrics (per family, on 2025)

1. **Brier score** of P(over threshold) vs the realized indicator, at
   each threshold, then a size-weighted mean per family.
2. **Base-rate Brier** — same labels, probability = train-set hit rate
   for that family × threshold × position. The model must beat this.
3. **Decile reliability** — 10 equal-count bins of predicted P.
   Report predicted mean, empirical rate, and n per bin.
4. **ECE** — mean absolute gap × bin weight across the 10 bins.
5. **Secondary** — MAE of predicted mean yards vs actual yards.

## Pass bar (all must hold on the 2025 test)

| Check | Threshold |
|---|---|
| Family Brier | Strictly below that family’s base-rate Brier |
| ECE | ≤ 0.05 at the primary threshold (rec 40 / rush 40 / pass 225 / recs 4 / rush att 12 / pass att 32) |
| Top two deciles | Empirical rate within 8 points of predicted mean, and n ≥ 40 per decile |
| Walk-forward | Same Brier-beats-base and ECE ≤ 0.06 on the week-by-week 2025 cut |

A family that misses the bar is not used. Passing receiving yards does
not admit rushing yards.

## What happens if it clears

Report only. No keep gate, no `/tickets` change, no live edge vs PP.
A later experiment (real faces, scraped N-correct quote, Wilson bar)
is a new pre-registration.

## What happens if it fails

Say so. Do not retune thresholds on 2025 and call it a pass. The next
allowed change is a dated amendment that names the leak or the feature
that was wrong, then a new held-out year or a frozen walk-forward.

## Plumbing that has to exist first

- `assert_nfl_volume_week_fresh` — snap and route `through_week` must
  cover the last completed NFL week or the step fails.
- Pre-kick snapshots in `Sports/NFL/data/cache/nfl_volume_snapshots.jsonl`.
- Route builder overlays `pbp_participation` when that file exists;
  until then `route_pct_source=snap_proxy` is the documented stand-in.
