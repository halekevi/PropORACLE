# Cursor Brief: NFL Leg Probability Model (walk-forward baseline)

## Goal

Predict hit probability for every NFL leg, both sides. Ticket on probability instead of stacked gates. Prove it beats the current gate before it replaces anything.

## Step 1: Training data

Build `Sports/NFL/data/nfl_leg_training_rows.csv` from all graded weeks (W1 to latest).

- One row per unique player-game, prop, side, pick_type. Dedupe ladder rungs: keep the deepest L5=5 rung, or the rung closest to the median line.
- Skip VOID, NO_ACTUAL, DNP, and Demons.
- Label: hit (1/0).
- Commit it. Without this file nothing else works.

## Step 2: Features (~12)

- `l5_hits`, `l10_hits`, `min_m_pct` (min L5 margin / line), `cover_pct` (cover / line)
- `snap_l3` (null for kickers)
- `opp_pass_d_tier` or `opp_rush_d_tier` (correct D axis for the prop family) → stored as `opp_d_tier` + `opp_d_axis`
- `not_elite_off` flag
- `pace_tier`
- `pick_type`, `prop`, `side`

Do **not** use: DL run tier, combined DL tier, secondary tier.

## Step 3: Model

Logistic regression only. Over and Under as separate rows.

## Step 4: Walk-forward

Train on weeks 1..N, score week N+1, repeat. Never score training rows. Save OOS to `nfl_leg_model_oos.csv`.

## Step 5: Report

Calibration; model vs gate; by prop family; direction accuracy.

## Rules fixed before results

- Ticket at p >= 0.75
- Replace gate only if OOS HR >= gate and volume >= 70% of gate; else packing tiebreaker
- Calibration within 5 points in the 70-80 bin or not trusted
- Bins under n=30 flagged thin
