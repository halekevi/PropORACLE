# Pre-registration: Goblin specialist retrain on cleaned ledger

**Registered 2026-10-28 before any holdout / test metrics for this run.**
Machine copy: `goblin_specialist_clean_retrain_2026-10-28.json`.

**Nothing promotes from this run.** This file locks the split, the
filters, the features, the report, and the no-promote rule. A later
dated prereg is required before any candidate pickle can replace a
live artifact.

## Why this run

The August 2026 Goblin-OVER specialist (`models/edge_model_goblin_over.pkl`,
trained ~2026-08-28) reported `goblin_test_corr` 0.30 on the slice it
was given. The ledger has since been cleaned: face verification,
stat-attach invalids, stale-board / `SOURCE_BOARD_DATE` mismatch
drops, as-of L5, `l5_current_season_n`, and clean minutes.

This run retrains that specialist on the cleaned ledger so we can
see whether Goblin ranking quality still clears **corr > 0.15**
out of sample. It is not a production swap. It does not overwrite
`models/edge_model_unified.pkl`. It does not write live tickets.

## Locked split

| Slice | Rule |
|---|---|
| Train (fit + Platt calib) | event / slate date **strictly before 2026-09-01** |
| Test | event / slate date **≥ 2026-09-01**, dates the model has not seen |
| Cut | `2026-09-01` inclusive on test; exclusive on train |

Train never includes a row whose date is on or after the cut.
Test never includes a row whose date is before the cut.
No random split. No peeking at test labels to choose features,
hyperparameters, or a second cut.

Inside the train pool, the existing trainer three-way split
(fit / Platt calib / unused-within-train) may run **only on dates
< 2026-09-01**. The reported OOS numbers for this prereg are the
≥ 2026-09-01 test slice, not the within-train leftover.

## Locked population

Source: `analysis_legs(decided_only=True)` (face-verified +
stat-attach-ok + HIT/MISS). Additional hard drops:

1. **Verified faces only.** Unverified sport-dates stay out
   (`include_unverified=False`).
2. **Stale boards dropped.** `ticket_leg_row_is_stale_board` and
   `invalid_reason == stale_board`, including `SOURCE_BOARD_DATE`
   vs game/slate date mismatches.
3. **As-of features only.** No post-tip box, no lookahead L5/L10,
   no post-game actuals as inputs. Label is HIT/MISS only.
4. **Invalid-stat-attach props already dropped** by `analysis_legs`.

## Locked features (adds)

Carry the August Goblin specialist feature list as the starting
recipe. This run **must include** (add if the dataset builder is
missing them):

- `l5_current_season_n` — how many of the card L5 games are
  current-season RS (0–5). Last-season pad is a different
  population.
- `clean_minutes` — uncontaminated minutes pool (NBA / WNBA
  safety column; other sports may be missing / neutral fill).

Do not add post-game columns. Do not use `edge` / `abs_edge` /
`prop_score` / `result` / `actual_value` as tree inputs
(`ALWAYS_EXCLUDE_FROM_EDGE_TRAINING` stays).

## Locked model

| Item | Value |
|---|---|
| Start from | `models/edge_model_goblin_over.pkl` (Goblin specialist, ~2026-08-28), **not** the June unified pickle |
| Train filter | Goblin OVER (book = goblin, direction = OVER) for the specialist fit |
| Candidate write | `models/edge_model_goblin_over_clean_2026-10-28.pkl` |
| Sidecars | `{stem}_features.json`, `{stem}_metadata.json`, `{stem}_slice_calibrators.pkl` beside the candidate |
| Production unified | **do not write** `models/edge_model_unified.pkl` |
| Live tickets | **do not write** `tickets_latest.json` / do not publish |

The specialist is fit on Goblin OVER. Evaluation still scores
**every rung** on the held-out dates so we can see whether the
cleaned ledger changed ranking quality outside Goblin.

## Locked report (after train, not before)

On the test slice only, report:

- Pearson **corr**(p, hit)
- ROC-**AUC**
- **Calibration** (mean predicted p vs observed hit rate; plus
  reliability buckets when n allows)

**Broken by sport × rung**, where rung is Goblin / Standard / Demon.

Primary gate for this file: **Goblin corr > 0.15** on the
≥ 2026-09-01 test slice (pooled Goblin OVER; also print per-sport
Goblin). Compare that pooled Goblin corr **honestly** to the
August slice’s 0.30. A lower number on a cleaner, later window is
not a failure of the old artifact — it is the answer this run
exists to get.

Cells with n too small for AUC (trainer floor) or corr (n < 40 or
zero variance) print as missing, not 0.

## Out (this run does not)

- Promote the candidate to production
- Overwrite `edge_model_unified.pkl` or the August Goblin pickle
- Change Goblin-70 / mixer / keep gates / `/tickets`
- Use test metrics to retune, recut, or add features
- Edit `H:\PropORACLE_main_cp`

## Pass / fail for *this* file

This prereg **passes as a study** when the candidate is written
and the sport × rung table exists. Clearing corr > 0.15 does
**not** promote. Failing corr > 0.15 also does not change live
artifacts. Promotion needs a later dated file.
