# Player-history shrinkage + reusable deciles (May–June)

## Data note

`graded_props` has **no** persisted `player_prior_n` / games-played field.
Loader now pass-throughs extras if present; for this run `rank_by=player_shrunk`
used **walk-forward prop counts** per `player_id` (`history=walkforward_count`).

## Direction deciles (`standard_by_direction.csv`)

OVER carries the overconfidence hook (decile 10: pred 0.99 → hit 0.35).
UNDER stays nearer calibrated through the mid pack; top UNDER still overshoots
(pred 0.89 → hit 0.53) but less violently than OVER.

## Player-shrunk vs raw (Standard, May–June, n=61 days)

| Shape | Rank | Mean raw conf | WR | Wilson | BE | Clears? |
|--|--|--|--|--|--|--|
| 3-leg ALL | model_prob | 93.8% | 14.8% | 8.0% | 16.7% | No |
| 3-leg ALL | **player_shrunk** | 67.6% | **42.6%** | **31.0%** | 16.7% | **Yes / Yes** |
| 4-leg ALL | **player_shrunk** | 67.2% | **34.4%** | **23.7%** | 10.0% | **Yes / Yes** |
| 3-leg MLB | player_shrunk | 60.9% | 13.1% | 6.8% | 16.7% | No |

Cross-sport deep Standard **reopens** under player-history shrinkage (stronger than
decile recalibration alone). MLB-only still fails — mean selected `player_prior_n`
only ~22 there vs ~75 cross-sport.

## CLI

```
py -3 scripts/build_leg_correlation_matrix.py --skip-matrix `
  --build-calibration-deciles data/reports/standard_by_direction.csv `
  --calibration-group-by direction --calibration-pick-type standard

py -3 scripts/build_leg_correlation_matrix.py --skip-matrix `
  --top-confidence-n-legs 3 --top-confidence-pick-type standard `
  --top-confidence-rank-by player_shrunk --player-shrinkage-prior 150 `
  --payout-table data/reports/composition_payout_n_correct.json
```
