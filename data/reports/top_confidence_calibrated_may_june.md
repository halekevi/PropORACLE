# Calibrated vs raw model_prob top-confidence (Standard)

Calibration knots: 10 from calibration_standard_goblin_deciles_may_june.csv

## Thread closeout

See `thread_closeout_wr_bleed.md`.

**Shipped:** Goblin mixer floor `min_hit_prob_actionable` **0.50 → 0.35** (`pipeline_read_checklist.json`).
**Standard:** player-history shrinkage (separate fix — inverting tail, not a plateau).
**Residual June 2 gap after floor retune (~140 vs 238)** = second same-day filter, not a third mystery.

## Top-confidence construction simulation


## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_standard` sport=MLB rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=24.6%  wilson_lower=15.5%
- selected mean raw model_prob=75.4%  mean rank_signal=0.754
- payout_x=3.0  breakeven WR=33.3%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.2623  ev_conservative=-0.5348

Population row MLB 2-leg all_standard: n=381 WR=28.3% wilson=24.1%
Selectivity lift vs population WR: -3.8 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_standard` sport=MLB rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=32.8%  wilson_lower=22.3%
- selected mean raw model_prob=52.7%  mean rank_signal=0.4932
- payout_x=3.0  breakeven WR=33.3%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.0164  ev_conservative=-0.33

Population row MLB 2-leg all_standard: n=381 WR=28.3% wilson=24.1%
Selectivity lift vs population WR: +4.4 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=MLB rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=9.8%  wilson_lower=4.6%
- selected mean raw model_prob=75.1%  mean rank_signal=0.7506
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.4098  ev_conservative=-0.7248

Population row MLB 3-leg all_standard: n=223 WR=14.3% wilson=10.3%
Selectivity lift vs population WR: -4.5 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=MLB rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=19.7%  wilson_lower=11.6%
- selected mean raw model_prob=52.7%  mean rank_signal=0.4932
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? True  (wilson)? False
- ev_raw=0.1803  ev_conservative=-0.3023

Population row MLB 3-leg all_standard: n=223 WR=14.3% wilson=10.3%
Selectivity lift vs population WR: +5.3 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=MLB rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=4.9%  wilson_lower=1.7%
- selected mean raw model_prob=74.8%  mean rank_signal=0.7478
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.5082  ev_conservative=-0.8313

Population row MLB 4-leg all_standard: n=150 WR=7.3% wilson=4.1%
Selectivity lift vs population WR: -2.4 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=MLB rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=9.8%  wilson_lower=4.6%
- selected mean raw model_prob=52.8%  mean rank_signal=0.4931
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.0164  ev_conservative=-0.5414

Population row MLB 4-leg all_standard: n=150 WR=7.3% wilson=4.1%
Selectivity lift vs population WR: +2.5 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 5-leg `all_standard` sport=MLB rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=1.6%  wilson_lower=0.3%
- selected mean raw model_prob=74.6%  mean rank_signal=0.7455
- payout_x=20.0  breakeven WR=5.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.6721  ev_conservative=-0.942

Population row MLB 5-leg all_standard: n=109 WR=5.5% wilson=2.5%
Selectivity lift vs population WR: -3.9 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 5-leg `all_standard` sport=MLB rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=3.3%  wilson_lower=0.9%
- selected mean raw model_prob=52.8%  mean rank_signal=0.493
- payout_x=20.0  breakeven WR=5.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3443  ev_conservative=-0.8192

Population row MLB 5-leg all_standard: n=109 WR=5.5% wilson=2.5%
Selectivity lift vs population WR: -2.2 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 6-leg `all_standard` sport=MLB rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=1.6%  wilson_lower=0.3%
- selected mean raw model_prob=74.3%  mean rank_signal=0.7434
- payout_x=40.0  breakeven WR=2.5%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3443  ev_conservative=-0.884

Population row MLB 6-leg all_standard: n=73 WR=2.7% wilson=0.8%
Selectivity lift vs population WR: -1.1 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 6-leg `all_standard` sport=MLB rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=0.0%  wilson_lower=0.0%
- selected mean raw model_prob=52.7%  mean rank_signal=0.4929
- payout_x=40.0  breakeven WR=2.5%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-1.0  ev_conservative=-1.0

Population row MLB 6-leg all_standard: n=73 WR=2.7% wilson=0.8%
Selectivity lift vs population WR: -2.7 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=ALL rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=14.8%  wilson_lower=8.0%
- selected mean raw model_prob=93.8%  mean rank_signal=0.9378
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.1148  ev_conservative=-0.5223

Population (same n_legs+tier_mix, all sports in CSV): n=966 WR=11.1%
Selectivity lift vs population WR: +3.7 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=ALL rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=11.5%  wilson_lower=5.7%
- selected mean raw model_prob=52.5%  mean rank_signal=0.4937
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3115  ev_conservative=-0.6597

Population (same n_legs+tier_mix, all sports in CSV): n=966 WR=11.1%
Selectivity lift vs population WR: +0.4 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=ALL rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=8.2%  wilson_lower=3.5%
- selected mean raw model_prob=93.5%  mean rank_signal=0.9353
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.1803  ev_conservative=-0.6448

Population (same n_legs+tier_mix, all sports in CSV): n=635 WR=5.8%
Selectivity lift vs population WR: +2.4 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=ALL rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=3.3%  wilson_lower=0.9%
- selected mean raw model_prob=52.5%  mean rank_signal=0.4936
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.6721  ev_conservative=-0.9096

Population (same n_legs+tier_mix, all sports in CSV): n=635 WR=5.8%
Selectivity lift vs population WR: -2.5 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 6-leg `all_standard` sport=ALL rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=1.6%  wilson_lower=0.3%
- selected mean raw model_prob=93.0%  mean rank_signal=0.9296
- payout_x=40.0  breakeven WR=2.5%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3443  ev_conservative=-0.884

Population (same n_legs+tier_mix, all sports in CSV): n=277 WR=1.1%
Selectivity lift vs population WR: +0.6 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 6-leg `all_standard` sport=ALL rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=0.0%  wilson_lower=0.0%
- selected mean raw model_prob=52.5%  mean rank_signal=0.4935
- payout_x=40.0  breakeven WR=2.5%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-1.0  ev_conservative=-1.0

Population (same n_legs+tier_mix, all sports in CSV): n=277 WR=1.1%
Selectivity lift vs population WR: -1.1 pp
