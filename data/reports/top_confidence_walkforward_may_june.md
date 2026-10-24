# Walk-forward empirical top-confidence (Standard, May-June 2026)

Empirical rates use only dates **strictly before** the decision day (warmed up from pre-window same-family history). No lookahead.

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_standard` sport=MLB rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=29.5%  wilson_lower=19.6%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=3.0  breakeven WR=33.3%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.1148  ev_conservative=-0.4134

Population row MLB 2-leg all_standard: n=381 WR=28.3% wilson=24.1%
Selectivity lift vs population WR: +1.2 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=MLB rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=11.5%  wilson_lower=5.7%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3115  ev_conservative=-0.6597

Population row MLB 3-leg all_standard: n=223 WR=14.3% wilson=10.3%
Selectivity lift vs population WR: -2.9 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=MLB rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=9.8%  wilson_lower=4.6%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.0164  ev_conservative=-0.5414

Population row MLB 4-leg all_standard: n=150 WR=7.3% wilson=4.1%
Selectivity lift vs population WR: +2.5 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 5-leg `all_standard` sport=MLB rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=4.9%  wilson_lower=1.7%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=20.0  breakeven WR=5.0%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.0164  ev_conservative=-0.6627

Population row MLB 5-leg all_standard: n=109 WR=5.5% wilson=2.5%
Selectivity lift vs population WR: -0.6 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 6-leg `all_standard` sport=MLB rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=1.6%  wilson_lower=0.3%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=40.0  breakeven WR=2.5%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3443  ev_conservative=-0.884

Population row MLB 6-leg all_standard: n=73 WR=2.7% wilson=0.8%
Selectivity lift vs population WR: -1.1 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_standard` sport=ALL rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=50.8%  wilson_lower=38.6%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=3.0  breakeven WR=33.3%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=0.5246  ev_conservative=0.1581

Population (same n_legs+tier_mix, all sports in CSV): n=1624 WR=22.0%
Selectivity lift vs population WR: +28.8 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=ALL rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=47.5%  wilson_lower=35.5%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=1.8525  ev_conservative=1.1318

Population (same n_legs+tier_mix, all sports in CSV): n=966 WR=11.1%
Selectivity lift vs population WR: +36.5 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=ALL rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=34.4%  wilson_lower=23.8%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=2.4426  ev_conservative=1.3747

Population (same n_legs+tier_mix, all sports in CSV): n=635 WR=5.8%
Selectivity lift vs population WR: +28.6 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 5-leg `all_standard` sport=ALL rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=24.6%  wilson_lower=15.5%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=20.0  breakeven WR=5.0%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=3.918  ev_conservative=2.1013

Population (same n_legs+tier_mix, all sports in CSV): n=467 WR=3.4%
Selectivity lift vs population WR: +21.2 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 6-leg `all_standard` sport=ALL rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=13.1%  wilson_lower=6.8%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=40.0  breakeven WR=2.5%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=4.2459  ev_conservative=1.7192

Population (same n_legs+tier_mix, all sports in CSV): n=277 WR=1.1%
Selectivity lift vs population WR: +12.0 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_goblin` sport=MLB rank_by=empirical
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=44.3%  wilson_lower=32.5%
- walk-forward empiric_min_n=20 (skipped days with no prior history: 0)
- payout_x=2.2  breakeven WR=45.5%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.0262  ev_conservative=-0.2848

Population row MLB 2-leg all_goblin: n=1513 WR=38.9% wilson=36.4%
Selectivity lift vs population WR: +5.4 pp


## Standard vs Goblin calibration (May-June, model_prob deciles)

```
STANDARD
 decile     n  mean_pred  mean_hit       gap
      1 10153   0.031021  0.124101  0.093080
      2 11138   0.120481  0.122553  0.002073
      3  3946   0.226212  0.261024  0.034812
      4  5426   0.304813  0.403059  0.098246
      5  7666   0.409299  0.457083  0.047784
      6  7676   0.478142  0.483325  0.005183
      7  7975   0.525188  0.494169 -0.031019
      8  7437   0.563366  0.484738 -0.078627
      9  7574   0.720407  0.429100 -0.291308
     10  7666   0.973932  0.359118 -0.614813

GOBLIN
 decile     n  mean_pred  mean_hit       gap
      1  8704   0.285784  0.510915  0.225131
      2  8722   0.504510  0.613047  0.108538
      3 13735   0.543791  0.628249  0.084458
      4  6590   0.559573  0.587405  0.027832
      5  5849   0.569136  0.597367  0.028231
      6  8690   0.611903  0.627733  0.015830
      7  8608   0.699282  0.652068 -0.047214
      8  8761   0.770105  0.620135 -0.149971
      9 17339   0.966651  0.619009 -0.347642
```

Top-decile Standard: pred=0.974 hit=0.359 gap=-0.615
Top-decile Goblin: pred=0.967 hit=0.619 gap=-0.348
Standard top-deciles hook downward (pred up, hit flat/down)? **True**
