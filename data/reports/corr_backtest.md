
## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=MLB rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=9.8%  wilson_lower=4.6%
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
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? True  (wilson)? False
- ev_raw=0.1803  ev_conservative=-0.3023

Population row MLB 3-leg all_standard: n=223 WR=14.3% wilson=10.3%
Selectivity lift vs population WR: +5.3 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_standard` sport=ALL rank_by=calibrated
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=21.3%  wilson_lower=12.9%
- payout_x=3.0  breakeven WR=33.3%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.3607  ev_conservative=-0.613

Population (same n_legs+tier_mix, all sports in CSV): n=1624 WR=22.0%
Selectivity lift vs population WR: -0.7 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 2-leg `all_standard` sport=ALL rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=26.2%  wilson_lower=16.8%
- payout_x=3.0  breakeven WR=33.3%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.2131  ev_conservative=-0.4948

Population (same n_legs+tier_mix, all sports in CSV): n=1624 WR=22.0%
Selectivity lift vs population WR: +4.2 pp

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=ALL rank_by=player_shrunk
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=42.6%  wilson_lower=31.0%
- selected mean raw model_prob=67.6%  mean rank_signal=0.8278803278688525
- player_shrunk prior_strength=150.0 history=walkforward_count mean_player_prior_n=75.04918032786885
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=1.5573770491803276  ev_conservative=0.8609017134780461

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=ALL rank_by=model_prob
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=14.8%  wilson_lower=8.0%
- selected mean raw model_prob=93.8%  mean rank_signal=0.9377704918032786
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.11475409836065575  ev_conservative=-0.5223352786366298

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 4-leg `all_standard` sport=ALL rank_by=player_shrunk
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=34.4%  wilson_lower=23.7%
- selected mean raw model_prob=67.2%  mean rank_signal=0.8166049180327869
- player_shrunk prior_strength=150.0 history=walkforward_count mean_player_prior_n=75.1360655737705
- payout_x=10.0  breakeven WR=10.0%
- clears breakeven (raw)? True  (wilson)? True
- ev_raw=2.442622950819672  ev_conservative=1.374733903986321

## Top-confidence construction simulation

Greedy daily tickets from highest-ranked eligible legs (Lever A + thin-competition).
ticket_hit = all legs hit (sweep) -- comparable to N-correct payout, not Flex partial.

- shape: 3-leg `all_standard` sport=MLB rank_by=player_shrunk
- window: 2026-05-01 .. 2026-06-30
- n=61  WR=13.1%  wilson_lower=6.8%
- selected mean raw model_prob=60.9%  mean rank_signal=0.6271639344262294
- player_shrunk prior_strength=150.0 history=walkforward_count mean_player_prior_n=22.016393442622952
- payout_x=6.0  breakeven WR=16.7%
- clears breakeven (raw)? False  (wilson)? False
- ev_raw=-0.21311475409836067  ev_conservative=-0.5921202312479141
