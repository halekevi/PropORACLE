# NBA Goblin OVER — gates vs scraped quotes

Current-season graded_props only contain NBA from 2026-10-20. Last season's 0.668 / 0.770 book lived in the Jan–Apr projection backtest at 2.5x. This run scores the live 3-day tape at scraped quotes. Wilson lower bound must clear the quote; a point-estimate lift is not enough.

Window **2026-02-19 → 2026-10-22** · unique Goblin OVER n=43853 · OOS dates=['2026-05-08', '2026-05-09', '2026-05-10', '2026-05-11', '2026-05-12', '2026-05-13', '2026-05-14', '2026-05-15', '2026-05-17', '2026-05-18', '2026-05-19', '2026-05-20', '2026-05-22', '2026-05-23', '2026-05-24', '2026-05-25', '2026-05-26', '2026-05-28', '2026-05-30', '2026-06-03', '2026-06-05', '2026-06-08', '2026-06-10', '2026-06-13', '2026-10-20', '2026-10-21', '2026-10-22']

Per-leg breakeven: Power-2 1.40x = 84.5%. Power-3 1.65x = 84.6%. Power-3 2.00x = 79.4%. Power-3 2.50x (old catalog) = 73.7%.

## Tools

- `pysubgroup`: MISSING
- `imodels`: MISSING
- `optbinning`: MISSING
- `shap`: MISSING
- `lightgbm`: MISSING
- `sklearn`: 1.8.0

## Generic stack vs ungated vs scout

| sleeve | HR | n | Wilson lo | EV @ 1.65x | EV @ 2.00x | EV @ 2.50x | Wilson clears 1.65x |
|---|---|---|---|---|---|---|---|
| ungated Goblin OVER | 64.3% | 43853 | 0.6382 | -0.562 | -0.469 | -0.3363 | no |
| L5=5 + L10>=8 + D | 77.9% | 140 | 0.7029 | -0.2212 | -0.056 | 0.18 | no |
| proj_edge scout (matched N) | 0.0% | 0 | None | None | None | None | no |

Catalog slices whose Wilson lower bound clears 1.65x: **1** / 61
Catalog slices whose Wilson lower bound clears 2.00x: **3** / 61

## Highest hit-rate catalog slices (not the same as +EV)

| HR | n | Wilson lo | EV@1.65x | EV@2.00x | gate |
|---|---|---|---|---|---|
| 91.0% | 178 | 0.859 | 0.2438 | 0.5076 | `L5=5 + L10>=8 + line_0.5` |
| 90.4% | 52 | 0.7939 | 0.2182 | 0.4765 | `L10=10 + D_ok` |
| 90.4% | 52 | 0.7939 | 0.2182 | 0.4765 | `L10=10 + D_weak` |
| 83.4% | 253 | 0.7832 | -0.0428 | 0.1602 | `L10=10` |
| 81.7% | 360 | 0.7734 | -0.1012 | 0.0895 | `L5=5 + season_HR>70%` |
| 79.6% | 936 | 0.7689 | -0.1681 | 0.0083 | `L5>=4 + season_HR>70%` |
| 79.9% | 668 | 0.7674 | -0.1571 | 0.0217 | `L5=5 + line_0.5` |
| 77.0% | 2095 | 0.7514 | -0.247 | -0.0873 | `season_HR>70%` |
| 83.3% | 96 | 0.7463 | -0.0453 | 0.1573 | `L10>=9 + season_HR>70%` |
| 76.1% | 1080 | 0.7348 | -0.2725 | -0.1182 | `L10>=9` |
| 79.4% | 180 | 0.7296 | -0.1728 | 0.0026 | `L10>=8 + season_HR>70%` |
| 76.2% | 694 | 0.7292 | -0.2694 | -0.1144 | `L10>=9 + D_elite` |
| 86.8% | 38 | 0.7267 | 0.0805 | 0.3098 | `L10=10 + cover>=8` |
| 83.6% | 61 | 0.7239 | -0.0356 | 0.169 | `L10=10 + cover>=2` |
| 79.6% | 147 | 0.7236 | -0.1681 | 0.0083 | `L10=10 + D_elite` |

## sklearn depth-3 tree (train dates → OOS leaves)

- `l5_5<=0.5 AND line<=1 AND l10_8>0.5`  HR 100.0% n=1
- `l5_5<=0.5 AND line<=1 AND l10_8<=0.5`  HR 0.0% n=1
- `l5_5<=0.5 AND line>1 AND line<=8.75`  HR 0.0% n=1
- `l5_5<=0.5 AND line>1 AND line>8.75`  HR 0.0% n=1
- `l5_5>0.5 AND line<=1 AND minutes_high<=0.5`  HR 0.0% n=1
- `l5_5>0.5 AND line<=1 AND minutes_high>0.5`  HR 0.0% n=1
- `l5_5>0.5 AND line>1 AND line<=10.75`  HR 0.0% n=1
- `l5_5>0.5 AND line>1 AND line>10.75`  HR 0.0% n=1
- OOS leaves predicted HIT: n=6639 HR=0.6938

NBA Goblin stays shadow until a slice clears Wilson at a scraped quote.

