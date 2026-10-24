# Gate discovery — all sports vs scraped quotes

Search target is EV at the real quote, not hit rate. Nothing here is wired into keep_gates. NBA G70 stays shadow.

Face filter (2026-10-24): rows on sport-dates whose graded faces were default labels (zero Goblin/Demon, e.g. NFL 09-09..10-19) or Standard ladders are dropped before scoring. Earlier NFL figures in this folder (Goblin OVER n=576, Standard UNDER 84.6% n=136) were computed on those labels and are superseded by the per-book `face_filter` block.

## Tools

- `pysubgroup`: present
- `imodels`: present
- `optbinning`: 1.0.0
- `sklearn`: 1.8.0
- `shap`: 0.54.1
- `lightgbm`: 4.7.0

Goblin breakeven: Power-2 1.40x = 84.5%. Power-3 1.65x = 84.6%. Power-3 2.00x = 79.4%. Power-3 2.50x = 73.7%.
Standard breakeven: Power-2 3x = 57.7%. Power-3 6x = 55.0%.

## NFL

### Goblin OVER  n=576  dates=2026-09-20 → 2026-10-22

| sleeve | HR | n | Wilson lo | EV @ 1.65x | EV @ 2.00x | split |
|---|---|---|---|---|---|---|
| ungated | 71.2% | 576 | 0.6735 | -0.4049 | -0.2787 |  |
| L5=5 + L10>=8 + D | 85.0% | 20 | 0.6396 | 0.0133 | 0.2282 |  |
| L5=5 + L10>=8 | 78.7% | 61 | 0.6688 | -0.196 | -0.0255 |  |

Wilson-clears primary: **1** `['line_0.5']`
Wilson-clears observe: **1** `['line_0.5']`
BH-keep vs quote: `['line_0.5']`

| HR | n | Wilson lo | train_n | oos_n | flag | gate |
|---|---|---|---|---|---|---|
| 97.0% | 67 | 0.8975 | 58 | 9 |  | `line_0.5` |
| 79.2% | 77 | 0.6888 | 68 | 9 |  | `L5>=4 + D_ok` |
| 79.2% | 77 | 0.6888 | 68 | 9 |  | `L5>=4 + D_weak` |
| 74.7% | 225 | 0.686 | 206 | 19 |  | `L5>=4` |
| 74.8% | 206 | 0.6841 | 184 | 22 |  | `D_ok` |
| 74.8% | 206 | 0.6841 | 184 | 22 |  | `D_weak` |
| 79.0% | 62 | 0.6736 | 57 | 5 |  | `line_1.5` |
| 71.2% | 576 | 0.6735 | 512 | 64 |  | `all` |

### Standard OVER  n=290  dates=2026-09-20 → 2026-10-22

| sleeve | HR | n | Wilson lo | EV @ 3x | EV @ 6x | split |
|---|---|---|---|---|---|---|
| ungated | 49.7% | 290 | 0.4394 | -0.2602 | -0.2652 |  |
| L5=5 + L10>=8 + D | 80.0% | 5 | 0.3755 | 0.92 | 2.072 |  |
| L5=5 + L10>=8 | 55.6% | 9 | 0.2666 | -0.0739 | 0.0291 |  |

Wilson-clears primary: **0** `['—']`
Wilson-clears observe: **0** `['—']`
BH-keep vs quote: `['—']`

| HR | n | Wilson lo | train_n | oos_n | flag | gate |
|---|---|---|---|---|---|---|
| 57.3% | 103 | 0.4764 | 83 | 20 |  | `D_ok` |
| 57.3% | 103 | 0.4764 | 83 | 20 |  | `D_weak` |
| 68.2% | 22 | 0.4732 | 18 | 4 |  | `L5>=4 + D_ok` |
| 68.2% | 22 | 0.4732 | 18 | 4 |  | `L5>=4 + D_weak` |
| 49.7% | 290 | 0.4394 | 242 | 48 |  | `all` |
| 54.4% | 68 | 0.4266 | 62 | 6 |  | `L5>=4` |
| 46.4% | 166 | 0.3897 | 166 | 0 |  | `cover>=1` |
| 45.5% | 143 | 0.3752 | 143 | 0 |  | `cover>=2` |

### Standard UNDER  n=211  dates=2026-09-20 → 2026-10-22

| sleeve | HR | n | Wilson lo | EV @ 3x | EV @ 6x | split |
|---|---|---|---|---|---|---|
| ungated | 49.3% | 211 | 0.4262 | -0.2711 | -0.2815 |  |
| L5=5 + L10>=8 + D | 0.0% | 2 | 0.0 | -1.0 | -1.0 |  |
| L5=5 + L10>=8 | 38.1% | 21 | 0.2075 | -0.5645 | -0.6682 |  |

Wilson-clears primary: **0** `['—']`
Wilson-clears observe: **0** `['—']`
BH-keep vs quote: `['—']`

| HR | n | Wilson lo | train_n | oos_n | flag | gate |
|---|---|---|---|---|---|---|
| 49.3% | 211 | 0.4262 | 197 | 14 |  | `all` |
| 51.0% | 104 | 0.4149 | 95 | 9 |  | `line_ge20` |
| 51.3% | 74 | 0.4018 | 69 | 5 |  | `D_ok` |
| 51.3% | 74 | 0.4018 | 69 | 5 |  | `D_elite` |
| 48.1% | 129 | 0.3962 | 119 | 10 |  | `line_ge10` |
| 45.5% | 132 | 0.3721 | 132 | 0 |  | `cover>=1` |
| 45.5% | 110 | 0.3646 | 110 | 0 |  | `cover>=2` |
| 48.1% | 52 | 0.351 | 52 | 0 |  | `cover>=8` |

## Decision

NBA Goblin-70 stays shadow. Do not wire a catalog slice that only clears because it is a 0.5-line counting prop or because train_n is near zero.

Scout (proj_edge) vs generic gate at scraped quotes: `py -3.14 -m ml.nba_scout_quotes` → `nba_scout_quotes.md` in this folder.

