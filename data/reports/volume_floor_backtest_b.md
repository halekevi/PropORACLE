# Volume-floor backtest B (nflverse, no lines)

How often a top-2 volume player's next game meets or beats his L5 minimum. No price attached.

| Window | Family | Role | HR | n | Wilson lo | thin |
|---|---|---|---:|---:|---:|---|
| headline_2021_2025 | rush_attempts | leader | 0.8512 | 1754 | 0.8338 | False |
| headline_2021_2025 | rush_attempts | top2 | 0.8532 | 2840 | 0.8397 | False |
| headline_2021_2025 | receptions | leader | 0.8513 | 1849 | 0.8343 | False |
| headline_2021_2025 | receptions | top2 | 0.8661 | 3659 | 0.8547 | False |
| headline_2021_2025 | receptions | top2_share_ge_15 | 0.863 | 3416 | 0.8511 | False |
| headline_2021_2025 | receptions | WR_top2 | 0.8597 | 2658 | 0.8459 | False |
| headline_2021_2025 | receptions | TE_top2 | 0.891 | 734 | 0.8664 | False |
| headline_2021_2025 | receptions | RB_top2 | 0.8614 | 267 | 0.8148 | False |
| headline_2021_2025 | rush_attempts | RB_leader | 0.8512 | 1754 | 0.8338 | False |
| extra_2026 | rush_attempts | leader | 0.7895 | 38 | 0.6365 | True |
| extra_2026 | rush_attempts | top2 | 0.7778 | 54 | 0.6506 | False |
| extra_2026 | receptions | leader | 0.9412 | 34 | 0.8091 | True |
| extra_2026 | receptions | top2 | 0.9143 | 70 | 0.8253 | False |
| extra_2026 | receptions | top2_share_ge_15 | 0.9231 | 65 | 0.8322 | False |
| extra_2026 | receptions | WR_top2 | 0.9412 | 51 | 0.8408 | False |
| extra_2026 | receptions | TE_top2 | 0.8235 | 17 | 0.5897 | True |
| extra_2026 | receptions | RB_top2 | 1.0 | 2 | 0.3424 | True |
| extra_2026 | rush_attempts | RB_leader | 0.7895 | 38 | 0.6365 | True |

## How to read

- 80%+ for target-leader / top-2 receptions → Demon tracks have a strong prior.
- Near 60% → Skattebo and Tucker were mostly a good day.
- This is a probability, not EV. Only the forward Demon logs can promote.
