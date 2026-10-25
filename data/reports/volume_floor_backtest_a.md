# Volume-floor backtest A (ledger)

Pre-registered 2026-10-25. Face-verified OVER rungs only. No honest Demon EV — quotes were not scraped.

Rows: 748017. Dropped unverified: 3281.

Cells below need n ≥ 50. Thinner slices stay in the JSON.

| Sport | Book | Slice | HR | n | Wilson lo | vs BE | BH |
|---|---|---|---:|---:|---:|---:|---|
| MLB | Goblin OVER | nonvolume_in_gate | 0.9545 | 88 | 0.8889 | 0.1085 | False |
| MLB | Goblin OVER | volume_floor | 0.7679 | 56 | 0.6423 | -0.0781 | False |
| MLB | Goblin OVER | gate | 0.9556 | 90 | 0.8912 | 0.1096 | False |
| NBA | Goblin OVER | nonvolume_in_gate | 0.7883 | 463 | 0.7488 | -0.0577 | False |
| NBA | Goblin OVER | volume_floor | 0.6429 | 56 | 0.5119 | -0.2031 | False |
| NBA | Goblin OVER | gate | 0.7871 | 465 | 0.7476 | -0.0589 | False |
| NBA | Standard OVER | volume_floor | 0.5079 | 63 | 0.3876 | -0.0691 | False |
| NFL | Goblin OVER | nonvolume_in_gate | 0.92 | 50 | 0.8116 | 0.074 | False |
| NFL | Goblin OVER | gate | 0.9138 | 58 | 0.8136 | 0.0678 | False |
| Soccer | Standard OVER | volume_floor | 0.6452 | 93 | 0.5439 | 0.0682 | False |
| WNBA | Goblin OVER | nonvolume_in_gate | 0.7374 | 1055 | 0.7101 | -0.1086 | False |
| WNBA | Goblin OVER | volume_floor | 0.8045 | 133 | 0.729 | -0.0415 | False |
| WNBA | Goblin OVER | gate | 0.7394 | 1090 | 0.7126 | -0.1066 | False |
| WNBA | Standard OVER | volume_floor | 0.5439 | 57 | 0.4159 | -0.0331 | False |

## How to read

- Volume beats non-volume inside the same gate → volume is a filter, not a promotion.
- Floor+top2 is the Demon-track analog on sports that have lines.
- BH keep is OOS vs the approximate rung breakeven. Demon breakeven is unset.
- Do not edit the three frozen forward tracks from this table.
