# Tennis discover_gates run

Window **2026-04-26 → 2026-10-07** · unique n=9853 · OOS dates=37

Serve UNDER Aces/DF already faded post-backfill; sanity zero rates.

## Clean OOS clears (no cover / season_HR atoms)

Requires `oos_ticket_clear` **and** `oos_split_reliable` (prop history not
skewed into the global holdout — WNBA FGA failure mode).

| HR | n | OOS | base | gate | cell |
|---|---|---|---|---|---|
| 87.5% | 56 | 90%/52 | 70.1% | `L5=5 + D_elite` | Goblin OVER games_won |
| 80.7% | 119 | 86%/64 | 70.1% | `L5=5 + L10>=8` | Goblin OVER games_won |
| 78.6% | 145 | 86%/77 | 70.1% | `L5=5` | Goblin OVER games_won |
| 75.4% | 240 | 79%/210 | 70.1% | `D_elite` | Goblin OVER games_won |
| 76.1% | 130 | 80%/113 | 70.1% | `L5>=4 + D_elite` | Goblin OVER games_won |
| 72.4% | 427 | 73%/226 | 70.1% | `L5>=4` | Goblin OVER games_won |
| 72.0% | 435 | 73%/138 | 70.1% | `L10>=8` | Goblin OVER games_won |
| 79.0% | 62 | 82%/22 | 70.1% | `L10=10` | Goblin OVER games_won |
| 72.4% | 246 | 72%/120 | 70.1% | `L5>=4 + L10>=8` | Goblin OVER games_won |
| 70.5% | 210 | 78%/85 | 70.1% | `L10>=9` | Goblin OVER games_won |
| 74.3% | 70 | 77%/62 | 70.1% | `L10>=9 + D_elite` | Goblin OVER games_won |
| 70.7% | 150 | 71%/121 | 59.5% | `D_weak` | Standard UNDER match_total_games |
| 70.2% | 158 | 73%/148 | 52.8% | `D_elite` | Standard OVER games_won |
| 71.1% | 97 | 77%/81 | 70.1% | `L10>=8 + D_elite` | Goblin OVER games_won |
| 72.5% | 40 | 76%/38 | 52.8% | `L5=5 + D_elite` | Standard OVER games_won |
| 93.8% | 16 | 93%/14 | 70.1% | `L10=10 + D_elite` | Goblin OVER games_won |
| 89.5% | 19 | 100%/14 | 52.8% | `L10=10` | Standard OVER games_won |
| 68.4% | 1351 | 71%/461 | 70.1% | `line_ge10` | Goblin OVER games_won |
| 85.7% | 21 | 73%/11 | 70.1% | `L5=5 + D_ok` | Goblin OVER games_won |
| 85.7% | 21 | 73%/11 | 70.1% | `L5=5 + D_weak` | Goblin OVER games_won |
| 87.5% | 16 | 75%/8 | 70.1% | `L5=5 + L10>=8 + D_ok` | Goblin OVER games_won |
| 80.7% | 31 | 75%/16 | 59.6% | `L10=10` | Goblin OVER match_total_games |
| 67.6% | 108 | 71%/100 | 52.8% | `L5>=4 + D_elite` | Standard OVER games_won |
| 64.7% | 184 | 70%/90 | 59.5% | `L5>=4` | Standard UNDER match_total_games |
| 72.7% | 33 | 79%/29 | 52.8% | `L10>=9 + D_elite` | Standard OVER games_won |

## Split-imbalance flags (do not trust global OOS on these cells)

- **Standard OVER double_faults**: train 122/122 · oos 0/122 · early_prop_train_skew: 100% of cell rows fall in global train (122/122)
- **Standard OVER aces**: train 120/120 · oos 0/120 · early_prop_train_skew: 100% of cell rows fall in global train (120/120)
- **Standard UNDER aces**: train 28/28 · oos 0/28 · early_prop_train_skew: 100% of cell rows fall in global train (28/28)
- **Standard UNDER double_faults**: train 8/8 · oos 0/8 · early_prop_train_skew: 100% of cell rows fall in global train (8/8)
- **Goblin OVER double_faults**: train 750/750 · oos 0/750 · early_prop_train_skew: 100% of cell rows fall in global train (750/750)
- **Goblin OVER aces**: train 692/692 · oos 0/692 · early_prop_train_skew: 100% of cell rows fall in global train (692/692)
- **Standard UNDER total_tie_breaks**: train 1/1 · oos 0/1 · early_prop_train_skew: 100% of cell rows fall in global train (1/1)

Exports: `discover_gates_tennis_export.csv`, `discover_gates_tennis_ranked.csv`, `discover_gates_tennis_sanity.json`

```text
py -3.14 scripts/discover_gates_sport.py --sport Tennis
```

