# Soccer discover_gates run

Window **2026-03-10 → 2026-10-04** · unique n=28359 · OOS dates=39

Keep Shots/SOT/Saves already gated; look for unseen props.

## Clean OOS clears (no cover / season_HR atoms)

Requires `oos_ticket_clear` **and** `oos_split_reliable` (prop history not
skewed into the global holdout — WNBA FGA failure mode).

| HR | n | OOS | base | 2-leg BE | 3-leg BE | flag | gate | cell |
|---|---|---|---|---|---|---|---|---|
| 75.0% | 954 | 100%/59 | 58.3% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `line_0.5` | Goblin OVER fouls |
| 79.6% | 98 | 79%/67 | 65.5% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L5>=4` | Goblin OVER sog |
| 72.0% | 2640 | 71%/1798 | 57.7% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `line_0.5` | Goblin OVER shots |
| 73.8% | 210 | 73%/121 | 62.8% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `line_1.5` | Goblin OVER saves |
| 76.8% | 69 | 78%/51 | 57.7% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L5=5` | Goblin OVER shots |
| 76.9% | 65 | 80%/44 | 57.7% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_ok` | Goblin OVER shots |
| 76.9% | 65 | 80%/44 | 57.7% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_weak` | Goblin OVER shots |
| 73.2% | 56 | 71%/42 | 57.7% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L5>=4 + L10>=8` | Goblin OVER shots |
| 71.9% | 64 | 73%/48 | 57.7% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L10>=8` | Goblin OVER shots |
| 95.2% | 21 | 93%/15 | 65.5% | 0.845 | 0.769 | n_under_30,search_chance | `L5=5` | Goblin OVER sog |
| 94.7% | 19 | 92%/13 | 65.5% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5=5 + line_0.5` | Goblin OVER sog |
| 92.0% | 25 | 100%/18 | 65.5% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + L10>=8` | Goblin OVER sog |
| 89.7% | 29 | 100%/21 | 65.5% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L10>=8` | Goblin OVER sog |
| 84.6% | 39 | 86%/29 | 57.7% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `L5=5 + line_0.5` | Goblin OVER shots |
| 89.5% | 19 | 85%/13 | 62.8% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_ok` | Goblin OVER saves |
| 89.5% | 19 | 85%/13 | 62.8% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_weak` | Goblin OVER saves |
| 85.7% | 28 | 90%/21 | 65.5% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_ok` | Goblin OVER sog |
| 85.7% | 28 | 90%/21 | 65.5% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_weak` | Goblin OVER sog |
| 67.9% | 1605 | 74%/1055 | 65.5% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `line_0.5` | Goblin OVER sog |
| 66.7% | 618 | 76%/129 | 62.6% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `line_0.5` | Goblin OVER fouls_drawn |
| 66.7% | 369 | 71%/286 | 65.5% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `D_ok` | Goblin OVER sog |
| 66.7% | 369 | 71%/286 | 65.5% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `D_weak` | Goblin OVER sog |
| 65.6% | 378 | 88%/25 | 58.3% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `D_elite` | Goblin OVER fouls |
| 81.0% | 21 | 81%/16 | 62.8% | 0.845 | 0.769 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5=5` | Goblin OVER saves |
| 62.6% | 468 | 71%/279 | 65.5% | 0.845 | 0.769 | wilson_under_3leg_breakeven | `D_elite` | Goblin OVER sog |

## Split-imbalance flags (do not trust global OOS on these cells)

- **Standard OVER saves_combo**: train 0/5 · oos 5/5 · late_prop_oos_skew: 100% of cell rows fall in global OOS (5/5); holdout is mostly this prop's own history
- **Standard UNDER saves_combo**: train 0/16 · oos 16/16 · late_prop_oos_skew: 100% of cell rows fall in global OOS (16/16); holdout is mostly this prop's own history
- **Standard UNDER sog**: train 4/32 · oos 28/32 · late_prop_oos_skew: 88% of cell rows fall in global OOS (28/32); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=4 < 15 while oos_n=28 (global split unreliable for this prop)
- **Standard UNDER saves**: train 270/384 · oos 114/384 · early_prop_train_skew: 70% of cell rows fall in global train (270/384)
- **Standard UNDER shots**: train 336/473 · oos 137/473 · early_prop_train_skew: 71% of cell rows fall in global train (336/473)
- **Goblin OVER goal_assist**: train 436/563 · oos 127/563 · early_prop_train_skew: 77% of cell rows fall in global train (436/563)
- **Goblin OVER fouls_drawn**: train 600/759 · oos 159/759 · early_prop_train_skew: 79% of cell rows fall in global train (600/759)
- **Standard OVER sog_combo**: train 6/7 · oos 1/7 · early_prop_train_skew: 86% of cell rows fall in global train (6/7)
- **Standard OVER sog**: train 194/213 · oos 19/213 · early_prop_train_skew: 91% of cell rows fall in global train (194/213)
- **Goblin OVER fouls**: train 1461/1542 · oos 81/1542 · early_prop_train_skew: 95% of cell rows fall in global train (1461/1542)
- **Standard OVER saves**: train 539/566 · oos 27/566 · early_prop_train_skew: 95% of cell rows fall in global train (539/566)
- **Goblin OVER goals**: train 334/349 · oos 15/349 · early_prop_train_skew: 96% of cell rows fall in global train (334/349)
- **Standard OVER shots**: train 4447/4479 · oos 32/4479 · early_prop_train_skew: 99% of cell rows fall in global train (4447/4479)
- **Standard OVER goals**: train 2236/2238 · oos 2/2238 · early_prop_train_skew: 100% of cell rows fall in global train (2236/2238)
- **Standard OVER goal_assist**: train 2832/2833 · oos 1/2833 · early_prop_train_skew: 100% of cell rows fall in global train (2832/2833)
- **Standard OVER passes**: train 90/90 · oos 0/90 · early_prop_train_skew: 100% of cell rows fall in global train (90/90)
- **Goblin OVER tackles**: train 10/10 · oos 0/10 · early_prop_train_skew: 100% of cell rows fall in global train (10/10)
- **Goblin OVER goals_allowed**: train 12/12 · oos 0/12 · early_prop_train_skew: 100% of cell rows fall in global train (12/12)
- **Standard OVER clearances**: train 20/20 · oos 0/20 · early_prop_train_skew: 100% of cell rows fall in global train (20/20)
- **Standard OVER tackles**: train 13/13 · oos 0/13 · early_prop_train_skew: 100% of cell rows fall in global train (13/13)

Exports: `discover_gates_soccer_export.csv`, `discover_gates_soccer_ranked.csv`, `discover_gates_soccer_sanity.json`

```text
py -3.14 scripts/discover_gates_sport.py --sport Soccer
```

