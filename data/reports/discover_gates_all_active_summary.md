# Discover gates — active sports rollup

# CFB discover_gates run

Window **2026-08-28 → 2026-09-12** · unique n=2911 · OOS dates=2

Early CFB window may be thin; do not auto-wire.

## Clean OOS clears (no cover / season_HR atoms)

Requires `oos_ticket_clear` **and** `oos_split_reliable` (prop history not
skewed into the global holdout — WNBA FGA failure mode).

| HR | n | OOS | base | 2-leg BE | 3-leg BE | flag | gate | cell |
|---|---|---|---|---|---|---|---|---|
| 100.0% | 33 | 100%/25 | 87.5% | 0.845 | 0.769 | split_balance,search_chance | `line_0.5` | Goblin OVER pass_td |
| 83.3% | 30 | 82%/17 | 87.5% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `D_ok` | Goblin OVER pass_td |
| 83.3% | 30 | 82%/17 | 87.5% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `D_weak` | Goblin OVER pass_td |
| 76.3% | 38 | 76%/25 | 87.5% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `line_1.5` | Goblin OVER pass_td |
| 71.4% | 63 | 77%/47 | 58.1% | 0.845 | 0.769 | split_balance,search_chance,wilson_under_3leg_breakeven | `L5>=4` | Goblin OVER rush_yds |
| 72.0% | 25 | 77%/22 | 58.1% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L10>=8` | Goblin OVER rush_yds |
| 90.0% | 20 | 89%/18 | 87.5% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `D_elite` | Goblin OVER pass_td |
| 89.5% | 19 | 94%/18 | 58.1% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5=5` | Goblin OVER rush_yds |
| 88.9% | 18 | 87%/15 | 87.5% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4` | Goblin OVER pass_td |
| 82.6% | 23 | 78%/18 | 70.3% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L10>=8` | Goblin OVER pass_yds |
| 78.3% | 23 | 88%/17 | 64.3% | 0.577 | 0.550 | split_balance,n_under_30,search_chance | `L5=5` | Standard UNDER rush_yds |
| 80.0% | 15 | 77%/13 | 48.7% | 0.577 | 0.550 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + L10>=8` | Standard UNDER pass_yds |
| 80.0% | 15 | 75%/12 | 70.3% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + L10>=8` | Goblin OVER pass_yds |
| 77.8% | 18 | 79%/14 | 58.1% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_ok` | Goblin OVER rush_yds |
| 77.8% | 18 | 79%/14 | 58.1% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_weak` | Goblin OVER rush_yds |
| 77.8% | 18 | 80%/10 | 56.0% | 0.577 | 0.550 | n_under_30,search_chance,wilson_under_3leg_breakeven | `D_weak` | Standard UNDER kick_pts |
| 73.9% | 23 | 80%/20 | 58.1% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + L10>=8` | Goblin OVER rush_yds |
| 76.5% | 17 | 73%/15 | 48.7% | 0.577 | 0.550 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L10>=8` | Standard UNDER pass_yds |
| 75.0% | 16 | 77%/13 | 48.7% | 0.577 | 0.550 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_weak` | Standard UNDER pass_yds |
| 66.7% | 36 | 77%/30 | 63.0% | 0.845 | 0.769 | split_balance,search_chance,wilson_under_3leg_breakeven | `L5>=4 + D_elite` | Goblin OVER rec_yds |
| 66.7% | 36 | 71%/24 | 70.3% | 0.845 | 0.769 | search_chance,wilson_under_3leg_breakeven | `D_elite` | Goblin OVER pass_yds |
| 73.3% | 15 | 85%/13 | 63.0% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5=5 + L10>=8` | Goblin OVER rec_yds |
| 73.3% | 15 | 89%/9 | 64.3% | 0.577 | 0.550 | n_under_30,search_chance,wilson_under_3leg_breakeven | `L5=5 + L10>=8` | Standard UNDER rush_yds |
| 65.5% | 29 | 76%/25 | 63.0% | 0.845 | 0.769 | split_balance,n_under_30,search_chance,wilson_under_3leg_breakeven | `L5=5` | Goblin OVER rec_yds |
| 61.8% | 34 | 70%/27 | 63.0% | 0.845 | 0.769 | split_balance,search_chance,wilson_under_3leg_breakeven | `L5>=4 + L10>=8` | Goblin OVER rec_yds |

## Split-imbalance flags (do not trust global OOS on these cells)

- **Standard OVER pass_td**: train 0/5 · oos 5/5 · late_prop_oos_skew: 100% of cell rows fall in global OOS (5/5); holdout is mostly this prop's own history
- **Standard OVER int**: train 0/4 · oos 4/4 · late_prop_oos_skew: 100% of cell rows fall in global OOS (4/4); holdout is mostly this prop's own history
- **Standard UNDER int**: train 0/3 · oos 3/3 · late_prop_oos_skew: 100% of cell rows fall in global OOS (3/3); holdout is mostly this prop's own history
- **Standard OVER longestreception**: train 0/84 · oos 84/84 · late_prop_oos_skew: 100% of cell rows fall in global OOS (84/84); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=0 < 15 while oos_n=84 (global split unreliable for this prop)
- **Goblin OVER pass_att**: train 0/27 · oos 27/27 · late_prop_oos_skew: 100% of cell rows fall in global OOS (27/27); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=0 < 15 while oos_n=27 (global split unreliable for this prop)
- **Standard OVER longestrush**: train 0/56 · oos 56/56 · late_prop_oos_skew: 100% of cell rows fall in global OOS (56/56); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=0 < 15 while oos_n=56 (global split unreliable for this prop)
- **Standard OVER pass_rush_yds**: train 0/11 · oos 11/11 · late_prop_oos_skew: 100% of cell rows fall in global OOS (11/11); holdout is mostly this prop's own history
- **Standard OVER pass_att**: train 0/14 · oos 14/14 · late_prop_oos_skew: 100% of cell rows fall in global OOS (14/14); holdout is mostly this prop's own history
- **Goblin OVER rush_rec_yds**: train 0/4 · oos 4/4 · late_prop_oos_skew: 100% of cell rows fall in global OOS (4/4); holdout is mostly this prop's own history
- **Standard OVER longestcompletion**: train 0/13 · oos 13/13 · late_prop_oos_skew: 100% of cell rows fall in global OOS (13/13); holdout is mostly this prop's own history
- **Standard OVER player_td**: train 0/2 · oos 2/2 · late_prop_oos_skew: 100% of cell rows fall in global OOS (2/2); holdout is mostly this prop's own history
- **Goblin OVER longestrush**: train 0/1 · oos 1/1 · late_prop_oos_skew: 100% of cell rows fall in global OOS (1/1); holdout is mostly this prop's own history
- **Standard UNDER rush_rec_yds**: train 2/29 · oos 27/29 · late_prop_oos_skew: 93% of cell rows fall in global OOS (27/29); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=2 < 15 while oos_n=27 (global split unreliable for this prop)
- **Goblin OVER passcompletions**: train 2/25 · oos 23/25 · late_prop_oos_skew: 92% of cell rows fall in global OOS (23/25); holdout is mostly this prop's own history
- **Standard OVER rec**: train 3/35 · oos 32/35 · late_prop_oos_skew: 91% of cell rows fall in global OOS (32/35); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=3 < 15 while oos_n=32 (global split unreliable for this prop)
- **Goblin OVER rush_td**: train 2/20 · oos 18/20 · late_prop_oos_skew: 90% of cell rows fall in global OOS (18/20); holdout is mostly this prop's own history
- **Standard OVER kick_pts**: train 2/16 · oos 14/16 · late_prop_oos_skew: 88% of cell rows fall in global OOS (14/16); holdout is mostly this prop's own history
- **Standard OVER rush_att**: train 2/16 · oos 14/16 · late_prop_oos_skew: 88% of cell rows fall in global OOS (14/16); holdout is mostly this prop's own history
- **Standard UNDER pass_rush_yds**: train 5/33 · oos 28/33 · late_prop_oos_skew: 85% of cell rows fall in global OOS (28/33); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=5 < 15 while oos_n=28 (global split unreliable for this prop)
- **Standard UNDER longestreception**: train 14/85 · oos 71/85 · late_prop_oos_skew: 84% of cell rows fall in global OOS (71/85); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=14 < 15 while oos_n=71 (global split unreliable for this prop)

Exports: `discover_gates_cfb_export.csv`, `discover_gates_cfb_ranked.csv`, `discover_gates_cfb_sanity.json`

```text
py -3.14 scripts/discover_gates_sport.py --sport CFB
```


