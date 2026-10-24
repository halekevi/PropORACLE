# WNBA discover_gates run

Window **2026-05-01 → 2026-10-07** · unique n=43217 · OOS dates=33

Full-game WNBA only (not 1H/1Q).

## Clean OOS clears (no cover / season_HR atoms)

Requires `oos_ticket_clear` **and** `oos_split_reliable` (prop history not
skewed into the global holdout — WNBA FGA failure mode).

| HR | n | OOS | base | gate | cell |
|---|---|---|---|---|---|
| 80.3% | 71 | 83%/29 | 66.9% | `L10>=9 + D_elite` | Goblin OVER threes |
| 75.6% | 225 | 79%/72 | 66.9% | `L5=5` | Goblin OVER threes |
| 82.6% | 46 | 73%/11 | 65.8% | `L10>=9 + D_ok` | Goblin OVER pts+ast |
| 82.6% | 46 | 73%/11 | 65.8% | `L10>=9 + D_weak` | Goblin OVER pts+ast |
| 75.9% | 166 | 75%/52 | 66.9% | `L5=5 + L10>=8` | Goblin OVER threes |
| 81.6% | 49 | 81%/27 | 64.1% | `L5=5` | Goblin OVER threes_att |
| 72.4% | 381 | 75%/136 | 66.9% | `L5>=4 + L10>=8` | Goblin OVER threes |
| 71.1% | 736 | 76%/246 | 66.9% | `L5>=4` | Goblin OVER threes |
| 74.7% | 170 | 75%/56 | 66.9% | `L10>=9` | Goblin OVER threes |
| 71.9% | 437 | 75%/154 | 66.9% | `L10>=8` | Goblin OVER threes |
| 70.1% | 1073 | 75%/365 | 66.9% | `line_0.5` | Goblin OVER threes |
| 77.0% | 87 | 78%/23 | 66.9% | `L5=5 + D_ok` | Goblin OVER threes |
| 77.0% | 87 | 78%/23 | 66.9% | `L5=5 + D_weak` | Goblin OVER threes |
| 73.4% | 173 | 76%/58 | 66.9% | `L5=5 + line_0.5` | Goblin OVER threes |
| 71.3% | 286 | 75%/89 | 66.9% | `L5>=4 + D_ok` | Goblin OVER threes |
| 71.3% | 286 | 75%/89 | 66.9% | `L5>=4 + D_weak` | Goblin OVER threes |
| 72.6% | 179 | 73%/73 | 66.9% | `L10>=8 + D_elite` | Goblin OVER threes |
| 73.0% | 126 | 70%/40 | 66.9% | `L5=5 + L10>=8 + line_0.5` | Goblin OVER threes |
| 74.4% | 90 | 77%/35 | 66.9% | `L5=5 + D_elite` | Goblin OVER threes |
| 74.3% | 70 | 70%/20 | 58.9% | `L10>=8 + D_ok` | Goblin OVER reb+ast |
| 74.3% | 70 | 70%/20 | 58.9% | `L10>=8 + D_weak` | Goblin OVER reb+ast |
| 76.6% | 47 | 75%/12 | 65.8% | `L5=5 + L10>=8 + D_ok` | Goblin OVER pts+ast |
| 73.4% | 79 | 73%/41 | 64.1% | `L5>=4 + L10>=8` | Goblin OVER threes_att |
| 73.2% | 82 | 77%/22 | 63.6% | `L5=5 + D_ok` | Goblin OVER assists |
| 73.2% | 82 | 77%/22 | 63.6% | `L5=5 + D_weak` | Goblin OVER assists |

## Split-imbalance flags (do not trust global OOS on these cells)

- **Standard OVER dreb**: train 0/63 · oos 63/63 · late_prop_oos_skew: 100% of cell rows fall in global OOS (63/63); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=0 < 15 while oos_n=63 (global split unreliable for this prop)
- **Standard OVER oreb**: train 0/54 · oos 54/54 · late_prop_oos_skew: 100% of cell rows fall in global OOS (54/54); holdout is mostly this prop's own history; train_starved_vs_oos: train_n=0 < 15 while oos_n=54 (global split unreliable for this prop)
- **Goblin OVER fga**: train 106/380 · oos 274/380 · late_prop_oos_skew: 72% of cell rows fall in global OOS (274/380); holdout is mostly this prop's own history
- **Goblin OVER rebounds**: train 1589/2268 · oos 679/2268 · early_prop_train_skew: 70% of cell rows fall in global train (1589/2268)
- **Standard UNDER rebounds**: train 456/650 · oos 194/650 · early_prop_train_skew: 70% of cell rows fall in global train (456/650)
- **Goblin OVER steals**: train 149/212 · oos 63/212 · early_prop_train_skew: 70% of cell rows fall in global train (149/212)
- **Goblin OVER fg2m**: train 17/24 · oos 7/24 · early_prop_train_skew: 71% of cell rows fall in global train (17/24)
- **Goblin OVER points**: train 1888/2665 · oos 777/2665 · early_prop_train_skew: 71% of cell rows fall in global train (1888/2665)
- **Standard OVER rebounds**: train 541/761 · oos 220/761 · early_prop_train_skew: 71% of cell rows fall in global train (541/761)
- **Standard OVER points_combo**: train 578/812 · oos 234/812 · early_prop_train_skew: 71% of cell rows fall in global train (578/812)
- **Standard UNDER points**: train 841/1177 · oos 336/1177 · early_prop_train_skew: 71% of cell rows fall in global train (841/1177)
- **Standard UNDER threes_att**: train 240/335 · oos 95/335 · early_prop_train_skew: 72% of cell rows fall in global train (240/335)
- **Standard UNDER pts+reb**: train 533/743 · oos 210/743 · early_prop_train_skew: 72% of cell rows fall in global train (533/743)
- **Standard OVER threes_combo**: train 105/146 · oos 41/146 · early_prop_train_skew: 72% of cell rows fall in global train (105/146)
- **Standard UNDER pra**: train 494/668 · oos 174/668 · early_prop_train_skew: 74% of cell rows fall in global train (494/668)
- **Standard UNDER fga**: train 443/598 · oos 155/598 · early_prop_train_skew: 74% of cell rows fall in global train (443/598)
- **Standard OVER rebounds_combo**: train 228/307 · oos 79/307 · early_prop_train_skew: 74% of cell rows fall in global train (228/307)
- **Standard UNDER assists**: train 272/365 · oos 93/365 · early_prop_train_skew: 75% of cell rows fall in global train (272/365)
- **Standard UNDER blocks**: train 22/29 · oos 7/29 · early_prop_train_skew: 76% of cell rows fall in global train (22/29)
- **Standard OVER steals**: train 102/134 · oos 32/134 · early_prop_train_skew: 76% of cell rows fall in global train (102/134)

Exports: `discover_gates_wnba_export.csv`, `discover_gates_wnba_ranked.csv`, `discover_gates_wnba_sanity.json`

```text
py -3.14 scripts/discover_gates_sport.py --sport WNBA
```

