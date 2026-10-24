# NHL discover_gates run

Window **2026-02-25 → 2026-10-07** · unique n=15638 · OOS dates=31

Off-season for live card; still useful for gate catalog.

## Clean OOS clears (no cover / season_HR atoms)

Requires `oos_ticket_clear` **and** `oos_split_reliable` (prop history not
skewed into the global holdout — WNBA FGA failure mode).

| HR | n | OOS | base | gate | cell |
|---|---|---|---|---|---|
| 100.0% | 22 | 100%/22 | 42.5% | `line_0.5` | Standard OVER blocked_shots |
| 81.0% | 21 | 100%/12 | 41.2% | `line_0.5` | Standard OVER hits |
| 63.0% | 27 | 73%/11 | 61.4% | `D_elite` | Standard OVER goals_allowed |
| 52.4% | 105 | 71%/28 | 59.6% | `L10>=8` | Standard UNDER sog |
| 52.4% | 105 | 71%/28 | 59.6% | `L5>=4 + L10>=8` | Standard UNDER sog |
| 51.9% | 104 | 70%/27 | 59.6% | `L5=5` | Standard UNDER sog |
| 51.9% | 104 | 70%/27 | 59.6% | `L5=5 + L10>=8` | Standard UNDER sog |
| 51.0% | 96 | 75%/24 | 59.6% | `L10=10` | Standard UNDER sog |
| 50.5% | 97 | 72%/25 | 59.6% | `L10>=9` | Standard UNDER sog |
| 52.4% | 63 | 75%/12 | 59.6% | `L10=10 + D_ok` | Standard UNDER sog |
| 52.4% | 63 | 75%/12 | 59.6% | `L10=10 + D_elite` | Standard UNDER sog |
| 39.1% | 243 | 100%/23 | 28.8% | `D_ok` | Standard OVER goals |
| 39.1% | 243 | 100%/23 | 28.8% | `D_weak` | Standard OVER goals |

## Split-imbalance flags (do not trust global OOS on these cells)

- **Standard OVER pp_points**: train 226/302 · oos 76/302 · early_prop_train_skew: 75% of cell rows fall in global train (226/302)
- **Standard OVER sog**: train 1043/1349 · oos 306/1349 · early_prop_train_skew: 77% of cell rows fall in global train (1043/1349)
- **Standard OVER plus_minus**: train 741/916 · oos 175/916 · early_prop_train_skew: 81% of cell rows fall in global train (741/916)
- **Standard OVER goals**: train 1680/1991 · oos 311/1991 · early_prop_train_skew: 84% of cell rows fall in global train (1680/1991)
- **Standard OVER assists**: train 1405/1625 · oos 220/1625 · early_prop_train_skew: 86% of cell rows fall in global train (1405/1625)
- **Standard OVER points**: train 1651/1902 · oos 251/1902 · early_prop_train_skew: 87% of cell rows fall in global train (1651/1902)
- **Standard UNDER hits**: train 549/605 · oos 56/605 · early_prop_train_skew: 91% of cell rows fall in global train (549/605)
- **Standard UNDER sog**: train 762/816 · oos 54/816 · early_prop_train_skew: 93% of cell rows fall in global train (762/816)
- **Standard UNDER goals_allowed**: train 40/42 · oos 2/42 · early_prop_train_skew: 95% of cell rows fall in global train (40/42)
- **Standard UNDER blocked_shots**: train 385/398 · oos 13/398 · early_prop_train_skew: 97% of cell rows fall in global train (385/398)
- **Standard UNDER points**: train 714/722 · oos 8/722 · early_prop_train_skew: 99% of cell rows fall in global train (714/722)
- **Standard UNDER assists**: train 509/511 · oos 2/511 · early_prop_train_skew: 100% of cell rows fall in global train (509/511)
- **Standard UNDER goals**: train 288/288 · oos 0/288 · early_prop_train_skew: 100% of cell rows fall in global train (288/288)
- **Standard UNDER pp_points**: train 19/19 · oos 0/19 · early_prop_train_skew: 100% of cell rows fall in global train (19/19)
- **Standard UNDER plus_minus**: train 27/27 · oos 0/27 · early_prop_train_skew: 100% of cell rows fall in global train (27/27)
- **Goblin OVER sog**: train 2326/2326 · oos 0/2326 · early_prop_train_skew: 100% of cell rows fall in global train (2326/2326)
- **Goblin OVER points**: train 960/960 · oos 0/960 · early_prop_train_skew: 100% of cell rows fall in global train (960/960)
- **Goblin OVER assists**: train 144/144 · oos 0/144 · early_prop_train_skew: 100% of cell rows fall in global train (144/144)
- **Goblin OVER goals_allowed**: train 14/14 · oos 0/14 · early_prop_train_skew: 100% of cell rows fall in global train (14/14)
- **Goblin OVER blocked_shots**: train 7/7 · oos 0/7 · early_prop_train_skew: 100% of cell rows fall in global train (7/7)

Exports: `discover_gates_nhl_export.csv`, `discover_gates_nhl_ranked.csv`, `discover_gates_nhl_sanity.json`

```text
py -3.14 scripts/discover_gates_sport.py --sport NHL
```

