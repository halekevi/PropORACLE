# NFL unit-line ranks → prop D mapping

Source tables (built by `Sports/NFL/scripts/build_nfl_unit_line_ranks.py` from free nflverse):

| File | Unit | Affects |
|------|------|---------|
| `nfl_ol_ranks.csv` | Own offensive line | Own pass/rush volume & sack props |
| `nfl_dl_ranks.csv` | Opp defensive line | Opp QB pressure; own rush vs that front |
| `nfl_secondary_ranks.csv` | Opp secondary (CB/S) | Opp pass catchers; own pass OVERs/UNDERs |
| `nfl_box_ranks.csv` | Opp front-seven / box | Opp rush UNDERs; blitz → checkdown volume |

## Own OL (`nfl_ol_ranks`)

| Column | Better when | Prop impact |
|--------|-------------|-------------|
| `pass_block_rank` | low (1=best) | Own **passing_yards / completions OVER** need Elite\|Above; **sacks_taken OVER** vs Weak\|Below |
| `run_block_rank` | low | Own **rushing_yards OVER** |
| `sack_rate_allowed` / `pressure_rate_allowed` | low | Suppress pass OVERs / boost sack OVERs when high |
| `yards_before_contact` | high | Own rush OVER / longest rush |

## Opp DL (`nfl_dl_ranks`)

| Column | Better D when | Prop impact |
|--------|---------------|-------------|
| `pass_rush_rank` | low (elite rush) | Opp **passing_yards UNDER**, own **sacks OVER** vs that OL |
| `run_stop_rank` | low | Opp **rushing_yards UNDER** |
| `sacks_pg` / `pressures` | high | Sack props OVER; pass volume UNDER |
| `yards_before_contact_allowed` | low | Opp rush UNDER |

## Opp secondary (`nfl_secondary_ranks`)

| Column | Better D when | Prop impact |
|--------|---------------|-------------|
| `coverage_rank` / `yards_per_target` | low ypt | Opp **receiving_yards / receptions / longest_reception UNDER** |
| `completion_pct_allowed` | low | Completions / pass yards UNDER |
| `adot_allowed` / `air_yards_allowed` | context | Deep-ball / longest reception |
| `slot_rank` / `outside_rank` | (mirrors coverage until splits) | Slot vs X/Z alignment later |
| `coverage_scheme` | Zone/Man/Mixed | Man → contested / separation; Zone → YAC |

## Opp box (`nfl_box_ranks`)

| Column | Better D when | Prop impact |
|--------|---------------|-------------|
| `box_rank` / `run_stop_rank` | low | Opp rush UNDER |
| `blitz_rate` | high | More pressures + checkdowns (rec UNDER on X, OVER on TE/RB dumps — monitor) |
| `yards_after_contact_allowed` | low | Opp rush UNDER after contact |

## Gate wiring (planned)

Do **not** replace existing `def_tier` pass/rush axes yet. Attach as badges / soft context first:

1. step3/step7: join opp `dl_*` + `secondary_*` + `box_*` and own `ol_*`
2. Display: `OL Above` / `Opp DL Elite` / `Opp Sec Weak`
3. After Week 2+ ledger: promote secondary yards/target into receiving D axis when n supports it

Existing coarse D stays in `utils/nfl_prop_defense.py` (pass/rush/kick from `defense_rankings.csv`).

## Soft context only (do not hard-gate yet)

Unit ranks attach as badge/context columns. Do **not** promote into Gate70 until a Week 2+ unique-game ledger shows tier splits move hit rates beyond L5/L10/D.

First validation to run when Goblin grades land:

- Cross-tab **sacks_taken UNDER** hit rate by offense `pass_block` / `own_ol_tier`.
- If Elite OL UNDER > Weak OL UNDER at usable n, that is the unlock to promote OL pressure into a gate condition.

