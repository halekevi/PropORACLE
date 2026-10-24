# CFB unit-line ranks → prop mapping (soft context)

Source tables (views — **do not recompute z**):

| File | Unit | Source of the number |
|------|------|----------------------|
| `cfb_ol_ranks.csv` | Own OL **run-block** | `ol_index` from `cfb_line_secondary.csv` |
| `cfb_dl_ranks.csv` | Opp front-seven / run-stop | `dl_index` from the same CSV |
| `cfb_secondary_ranks.csv` | Opp secondary | `secondary_index` |
| `cfb_box_ranks.csv` | **Alias of `dl_index`** | Same number as DL. No distinct CFB box charting |
| `cfb_pace_ranks.csv` | Own pace | ESPN `off_plays_pg` + `off_clock_tier` |
| `cfb_route_participation.csv` | Player volume share | `cfb_player_volume.csv` (`rec_tgt_share` / `rush_att_share`). **No WOPR. No snap %.** |

`ol_index` / `dl_index` / `secondary_index` are equal-weight placeholders (`equal_mean_placeholder`). Empty until `CFBD_API_KEY` is set. Do not invent ranks.

## Own OL (`cfb_ol_ranks`)

| Column | Better when | Prop impact |
|--------|-------------|-------------|
| `ol_index` / `ol_rank` | high z / low rank | Own **rushing_yards OVER** (run-block: line yards, stuff, power) |
| `sack_rate_allowed_rank` | low (fewest sacks taken) | ESPN proxy only — **not** pass-block z. Sacks UNDER uses this, not `ol_index` |

OL composite is **run-block only**. Pass-pro (pressure / time-to-throw) is not in this layer.

## Opp DL (`cfb_dl_ranks`) vs Sacks

| Column | Better D when | Prop impact |
|--------|-------------|-------------|
| `dl_index` / `dl_rank` | high z / low rank | Opp **rushing_yards UNDER** (run-stop + mixed `havoc.frontSeven`) |
| `espn_sacks_pg` / `espn_sacks_rank` | high sacks | **Sacks UNDER** on the opponent (they generate sacks) |

**DL tier does not explain Sacks UNDER.** `havoc.frontSeven` blends TFLs and forced fumbles with sacks rather than isolating sack rate. A team can rank Elite on `dl_index` and Avg on ESPN sacks/G (or the reverse). Print both; do not reconcile them. A future audit must not assume DL rank explains sack numbers.

## Opp secondary (`cfb_secondary_ranks`)

| Column | Better D when | Prop impact |
|--------|-------------|-------------|
| `secondary_index` / `secondary_rank` | high z / low rank | Opp **receiving_yards / receptions / longest_reception UNDER** |

## Opp box (`cfb_box_ranks`)

Same as `dl_index`. Label as front-seven. Do not treat as NFL FTN box (players in box / blitz).

## Own pace (`cfb_pace_ranks`)

| Column | Faster when | Prop impact |
|--------|-------------|-------------|
| `pace_rank` / `tier` | low rank / Elite | Volume props OVER (receptions / rush att / pass volume) |
| `clock_tier` | Pass-heavy / Run-heavy clock | Script lean — already on step8 |

## Gate wiring (planned)

Do **not** replace existing `def_tier`. Do **not** change `utils/cfb_keep_gates.py`.

Locked ticket decisions stay:

- Pass Yards Goblin OVER skips D
- Rush Yards Goblin OVER keeps D
- Rec Yards Standard UNDER off the cover-stack
- Standard Rush Yards UNDER keeps the full stack

Attach as badges first. Hard participation gates stay `PARTICIPATION_HARD_GATES_ENABLED = False`. Flip only after a unique-game HIT/MISS ledger on `graded_props_*.json` shows shares are **not inverted** (usage floors at 25–50% were). The weekly O/D markdown is not that ledger. Do not copy NFL snap/WOPR floors.

## Two authorities (do not mix)

| Question | Authoritative source | Not this |
|----------|----------------------|----------|
| Who is on this week's slate / matchup leans / percentile tiers | `cfb_od_rankings_week{N}.md` from latest step8 (this checkout, then `PropORACLE_main_cp`) + ESPN unit rankings + optional `cfb_line_secondary.csv` | Graded JSON. A live board can list 9/19 pairings from main_cp step8 while 9/19 remains a **permanent grade hole**. |
| Did this prop HIT/MISS? Pass Yards veto cell, Rush D+ close vs blow-fav, usage inversion | `ui_runner/templates/graded_props_*.json` unique player-game | The O/D board. Percentile tiers are not hit rates. |

When a CFBD key fills `ol_index` / `dl_index` / `secondary_index`, correlating those against results means **join the composite onto the graded tape** (date + team/player), then cut HIT/MISS. Reading Elite/Weak off the weekly markdown is not an audit. Same seam as DL tier vs ESPN sacks: two related numbers from two pipelines can disagree without either being wrong.
