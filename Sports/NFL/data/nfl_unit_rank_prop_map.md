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
| `pass_rush_rank` / **`dl_pass_tier`** | low / Elite | Opp **passing_yards UNDER**, own **sacks OVER** vs that OL |
| `run_stop_rank` / **`dl_run_tier`** | low / Elite | Soft rush context only — Elite run-stop did **not** lift UNDER in W1+W2 |
| `tier` (combined) | average of pass+run ranks | Badge only — mixes sack and run-stop signal |
| `sacks_pg` / `pressures` | high | Sack props OVER; pass volume UNDER |
| `yards_before_contact_allowed` | low | Opp rush UNDER |

**Do not** gate rush yards on combined `tier` / `opp_dl_tier`. Soft W1+W2 under
`rushing_yards` Standard UNDER (n=52, base 67%):

| Elite DL definition | HR | vs base |
|---------------------|----|---------|
| combined `tier` | 10/18 = 56% | −12pp |
| `dl_pass_tier` | 12/18 = 67% | flat |
| `dl_run_tier` | 10/18 = 56% | −12pp |
| pass-only (pass Elite, run not) | 8/11 = 73% | +5pp |
| run-only (run Elite, pass not) | 6/11 = 55% | −13pp |

Split did **not** rescue Elite DL as an UNDER lift — Elite **run-stop** still
hurts in this sample (likely line still prices run-stop D). Prefer
`defense_rankings.rush_def` / `opp_rush_d_tier` for rush UNDER (15/19 = 79%,
+12pp). Keep `opp_dl_run_tier` as soft context only; use `opp_dl_pass_tier` for
sacks / pass pressure.

## Opp secondary (`nfl_secondary_ranks`)

| Column | Better D when | Prop impact |
|--------|---------------|-------------|
| `coverage_rank` / `yards_per_target` | low ypt | Opp **receiving_yards / receptions / longest_reception UNDER** |
| `completion_pct_allowed` | low | Completions / pass yards UNDER |
| `adot_allowed` / `air_yards_allowed` | context | Deep-ball / longest reception |
| `slot_rank` / `outside_rank` | (mirrors coverage until splits) | Slot vs X/Z alignment later |
| `coverage_scheme` | Man-lean / Zone-lean / Mixed | **Proxy only** from FTN contested-rate terciles (free pbp has no Zone/Man label). Man-lean → receptions UNDER lean; Zone-lean → more short catches / YAC. Revisit when true coverage charting is free. |
| `contested_rate` | high ≈ Man | Supports scheme lean; soft badge |

## Opp box (`nfl_box_ranks`)

| Column | Better D when | Prop impact |
|--------|---------------|-------------|
| `box_rank` / `run_stop_rank` | low | Opp rush UNDER |
| `avg_box_players` | high | Opp rush UNDER (FTN×pbp) |
| `blitz_rate` | high | More pressures + checkdowns (rec UNDER on X, OVER on TE/RB dumps — monitor) |
| `yards_after_contact_allowed` | low | Opp rush UNDER after contact |

## Gate wiring (planned)

Do **not** replace existing `def_tier` pass/rush axes yet. Attach as badges / soft context first:

1. step3/step7: join opp `dl_*` + `secondary_*` + `box_*` and own `ol_*`
2. Display: `OL Above` / `Opp DL Elite` / `Opp Sec Weak`
3. After Week 2+ ledger: promote secondary yards/target into receiving D axis when n supports it

Existing coarse D stays in `utils/nfl_prop_defense.py`:

| Prop family | Axis | Column |
|-------------|------|--------|
| Pass / receiving | `pass` | `opp_pass_def_rank` |
| Rush skill | `rush` | `opp_rush_def_rank` |
| Kick | `kick` | `opp_fg_def_rank` |
| **Tackles / Tackles+Ast** | **`opp_rush_off`** | **`opp_rush_off_rank`** (opp rush *offense*) |

Keep/fade book + polarity: `Sports/NFL/data/nfl_keep_fade.md`.

## Soft hierarchy (W1+W2 unique-game) — parallel to rush

**Receiving yards / receptions / longest reception**

| Side | Primary signal | Noise / anti |
|------|----------------|--------------|
| UNDER | `opp_pass_d_tier` Elite/Above | `opp_secondary_tier` alone flat-to-hurt; WOPR low alone flat |
| UNDER stack | NOT Elite Pass Off + Elite Pass D (rec 41/44=93%; rec yds 44/55=80%) | — |
| UNDER stack | WOPR low + Elite Pass D on **receptions** (42/44=96%) | WOPR low alone ~+1–6pp |

**Option C (live):** receptions UNDER allowlist unchanged. Pack sort
(`standard_sort_key`) bumps WOPR low + Pass D Elite|Above ahead of other
receptions UNDER at equal L5. Not a ticket gate.

Week 3 grade check (ledger split on bump fired vs not):
- Bumped ~96% expected; **90%+** with larger n → Option A candidate
- Non-bumped ~75% expected
- Bumped drift to ~80% while non-bumped holds → do not harden
- Option A also needs ticket D = `opp_pass_d_tier` (not badge-only)
| OVER | `opp_pass_d_tier` Weak/Below | Weak Sec alone hurts receptions OVER (−13pp) |
| OVER stack | Fast pace + Weak Pass D on rec yds (14/23=61%, +20pp) | Elite WR alone weak (line inflation) |

Same line-inflation mirror as rush: market overprices elite WR/pass-off; mid-tier + favorable D is the lift.

**Passing yards / completions / completion%**

| Side | Primary | Soft note |
|------|---------|-----------|
| UNDER | Elite Pass D | + Elite DL pass helps completion% (8/12=67%) but chip still weak |
| OVER | Weak Pass D / Elite own pass block | thin n |

**Sacks taken UNDER** — `opp_dl_pass_tier` Weak + own Elite pass block (thin n; correct DL axis).

**Dead even with context:** player_touchdowns OVER (~14%), sacks OVER (~14%), FG made-yds combo UNDER (0%).

## Own pace (`nfl_pace_ranks`)

| Column | Faster when | Prop impact |
|--------|-------------|-------------|
| `pace_rank` / `tier` | low rank / Fast | Own **receptions / rush attempts / pass volume OVER**; Slow supports UNDERs |
| `plays_per_game` | high | Volume props OVER |
| `sec_per_play` | low | Same — compresses clock, more plays |
| `no_huddle_rate` | high | Fast-script lean |

Opp pace columns (`opp_pace_*`) describe the **opponent offense** tempo (how many plays they force you to defend / how the game script may run). Soft badge only — do not hard-gate until Week 2+ ledger.

Distinct from step6b ``pace_tier`` (that one is Vegas total HIGH/NORMAL/LOW, not actual snaps).

## Soft context only (do not hard-gate yet)

Unit ranks attach as badge/context columns. Do **not** promote into Gate70 until a Week 2+ unique-game ledger shows tier splits move hit rates beyond L5/L10/D.

First validation to run when Goblin grades land:

- Cross-tab **sacks_taken UNDER** hit rate by offense ``pass_block`` / ``own_ol_tier``.
- If Elite OL UNDER > Weak OL UNDER at usable n, that is the unlock to promote OL pressure into a gate condition.

## Player route / target participation (`nfl_route_participation`)

| Column | Higher when | Prop impact |
|--------|-------------|-------------|
| `snap_pct_L3` / `route_pct_L3` | more offensive snaps | Floor for all skill props; `<40%` hard-suppress when enabled |
| `wopr_L3` | more targets + air yards | Receiving yards OVER needs volume; `<0.50` blocks OVER when hard-on |
| `target_share_L3` | share of team targets | Receptions OVER floor (`<0.20` blocks when hard-on) |

Soft attach via `utils.nfl_route_participation_gate.attach_participation`. See `nfl_route_participation_readme.md`.

## QB mobility + red zone (soft)

| Soft layer | Column | Props |
|---|---|---|
| QB mobility | `qb_mobility_tier` (Pocket/Mixed/Mobile) | `sacks_taken` UNDER soft-split |
| Red zone team | `own_rz_rush_tier` / `own_rz_td_tier` | `rush_tds` / `rec_tds` soft |
| Red zone player | `player_high_rz_rush` / `player_high_rz_target` | TD watches |

Builders: `build_nfl_qb_mobility.py`, `build_nfl_red_zone_rates.py`. Attach: `utils.nfl_context_layers` via step3.

