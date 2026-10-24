# Audit: Soccer Passes/Clearances + NFL period props + Tennis 1st-set

## 1. Soccer Standard OVER Passes Attempted / Clearances — grading broken (confirmed)

Same method as NHL sog: live `graded_props` (main_cp first), HIT/MISS only.
JSON: `data/reports/audit_soccer_passes_clearances_actuals.json`.

| Cell | n | HR | line mean | actual mean | act > line | zero-share |
|---|---:|---:|---:|---:|---:|---:|
| **Passes Attempted OVER** | 112 | **0.0%** | **37.1** | **2.14** | **0 / 112** | 19.6% |
| **Clearances OVER** | 33 | **3.0%** | **5.20** | **0.94** | **1 / 33** | 39.4% |

Samples (Passes): Trippier 41.5 → actual 0; Sánchez 58.5 → actual 1; Kolasinac 41.5 → actual 1.

**Not the NHL missing→0 UNDER signature.** Zeros are only ~20%, and they all MISS on OVER (correct direction for a zero). The failure mode is **systematically tiny actuals** (0–20) against **volume lines** (20–60). That is wrong-field / incomplete Sofascore fill / ESPN-null coercion — not a real 0% market.

Code already knows these are weak ESPN markets:

- `scripts/step4_db_reader.py`: “ESPN summaries leave these NULL… Sofascore fill”
- `SOCCER_EXCLUDED_PROPS` in `combined_slate_tickets.py` already bans passes attempted / clearances / tackles / fouls / attempted dribbles from the ticket pool
- `utils/soccer_keep_gates.py` keep set is only Shots / SOT / Saves

**Verdict:** Catalog 0% / 3% are unusable. Do not gate on them. Ticket path already fades them; leave faded until Sofascore `pa` / clearances backfill is verified row-by-row against real match logs.

Related gap props (same ESPN-null family): Tackles / Crosses / Attempted Dribbles show more plausible means but stay outside keep gates.

## 2. NFL period / split props — gate status (updated)

**Decision: step2 `unsupported_prop` + step5 hard-stop. No separate step7
edge_sanity gate.**

| Layer | Behavior |
|---|---|
| `utils/nfl_unsupported_props.py` | Detects halves/quarters/first-N family |
| NFL `step2_clean_props` / CFB `step2_normalize` | Sets `unsupported_prop=1` |
| `step5b.prop_value` | Returns `None` for these (blocks fuzzy full-game map) |
| `step5b` row loop | Early `UNSUPPORTED_PROP` if flagged |
| NFL `step7` | Clears hit_rate + drops from ELIGIBLE if flag present |
| `utils/nfl_keep_gates.py` | Allowlist already excludes them from tickets |

Why not revive a step7-only edge_sanity gate: step5 was already fabricating
L5 via fuzzy `prop_value` (`yards_on_first_rush_attempt` → `RUSH_ATT`,
`quarters_with_5_rush_yards` → `RUSH_YDS`). Filtering at step7 leaves bad
numbers on the ranked sheet and in any consumer that reads step5/6. Source
mark + step5 stop matches Tennis 1st-set and is sufficient.

Grading after the game (`fetch_football_actuals`) stays intact — unsupported
is about projection/L5, not settlement.

## 3. Tennis 1st-set props — unsupported, almost no history

`Sports/Tennis/scripts/step2_attach_picktypes_tennis.py` supported set:

`aces`, `double_faults`, `games_won`, `sets_won`, `match_total_games`, `break_points_won`, `total_sets`, `total_tie_breaks`

**Not included:** `aces_set1`, `games_won_set1`, `match_total_games_set1` → `unsupported_prop=1` → step5 marks `UNSUPPORTED_PROP` (no L5/L10).

Master catalog: 1st Set Aces n=1; Total Tie Breaks live books n≤2; Total Sets only Demon-heavy. **Do not gate** until set-scoped history exists — same open question as NBA1Q, already correctly blocked at step2.

## 4. Coverage note (excl. Fantasy) — agrees with your board pass

| Sport | Shape |
|---|---|
| MLB / WNBA | Catalog-complete for live list books; fades are deliberate |
| Soccer | Keep 3 props; Passes/Clearances “covered” but **broken** (above); Tackles/Crosses untested on purpose |
| Tennis | 4 live keeps; 1st-set / BP / sets mostly unsupported or Demon-only |
| NFL / CFB | Biggest gaps; period/split family is the dangerous overlap with missing L5 scaffolds |
| NFL Std UNDER allowlist | Sacks / Rec Yards / Receptions flat at high ungated — leave ungated keep (your read matches `nfl_keep_gates.py`) |

## 5. Still parked (your catalog insights, not re-audited here)

- MLB pitcher-side **L10≥8-primary** (drop/loosen L5) — replicated across Ks / HA / BB / Outs / Pitches / ERA
- Soccer Shots L5≥4 volume tradeoff vs L5=5+L10≥8
- CFB Pass Yards Goblin — L5 hurts; D-skip design confirmed by numbers
- Tennis Goblin Total Games combined gate real; ignore stale Aces/DF 90% cells
