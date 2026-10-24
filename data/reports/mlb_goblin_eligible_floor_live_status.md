# Live status: `pick_type_eligible` 0.50 Goblin floor (`31b51ab6b`)

## Is it still active today?

**Yes.**

| Layer | Status |
|--|--|
| `data/pipeline_read_checklist.json` → `pick_type_rules.goblin.min_hit_prob_actionable` | still **0.50** |
| `utils/pipeline_read_enrichment.py` `_min_hit_prob_actionable_for_row` | floor applies only when `pick_type in (goblin, demon)`; **Standard returns `None` (no floor)** |
| `scripts/combined_slate_tickets.py` `filter_eligible` | still drops `pick_type_eligible=False` |
| `scripts/build_goblin70_tickets.py` | does **not** call `pick_type_eligible` — uses its own L5/L10/D keep gates |

So: mixer / win-rate-main path = live surface. Goblin-70 dual-card path = separate gate stack.

## Intent (from the commit, not the data)

`31b51ab6b` message: *"Goblin legs require min_hit_prob_actionable (0.50 global, 0.45 Tennis)."*

The asymmetry was **deliberate**, not a missing Standard wire-up:
- Goblin got a new `min_hit_prob_actionable` floor.
- Tennis got an explicit softer override (0.45).
- Demon already had its own `min_hit_prob_over` (0.62).
- Standard was left with **no** probability floor in this path.

So "fix" means: is **0.50 the right Goblin quality bar** (and does it cut good Goblin with the junk) — not "add the forgotten Standard check." Removing the asymmetry outright would be a product decision against the written intent of that commit.

## Score distribution vs 0.50 (cliff-day board)

Best at-cliff board: `pipeline_read_enriched_2026-06-02.csv` (no May MLB enriched snapshot on disk).

**MLB Goblin** (n=1677): mean 0.566, median 0.60; **37.4% < 0.50**, 62.6% ≥ 0.50.
**MLB Standard** (n=603): mean 0.623, median 0.60; 25.9% < 0.50 — **elig% = 100%**.

| Bucket | Goblin elig% | Standard elig% |
|--|--|--|
| < 0.50 | **0%** (n=628) | **100%** (n=156) |
| ≥ 0.50 | 100% | 100% |

100% of Goblin ineligibles are exactly `hit_prob_actionable < 0.50`.

## Does 37% leg cut explain 238→3 tickets? **No — second mechanism required**

May MLB `all_goblin` shape (n=4257): mean **3.40** legs (median 3); mix 34% 2-leg / 26% 3 / 18% 4 / 13% 5 / 10% 6.

Independent survival if each leg kept with p=0.626 (pass rate):

| Legs | Ticket survival |
|--|--|
| 2 | 39.2% |
| 3 | 24.5% |
| 4 | 15.4% |
| 5 | 9.6% |
| 6 | 6.0% |
| **May mix-weighted** | **24.0%** |

From June 1's 238 Goblin tickets that predicts **~57 survivors**, not 3.
Even pessimistic "every ticket needs all legs from the thinned pool" only gets you a ~76% drop — not 98.7%.

June 2 board still had **1049 eligible MLB Goblin legs** after the floor. A packer with that pool should still emit far more than 3 tickets. So the floor is a real, live, Goblin-only tax on the mixer — but the **238→3 cliff needs a second mechanism** (same-day enrichment / path change, or selection that stopped emitting MLB Goblin slips despite eligible supply).

## Practical priority

- Still highest-priority **mixer** item: live Goblin-only floor on the best-validated lever; Goblin-70 bypasses it.
- Do not treat the cliff as fully explained by this gate alone.
- Intent-aligned next step: measure whether sub-0.50 Goblin legs that the floor cuts are actually bad (hit rate / ticket WR) before retuning 0.50 — not "add Standard floor to match."
