# June 2 cliff — full `31b51ab6b` diff + concentration check

## Diff verdict: eligibility only (no assembly side-effect)

`31b51ab6b` touches 3 files, **+77 / −18**. In `combined_slate_tickets.py` specifically:

1. Funnel stage label `after_pick_type_eligible`
2. If `pick_type_eligible` missing → call `enrich_read_fields_dataframe` before gating
3. Drop rows where `pick_type_eligible` is False
4. Harden pool sort when `win_probability` / `ml_prob` / `hit_rate` is a duplicate-column DataFrame (crash fix)

**Not present:** diversity / same-game / per-slate caps, min-legs rules, Goblin-only assembly changes, or a different pool source for mixer reads.

So this commit does not, by itself, explain 1,049 eligible legs → 3 tickets. It only taxes the pool (~37%) and wires that tax into `filter_eligible`.

## Flag compute vs mixer consume

Same function (`enrich_read_fields_dataframe` → `_pick_type_eligible`) produces the audit CSV flag and what `filter_eligible` consumes when it has to enrich. If the column is already on the frame from the earlier same-day enrich wire-up (`beb9d8def`), filter uses that column without re-enriching — still the same writer. `fillna(True)` is permissive (missing → keep), not a silent wipe.

No evidence the “1,049 eligible” audit and mixer build-time gate are different logics.

## Concentration: ruled out

Joined June 2 enriched MLB Goblin × graded game keys:

| | June 1 (all Goblin board) | June 2 (eligible Goblin) |
|--|--|--|
| Legs | 965 | 1,049 (enriched elig) |
| Games | 12 | **18** |
| Top-3 game share | 36.5% | **22.5%** |
| HHI | 0.111 | **0.067** |

Eligible pool was **more** spread than the prior day’s full Goblin board — not starved into a few games. A diversity constraint would not turn this into 3 tickets.

## Where the second mechanism must live

Outside this commit’s assembly path. Same-day siblings that *do* touch packer behavior:

| Time (ET) | Commit | Relevance |
|--|--|--|
| 11:42 | `beb9d8def` introduce read-model enrich + `pick_type_eligible` on slate | creates the flag; not yet gated in filter |
| **12:03** | **`31b51ab6b`** enforce floor in `filter_eligible` | cliff-day named suspect; pool tax only |
| 19:34 | `5cb040cc8` penalized/void → ineligible | further eligibility tighten (WNBA-framed; void wipe broader) |
| 19:46 | `f68496216` **prefer `hit_prob_actionable` in `_resolve_leg_prob`** | changes what assembly *ranks/thresholds* on, after eligibility — strongest next suspect for “legs exist, tickets don’t” |

Among eligible June 2 MLB Goblins, only ~51% have `hit_prob_actionable >= 0.65`. If packing that day already used a ~0.65 min leg-prob on enrichment-first probs, that would cut again — but that commit landed evening-of; day-level ticket_eval can’t split AM vs PM. Still the right next dig for the remaining 57→3 gap.
