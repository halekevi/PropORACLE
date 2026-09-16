# MLB Standard ban vs May–June investigation — reconciliation

Question: `_leg_mlb_keep_banned` (all MLB Standard off on MAIN/FINAL) — does it make
this thread's MLB Standard findings moot, or did May–June tickets come from another
surface the ban never covered?

## Verdict

**Possibility 2 — the ban is newer than May–June, and those tickets were MAIN mixer.**

The May–June MLB Standard tickets this thread analyzed were real published
`combined_slate_tickets_*.json` slips (`MLB Standard N-Leg`, `MLB Mixed N-Leg`
group names). They were **not** Goblin-70. The full-Standard ban that closes that
surface today did **not** exist during May–June; it landed **2026-09-08**
(`f448265e5`, piggybacked on a payout-scrape commit). An earlier **OVER-only**
ban landed **2026-07-19** (`2807c49e5`) — also after the May–June window.

So the thread's MLB Standard findings describe a historically-real MAIN surface
that current production already excludes by gates **unrelated to player-shrinkage**.
Shrinkage is not what fixed MLB Standard on MAIN; the keep/OVER bans did.

## Timeline

| When | Gate | Effect on MLB Standard |
|--|--|--|
| May–June 2026 | No all-Standard ban | Mixer published heavy MLB Std volume (May: **7,393** tickets / 11,134 Std legs; June: **2,022** / 3,357). May legs were **100% OVER**. |
| **2026-07-19** `2807c49e5` | `_leg_mlb_standard_over_banned` via `_leg_mlb_construction_banned` on FINAL/long | Bans MLB **Standard OVER** only. July volume collapses (657 tickets / 8 days). |
| 2026-08-03 | Perfect-L5 Std OVER avoid | Further hygiene; August nearly gone (**11** tickets total). |
| **2026-09-08** `f448265e5` | **`_leg_mlb_keep_banned`** — `if pt == "Standard": return True` | Bans **all** MLB Standard (OVER+UNDER) on MAIN/FINAL/long + `filter_eligible` hygiene. |

`git log -S "_leg_mlb_keep_banned"` → single hit: `f448265e5` (2026-09-08).

## Path identity (May–June tickets)

Published snapshot groups (not ticket_eval article noise):

- **May** top groups: `MLB Mixed 5-Leg #…`, `MLB Mixed 6-Leg #…` (mixer FINAL sheets)
- **June** top groups: `MLB Standard 2-Leg #1…`, `MLB Standard 3-Leg…`, plus some `X-Sport Mixed`
- Direction: May all OVER; June OVER+UNDER after packing mix flip

That is the same family of builders that today call
`_ticket_rows_mlb_construction_banned` / `filter_eligible` MLB hygiene — i.e. the
surface `_leg_mlb_keep_banned` covers.

Goblin-70: `utils/ticket_70_pool.py` already says “MLB Standard never passes” /
“MLB Standard stays off” — not the May–June volume source.

## What this means for the thread

| Finding | Status vs current MAIN |
|--|--|
| MLB soft May→June was Goblin→Standard **mix**, not skill decay | Still true historically; current MAIN no longer packs that Standard share |
| MLB-only player-shrunk Standard fails BE (~22 prior/player) | Describes a surface **already closed** on MAIN/FINAL by Sep-8 ban |
| Cross-sport player-shrunk Standard clears BE | Still the live-relevant Standard lever (non-MLB + paths that remain open) |
| Shadow `mlb_share_*` on pre-ban pool | Diagnostic only; production MAIN should show ~0 MLB Standard tickets |

## Open residual (narrow)

If any non-MAIN path still emits MLB Standard **without** going through
`_leg_mlb_construction_banned` (legacy Excel-only sheets, a sidecar, or a
future track), that path would still be unguarded relative to May–June lessons.
Quick residual volume: August snapshots already near-zero before the full ban;
post–Sep-8 MAIN should stay at zero. Worth a one-line assert in CI/smoke if you
want this locked: `mlb_standard_tickets == 0` on MAIN payload.

## Bottom line

MLB Standard on MAIN is **already fixed by an independent, later ban** (OVER
2026-07-19 → all-Standard 2026-09-08). The shrinkage work should not be sold as
the MLB-Standard fix; its value is cross-sport / non-MLB Standard ranking, with
MLB MAIN treated as a closed case unless a bypass path reappears.
