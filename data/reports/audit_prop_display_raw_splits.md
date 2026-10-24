# Audit: display_prop vs raw graded labels

Window: graded_props **2026-02-19 → 2026-09-16** (206 files).  
JSON: `data/reports/audit_prop_display_raw_splits.json`.

## Finding

One **semantic** display/raw split of the NHL sog kind. Everything else in the
“hard split” dump is abbreviation cosmetics (`ast` → Assists, `pts` → Points)
or title-case shortening (`Fantasy Score` → Fantasy, `Time On Ice` → Toi).

| Sport | Canon | Display (was) | Raw graded labels | Std UNDER zero-share |
|---|---|---|---|---:|
| **NHL** | `sog` | **Shots On Target** | Shots On Goal / shots_on_goal | **65.9%** (bug) |
| NHL | `toi` | Toi | Time On Ice | 1.5% (cosmetic only) |
| NHL | `pp_points` | Pp Points | Power Play Points | 45% (thin n=20; rare-event) |
| CBB | many | Assists / Points / … | ast / pts / … | mostly fine |
| NBA/NFL/WNBA | fantasy | Fantasy | Fantasy Score | ~0% zeros |

**Root seam:** `utils/prop_norm.py` `DISPLAY["sog"] = "Shots On Target"` is soccer
PP wording. NHL and soccer share canon `sog`, so the catalog relabeled every NHL
Shots-On-Goal row as Target — same field, different name, looked “unaudited.”

## Fix landed

`display_prop(canon, sport=…)` is now sport-aware:

- NHL `sog` → **Shots On Goal**
- Soccer / default `sog` → Shots On Target
- NHL `toi` / `pp_points` → Time On Ice / Power Play Points

`all_ranked_prop_hit_rates.py` and `apply_canon_row` pass sport through.

## Other high zero-share UNDER (not the same bug)

| Cell | zero-share | Read |
|---|---:|---|
| NFL Sacks UNDER | 89.6% | Likely **legit rare event** (0 sacks common); not a display split |
| NHL Goals / Assists UNDER | 76% / 64% | Rare-event shape; display matches raw |
| MLB Hits / Singles / TB UNDER | ~48–55% | Counting zeros; not a Target/Goal rename |

Only NHL `sog` pairs a **misleading display rename** with the missing→0 UNDER
inflation. Grading actuals for that field are still unfixed.

## Master rebuild lag (Tennis lesson)

`scripts/all_ranked_prop_hit_rates.py` now:

- Watches `correction_reports_watched` (tennis serve backfill + this NHL audit JSON)
- Prints **WARN** when those files (or newer graded_props) beat the existing master
- Stamps `generated_at` / `stale_inputs_at_start` into the JSON

Not a full auto-rebuild hook — that still wants a Task Scheduler / post-backfill
call — but regenerating without noticing a newer correction report should no
longer be silent.

## Still open (catalog pass)

1. **NBA1Q** Goblin OVER Rebounds / Assists — L5=5 ~93% / ~91% at real volume; strongest untested bespoke-gate candidate.
2. **CBB** — every live book row still has empty L5/L10 windows; need history attach before any filter test.
