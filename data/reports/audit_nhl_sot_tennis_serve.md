# Audit: Tennis Std UNDER Aces/DF + NHL Std UNDER Shots On Target

Generated from live `graded_props_*.json` (main_cp preferred, same discovery as
`scripts/all_ranked_prop_hit_rates.py`), window **2026-02-19 → 2026-09-16**
(206 files). Full JSON: `data/reports/audit_nhl_sot_tennis_serve_actuals.json`.

## 1. Tennis Standard UNDER Aces / Double Faults — contradiction resolved

| Source | Aces UNDER | DF UNDER |
|---|---|---|
| Stale master `all_ranked_prop_hit_rates.json` (to 2026-08-30) | **90.6% (n=96)** | **90.0% (n=50)** |
| Live graded after serve backfill (this audit) | **57.4% (n=54)** | **28.6% (n=28)** |
| `tennis_serve_backfill_under_inflation.json` | 57.4% n=54 | 28.6% n=28 |

**Verdict:** The master catalog was stale (pre-backfill / pre-void). Live rows
match the keep-gate fade call exactly. Actual-value check:

- Aces UNDER: mean actual **6.93**, zero-share **1.9%** (1 zero→HIT only).
  HR excluding zeros **56.6%**.
- DF UNDER: mean actual **6.75**, zero-share **0%**. HR **28.6%**.

Production stay **off**. Do not trust the 90% cells in any report that still
cites the old master without rebuild.

## 2. NHL Standard UNDER “Shots On Target” — grading bug confirmed

Display name in the catalog is **Shots On Target** (`utils/prop_norm.py`
maps canon `sog` → that label). Raw graded prop labels are **Shots On Goal** /
`shots_on_goal` only — no true “Shots On Target” string in NHL graded files.

| Slice | n | HR | zero-share | mean actual | HR excl zeros |
|---|---:|---:|---:|---:|---:|
| Std UNDER sog | 1110 | **81.6%** | **65.9%** | 0.84 | **46.2%** |
| Std OVER sog | 3575 | 16.3% | 56.8% | 1.04 | 37.7% |
| Goblin OVER sog | 2412 | 45.9% | 39.9% | 1.63 | 76.3% |
| Std UNDER Hits (control) | 672 | 57.1% | 27.8% | 2.25 | 40.6% |

UNDER sog: **731 / 731 zeros are HIT** (classic missing→0 UNDER inflation —
same shape as the earlier Shots-On-Goal / Hits investigation). Mean ~0.8 shots
is not a real NHL volume distribution.

**Verdict:** Do not use NHL Std UNDER sog / “Shots On Target” for tickets or
gates until actuals are re-sourced. Treat the catalog 81.6% as contaminated.

Display seam closed in code: `display_prop("sog", sport="NHL")` now returns
**Shots On Goal** (soccer keeps Target). See `audit_prop_display_raw_splits.md`.

## 3. Side notes from the same pull

- Tennis Std OVER Aces/DF still carry some zero actuals (20.5% / 12.8%) but
  zeros always MISS on OVER, so they depress HR rather than inflate it —
  consistent with the known asymmetry.
- Hits UNDER zero-share (27.8%) is elevated but far below sog’s 65.9%; sog is
  the clear broken cell.

## Action

1. Rebuild `all_ranked_prop_hit_rates.json` from current graded_props (so Tennis
   90% cells disappear).
2. Keep Tennis Std UNDER Aces/DF faded.
3. Flag / exclude NHL `sog` Standard UNDER from gate discovery until grader
   actuals are fixed.
