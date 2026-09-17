# MLB discover_gates run (2026-09-17)

Systematic gate scan over graded MLB + dated step8 + slash BA/K% + team
pitch/bat tiers. Outcome columns excluded from candidate features.

## Exports

| File | What |
|---|---|
| `data/reports/discover_gates_mlb_export.csv` | Row-level enriched export (features + `hit`/`actual_value` as labels/sanity only) |
| `data/reports/discover_gates_mlb_ranked.csv` | Ranked gate table (all book×prop×gate clears with n≥15) |
| `data/reports/discover_gates_mlb_ranked.json` | Full payload (coverage, OOS dates, per-cell tops, leakage ban list) |
| `data/reports/discover_gates_mlb_sanity.json` | Zero-actual / actual≪line flags |

Re-run:

```powershell
py -3.14 scripts/discover_gates_for_all_props.py --top 80
```

## Window / coverage

- **198,160** graded HIT/MISS rows · **164** dates (2026-03-30 → 2026-09-16)
- Train = first 70% of dates (115) · OOS = last 30% (49)
- Feature coverage: L5 57% · L10 64% · BA 84% · K% 83% · own/opp pitch ~99% · cover 31% (step8-dependent)
- Leakage ban: `actual_value`, `margin`, `hit`, `result`, and aliases — never atoms

## Sanity (run this before trusting cells)

27 book×prop cells flagged. **Most are 0.5-line counting props** where zeros are
structurally common (Std Hits/Runs/Singles/Walks, 1st-inning walks/runs) — not
the NHL-SOG “missing→0” signature. Still verify any gate you promote:

- Zero rate on UNDER with mean_line ≈ 0.5 is expected; check that non-zero
  actuals look like real counting stats.
- No `actual_<<_line` (soccer Passes-style) flag fired on this MLB export.
- Goblin volume props were **not** in the high-zero flag set.

## Headline patterns (Goblin OVER, ticket bar HR≥70% n≥40, OOS held)

Already-known L10-primary pitcher shape still clears; scan also surfaces
**cover / season-cushion** stacks that beat baseline hard when present.

### Cover≥1 — OOS restatement + missingness confound (2026-09-17)

See `discover_gates_mlb_cover_oos_check.json`.

| Cell | Pooled (ranked table) | **OOS-only** (Jul 26–Sep 16) | Train-only |
|---|---|---|---|
| Hits Allowed `cover≥1` | 88.8% n=214 | **90.2% n=163** | 84.3% n=51 |
| Pitcher Ks `L10≥8 + cover≥1` | 87.8% n=196 | **89.7% n=155** | 80.5% n=41 |

Pooled figures were **not** OOS-only; restated holdout still clears ticket bar.

**Cover missingness is CONFOUNDED with date — not benign scatter:**

| Window | Cover rate |
|---|---|
| Mar–Jun (early half) | **0.0%** |
| Late half | 47.3% |
| Train dates | 13.0% |
| OOS dates | 59.6% |
| Onset (7 consecutive days ≥50%) | **2026-07-23** |
| Post-onset | 61.6% |

Monthly: Mar–Jun 0% → Jul 52% → Aug 33% → Sep 100%.

So `cover≥1` partly selects “leg from the post-cover pipeline era.” Cover-present
vs missing **ungated** base HR delta is only ~+5pp (not enough alone to explain
88%), but the fair test is post-onset / backfilled cover — **do not wire** on the
full Mar–Sep pool until that.

ERA L5=5 + L10≥8 + line_0.5 ≈ **89.9% n=247** (OOS 92%/194) — reinforces keeping
ERA on the L5∩L10 path rather than L10-only. Unaffected by the cover confound.

## Cover vs season_cushion — mature-window follow-up (2026-09-17 evening)

Independent pass correctly showed **cover availability** is a date confound:
populated vs ungated in a coverage-saturated slice are identical (Sep chron-val
both 74.0%). That kills the “having cover data” story behind the pooled 88%.

`season_cushion_dir` has the **same rollout as cover_dir** (Mar–Jun 0%, onset
~2026-07-23, Sep ~100%). Same test required before any cushion gate.

| Test | What it answers |
|---|---|
| Populated vs ungated post-onset | Availability confound (cover + cush: null when saturated) |
| Threshold (`cover≥1`, `season_cush≥1`) in **Sep-only** (100% coverage) | Does the cut add lift when availability cannot differ? |

Sep-only (availability fixed) still shows threshold lift — e.g. Hits Allowed
`cover≥1` 91.5% n=117 vs ungated 74.2%; `season_cush≥2` 87.8% n=222. That is
**not** the same as the availability-null result, and Sep is only ~2 weeks with
no chron holdout inside the mature era.

**Still do not wire cover or season_cush.** Reasons to hold:

1. Pooled ranked-table lift was era-confounded (settled).
2. Pitcher Ks `L10≥8+cover` remains thin/unstable in early post-onset train.
3. Sep-mature threshold lifts are in-sample on a short mature calendar — need
   more cover/cush-mature weeks (or a chron split entirely inside mature dates)
   before either threshold is answerable the way L10-primary was.

L10-primary pitcher keeps (no cover/cush) stay the validated MLB result from
this pass.

Artifacts: `discover_gates_mlb_cover_oos_check.json`,
`discover_gates_mlb_season_cush_onset_check.json`,
`scripts/audit_mlb_feature_onset_gates.py`.
