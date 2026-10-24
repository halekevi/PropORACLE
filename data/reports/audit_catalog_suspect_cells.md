# Catalog suspect-cell audit

Re-grade with strict DNP-zero voids (positive evidence only), leakage-pass where boxes allow, pick_type twin checks, and NBA1Q full-game contamination drop (actual == full-game box).

## Re-graded cells (before → after)

| Cell | Gate | Catalog before | After filters | Δ | Notes |
|---|---|---|---|---|---|
| NBA1Q Goblin OVER points | l5eq5 | 84.6% (738/872) Wilson[82.1–86.9] | 84.4% (722/855) Wilson[81.9–86.7] | -0.0019 | FG-match drops=17; plausible 1Q=512; unchecked=343 |
| NBA1Q Goblin OVER rebounds | l5eq5 | 93.2% (399/428) Wilson[90.4–95.2] | 93.1% (393/422) Wilson[90.3–95.2] | -0.0009 | FG-match drops=6; plausible 1Q=235; unchecked=187 |
| NBA1Q Goblin OVER assists | l5eq5 | 91.2% (198/217) Wilson[86.7–94.3] | 90.4% (179/198) Wilson[85.5–93.8] | -0.0084 | FG-match drops=19; plausible 1Q=119; unchecked=79 |
| NBA1Q Standard OVER assists | l5eq5 | 89.4% (42/47) Wilson[77.4–95.4] | 100.0% (2/2) Wilson[34.2–100.0] | 0.1064 | FG-match drops=1; plausible 1Q=1; unchecked=1; twin drops=44 |
| NHL Standard UNDER points | l5eq5_l10ge8 | 80.4% (135/168) Wilson[73.7–85.7] | 83.5% (81/97) Wilson[74.9–89.6] | 0.0315 | leak fail=71; DNP void=0; toi0=0 |
| NHL Standard UNDER assists | l5eq5_l10ge8 | 81.8% (135/165) Wilson[75.2–87.0] | 80.7% (96/119) Wilson[72.7–86.8] | -0.0115 | leak fail=46; DNP void=0; toi0=0 |

## Bug explanations

### NFL Standard UNDER inflated / non-filtering gates
Two separate bugs:

1. **L5=5 cells at 97–100% (Rec Yds 27/27, Rush 15/15, Receptions 44/45)** — UNDER hits with `actual=0` (DNP/inactive) without positive PT evidence stay graded as HIT. In the live graded pool, Rec Yds has 48 actual=0 UNDER hits; Sacks 265. Strict DNP-zero only voids when minutes/snaps/TOI==0 or an inactive flag is present — many NFL rows lack that, so zeros remain.

2. **Near-identical gate columns (Sacks, Longest Reception in the tracked md)** — not because L5 fields are null (they are present), but because **almost every row already has directional L5=5** (Sacks: 723/725). The L5/L10 gates barely filter, so ungated ≈ L5=5 ≈ joint counts.

```
{
  "receiving yards": {
    "ungated": "87/100",
    "listed": "58/66",
    "l5eq5": "27/27",
    "l10ge8": "46/47",
    "both": "27/27",
    "identical_gates": false
  },
  "receptions": {
    "ungated": "80/96",
    "listed": "68/76",
    "l5eq5": "44/45",
    "l10ge8": "59/62",
    "both": "43/44",
    "identical_gates": false
  },
  "rushing yards": {
    "ungated": "37/56",
    "listed": "29/31",
    "l5eq5": "15/15",
    "l10ge8": "24/26",
    "both": "15/15",
    "identical_gates": false
  },
  "sacks": {
    "ungated": "73/77",
    "listed": "73/77",
    "l5eq5": "73/77",
    "l10ge8": "68/72",
    "both": "68/72",
    "identical_gates": false
  }
}
```

Per-prop L5 presence / actual=0 UNDER hits:
```
{
  "receiving yards": {
    "n": 865,
    "l5_present": 865,
    "l5_5": 285,
    "l10_present": 865,
    "under_hit_actual0": 48,
    "actual0": 48,
    "l5_4": 317,
    "l5_3": 213,
    "l5_2": 41,
    "l5_1": 9
  },
  "rushing yards": {
    "n": 422,
    "l5_present": 422,
    "l5_4": 147,
    "l10_present": 422,
    "l5_5": 147,
    "under_hit_actual0": 25,
    "actual0": 25,
    "l5_3": 101,
    "l5_2": 24,
    "l5_1": 3
  },
  "longest reception": {
    "n": 962,
    "l5_present": 962,
    "l5_5": 886,
    "l10_present": 962,
    "under_hit_actual0": 16,
    "actual0": 16,
    "l5_3": 27,
    "l5_4": 37,
    "l5_2": 5,
    "l5_1": 7
  },
  "receptions": {
    "n": 719,
    "l5_present": 719,
    "l5_5": 287,
    "l10_present": 719,
    "l5_4": 254,
    "under_hit_actual0": 39,
    "actual0": 39,
    "l5_3": 149,
    "l5_2": 27,
    "l5_1": 2
  },
  "sacks": {
    "n": 725,
    "l5_present": 725,
    "l5_5": 723,
    "l10_present": 725,
    "under_hit_actual0": 265,
    "actual0": 265,
    "l5_4": 2
  },
  "pass completions": {
    "n": 158,
    "l5_present": 158,
    "l5_5": 147,
    "l10_present": 158,
    "l5_3": 5,
    "l5_2": 1,
    "l5_1": 1,
    "l5_4": 4
  }
}
```

### NHL Standard OVER Faceoffs Won 98.4%
**Faceoff win % graded against faceoffs-won count lines.** Examples: Tage Thompson line 5.5 actual **44.4** HIT; J.T. Miller 10.5 actual **61.1** HIT; Adam Lowry 6.5 actual **60.0** HIT. FO% (typically 30–70) almost always clears a FOW count line (~5–12), so the cell is ~98% by construction — not a skill edge.
Catalog: 98.4% (125/127) Wilson[94.4–99.6]; rows seen=182
Actual value counts: [('None', 21), ('10.0', 12), ('6.0', 11), ('4.0', 9), ('50.0', 8), ('9.0', 8), ('5.0', 8), ('33.3', 7), ('8.0', 6), ('60.0', 5), ('70.0', 4), ('7.0', 4), ('2.0', 4), ('11.0', 4), ('66.7', 4)]

### Soccer Passes Attempted / Clearances ~0%
0% / ~3% on Passes Attempted / Clearances OVER means the grader almost never finds a matching box column — actual stays missing/0 while the line is large, so OVER always misses. Not a real skill cell.
Hit counts: {'passes attempted|None': 2473, 'clearances|None': 454, 'passes attempted|0': 112, 'clearances|0': 32, 'clearances|1': 1}

## Verdict

- **NBA1Q Goblin L5=5 after FG-contamination drop:** Rebounds Wilson low **90.3%** and Assists **85.5%** still clear the ~84.5% Goblin Power bar; Points Wilson low **81.9%** does **not**. Only 17/6/19 legs matched full-game boxes exactly (dropped); most checked legs had actual < FG (plausible 1Q). Large unchecked share (no FG box join) remains a residual risk.
- **NBA1Q Standard OVER Assists:** almost all L5=5 rows fail the Goblin-twin check (Standard line below Goblin) — collapses to **2/2**. Do not use.
- **NHL Std UNDER Points/Assists L5=5+L10≥8 after leakage:** Points **83.5%** (81/97) Wilson[74.9–89.6]; Assists **80.7%** (96/119) Wilson[72.7–86.8]. Leakage failed 71/46 rows. No strict DNP voids found in this cut. Strong vs Standard BE, not Goblin Power bar.
- **NFL Std UNDER 97–100% / flat gate columns: do not use** (zero-actual UNDER hits + non-filtering L5 on sacks/longest).
- **NHL FOW 98% and Soccer Passes/Clearances 0%: grading bugs**, not edges.

