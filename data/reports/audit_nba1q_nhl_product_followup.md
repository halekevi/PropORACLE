# NBA1Q / NHL product follow-up

## NBA1Q Goblin OVER Rebounds / Assists (L5=5)

### rebounds
- **FG join unchecked:** 187/428 = **43.7%** (checked=241; plausible_1q=235; fg_match_drop=6)
- Leakage counts: `{'pass': 372, 'unchecked': 2, 'fail': 48}` — Leakage uses FULL-GAME nba boxes vs 1Q lines (proxy). Fail drops; unchecked kept but reported separately.
- After drop leakage-fail (unchecked kept): **92.8% (347/374) Wilson[89.7–95.0]**
- Leakage-pass only: **92.7% (345/372) Wilson[89.6–95.0]**
- Distinct players=112, dates=49; top-10 share=**24.6%**; max player=**2.9%**
- Top-10 players:
  - Stephon Castle: 11 (2.9%)
  - Donovan Mitchell: 11 (2.9%)
  - Julian Champagnie: 10 (2.7%)
  - Ausar Thompson: 10 (2.7%)
  - Desmond Bane: 9 (2.4%)
  - Shai Gilgeous-Alexander: 9 (2.4%)
  - Devin Vassell: 8 (2.1%)
  - VJ Edgecombe: 8 (2.1%)
  - Naz Reid: 8 (2.1%)
  - Max Strus: 8 (2.1%)

### assists
- **FG join unchecked:** 79/217 = **36.4%** (checked=138; plausible_1q=119; fg_match_drop=19)
- Leakage counts: `{'pass': 180, 'unchecked': 2, 'fail': 16}` — Leakage uses FULL-GAME nba boxes vs 1Q lines (proxy). Fail drops; unchecked kept but reported separately.
- After drop leakage-fail (unchecked kept): **90.7% (165/182) Wilson[85.5–94.1]**
- Leakage-pass only: **90.6% (163/180) Wilson[85.4–94.0]**
- Distinct players=68, dates=44; top-10 share=**36.3%**; max player=**6.6%**
- Top-10 players:
  - Josh Hart: 12 (6.6%)
  - Karl-Anthony Towns: 8 (4.4%)
  - Jalen Brunson: 7 (3.9%)
  - Evan Mobley: 7 (3.9%)
  - Paolo Banchero: 6 (3.3%)
  - De'Aaron Fox: 6 (3.3%)
  - Stephon Castle: 5 (2.8%)
  - Derrick White: 5 (2.8%)
  - Amen Thompson: 5 (2.8%)
  - Jalen Suggs: 5 (2.8%)

## NHL Standard UNDER Points / Assists (L5=5+L10≥8)

### points
- Uncapped: **83.5% (81/97) Wilson[74.9–89.6]**
- Line 0.5: 41 (42.3%) → 78.0% (32/41) Wilson[63.3–88.0]
- Line ≠0.5: 56 (57.7%) → 87.5% (49/56) Wilson[76.4–93.8]
- By role (TOI proxy):
  - middle: n=40 (41.2%) → 80.0% (32/40) Wilson[65.2–89.5]
  - starter: n=38 (39.2%) → 92.1% (35/38) Wilson[79.2–97.3]
  - depth: n=14 (14.4%) → 78.6% (11/14) Wilson[52.4–92.4]
  - unknown: n=5 (5.1%) → 60.0% (3/5) Wilson[23.1–88.2]
- Depth ∩ 0.5: n=14 (14.4%) → 78.6% (11/14) Wilson[52.4–92.4]
- Starter ∩ 0.5: n=10 (10.3%) → 100.0% (10/10) Wilson[72.2–100.0]
- One leg/game **alpha pick (bettable):** **85.7% (24/28) Wilson[68.5–94.3]** (multi-leg games=25, avg legs/game=3.464). Discard prefer-HIT 96% — that picks the winning leg after grading (hindsight).
- Rule 4: does **not** apply to NHL UNDER (depth is why UNDER hits).

### assists
- Uncapped: **80.7% (96/119) Wilson[72.7–86.8]**
- Line 0.5: 88 (74.0%) → 79.5% (70/88) Wilson[70.0–86.7]
- Line ≠0.5: 31 (26.1%) → 83.9% (26/31) Wilson[67.4–92.9]
- By role (TOI proxy):
  - middle: n=55 (46.2%) → 83.6% (46/55) Wilson[71.7–91.1]
  - starter: n=38 (31.9%) → 73.7% (28/38) Wilson[58.0–85.0]
  - depth: n=17 (14.3%) → 82.3% (14/17) Wilson[59.0–93.8]
  - unknown: n=9 (7.6%) → 88.9% (8/9) Wilson[56.5–98.0]
- Depth ∩ 0.5: n=17 (14.3%) → 82.3% (14/17) Wilson[59.0–93.8]
- Starter ∩ 0.5: n=19 (16.0%) → 68.4% (13/19) Wilson[46.0–84.6]
- One leg/game **alpha pick (bettable):** **81.5% (22/27) Wilson[63.3–91.8]** (multi-leg games=25, avg legs/game=4.407). Discard prefer-HIT 100% — hindsight.
- Rule 4: does **not** apply to NHL UNDER.

## Product decision notes

- Cap NHL UNDER at **one leg per game** on slips (correlation) — wired in `build_goblin70_tickets.can_add`. Honest backtest rate is **alpha pick ~82–86%**, not prefer-HIT.
- **Rule 4 does NOT apply to NHL Std UNDER.** NFL “no 0.5 on role players” is an OVER/volume concern. On UNDER, depth is why the leg hits. Keep 0.5 depth legs.
- NBA1Q post-leakage **92.8% / 90.7%** are an **upper bound**: leakage uses full-game boxes as an outcome-adjacent proxy; unchecked FG join is still 43.7% / 36.4%. Concentration is fine (top-10 ~25% reb). Paper only until n≥100 + Wilson low > BE at real quotes.
