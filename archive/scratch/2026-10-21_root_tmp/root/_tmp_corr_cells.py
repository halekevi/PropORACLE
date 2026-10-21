"""Summarize leg-correlation cells: counts, lifts, prior sensitivity."""
from __future__ import annotations

import json
from pathlib import Path

cells = json.loads(Path("data/cache/leg_correlation_matrix.json").read_text(encoding="utf-8"))
CATS = [
    "cat1_same_player_diff_stat",
    "cat2_same_team_diff_player_same_stat",
    "cat3_opposing_teams_same_game",
    "cat4_baseline_diff_game",
]


def pct(ns: list[int], p: float) -> int:
    if not ns:
        return 0
    s = sorted(ns)
    i = int(round((len(s) - 1) * p))
    return s[i]


print("=== cell counts by sport x category (n-weighted lifts) ===")
hdr = (
    f"{'sport':12} {'cat':40} {'cells':>7} {'sum_n':>10} {'med_n':>6} "
    f"{'p25':>6} {'p75':>6} {'w_raw':>8} {'w_shr':>8} {'n<25':>7} {'n<50':>7}"
)
print(hdr)
for sport in sorted(cells):
    for cat in CATS:
        entries = cells[sport].get(cat) or []
        if not entries:
            continue
        ns = [e["n"] for e in entries]
        wsum = sum(ns)
        wraw = sum(e["raw_lift"] * e["n"] for e in entries) / wsum
        wshr = sum(e["shrunk_lift"] * e["n"] for e in entries) / wsum
        thin25 = sum(1 for n in ns if n < 25) / len(ns)
        thin50 = sum(1 for n in ns if n < 50) / len(ns)
        print(
            f"{sport:12} {cat:40} {len(entries):7d} {wsum:10d} {pct(ns, 0.5):6d} "
            f"{pct(ns, 0.25):6d} {pct(ns, 0.75):6d} {wraw:8.3f} {wshr:8.3f} "
            f"{thin25:7.1%} {thin50:7.1%}"
        )

print("\n=== prior_strength sensitivity (all sports, n-weighted) ===")
for prior in [5, 10, 25, 50, 100]:
    print(f"-- prior={prior}")
    for cat in CATS:
        all_e = []
        for _sport, cats in cells.items():
            all_e.extend(cats.get(cat) or [])
        if not all_e:
            continue

        def shrunk(e: dict, p: int = prior) -> float:
            w = e["n"] / (e["n"] + p)
            return w * e["raw_lift"] + (1 - w) * 1.0

        wsum = sum(e["n"] for e in all_e)
        wshr = sum(shrunk(e) * e["n"] for e in all_e) / wsum
        wraw = sum(e["raw_lift"] * e["n"] for e in all_e) / wsum
        med_n = pct([e["n"] for e in all_e], 0.5)
        print(
            f"  {cat:40} cells={len(all_e):5d} med_n={med_n:5d} "
            f"w_raw={wraw:.3f} w_shrunk={wshr:.3f}"
        )

print("\n=== baseline diagnosis: is lift!=1 calibration or join? ===")
# Compare empirical joint vs product of empirical marginals would need raw hits;
# proxy: if raw_lift tracks (p_joint / expected) and expected uses model_prob,
# overconfident models -> lift < 1 even under independence.
for sport in sorted(cells):
    base = cells[sport].get("cat4_baseline_diff_game") or []
    if not base:
        continue
    wsum = sum(e["n"] for e in base)
    emp = sum(e["p_joint_empirical"] * e["n"] for e in base) / wsum
    exp = sum(e["expected_under_independence"] * e["n"] for e in base) / wsum
    wraw = sum(e["raw_lift"] * e["n"] for e in base) / wsum
    print(
        f"{sport:12} n={wsum:8d} emp_joint={emp:.4f} model_E[pa*pb]={exp:.4f} "
        f"ratio={emp/exp if exp else float('nan'):.3f} w_raw={wraw:.3f}"
    )
