"""Print current W1+W2 unique-game prop hit rates."""
from __future__ import annotations

import pandas as pd

df = pd.read_csv("Sports/NFL/data/nfl_prop_gate_ledger_week1_2.csv")
g = df[df.pick_type.isin(["Standard", "Goblin"]) & (df.n >= 5)].copy()


def cell(h, n, hr) -> str:
    if pd.isna(n) or n <= 0:
        return "—"
    return f"{int(h)}/{int(n)} = {hr:.0f}%"


for pick in ("Standard", "Goblin"):
    for side in ("UNDER", "OVER"):
        sub = g[(g.pick_type == pick) & (g.side == side)].sort_values("hr", ascending=False)
        if sub.empty:
            continue
        print(f"\n## {pick} {side}  (unique-game W1+W2)")
        print(f"{'Prop':32s}  {'base':16s}  {'L5=5':16s}  {'G70':16s}")
        print("-" * 90)
        for _, r in sub.iterrows():
            print(
                f"{r.prop:32s}  "
                f"{cell(r.hits, r.n, r.hr):16s}  "
                f"{cell(r.l5_eq5_hits, r.l5_eq5_n, r.l5_eq5_hr):16s}  "
                f"{cell(r.gate70_hits, r.gate70_n, r.gate70_hr):16s}"
            )
