#!/usr/bin/env python3
"""Find soccer combo gates with HR around 65%+."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

SRC = Path(r"H:\PropORACLE\data\reports\soccer_combo_gates_latest.xlsx")
SKIP = {"all"}  # ungated baseline


def melt_gates(df: pd.DataFrame, slice_name: str) -> pd.DataFrame:
    names = []
    for c in df.columns:
        if c.endswith("_hr") and c[:-3] + "_n" in df.columns:
            g = c[:-3]
            if g not in SKIP:
                names.append(g)
    rows = []
    for _, r in df.iterrows():
        for g in names:
            n = r.get(g + "_n")
            hr = r.get(g + "_hr")
            hits = r.get(g + "_hits")
            if pd.isna(n) or pd.isna(hr):
                continue
            n_i = int(n)
            if n_i <= 0:
                continue
            rows.append(
                {
                    "slice": slice_name,
                    "prop": r["prop"],
                    "gate": g,
                    "hits": int(hits) if pd.notna(hits) else None,
                    "n": n_i,
                    "hr": float(hr),
                    "baseline_hr": r.get("all_hr"),
                    "baseline_n": r.get("all_n"),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    parts = []
    for sheet, sl in [
        ("Combo HR Std+Goblin", "Std+Goblin"),
        ("Combo HR Goblin OVER", "Goblin OVER"),
        ("Combo HR Standard", "Standard"),
    ]:
        parts.append(melt_gates(pd.read_excel(SRC, sheet), sl))
    df = pd.concat(parts, ignore_index=True)
    df["pct"] = 100 * df["hr"]

    def show(title: str, sub: pd.DataFrame) -> None:
        print(title)
        if sub.empty:
            print("  (none)")
            print()
            return
        sub = sub.sort_values(["hr", "n"], ascending=[False, False])
        for _, r in sub.iterrows():
            print(
                f"  {r['slice']:<13} {str(r['prop']):<20} {str(r['gate']):<28} "
                f"{int(r['hits'])}/{int(r['n'])}={r['pct']:.1f}%"
            )
        print()

    # Dedup: same prop+gate can appear in all three slices; keep each slice.
    show("HR >= 65%  n>=20", df[(df["hr"] >= 0.65) & (df["n"] >= 20)])
    show("HR >= 65%  n>=10 (not already n>=20)", df[(df["hr"] >= 0.65) & (df["n"] >= 10) & (df["n"] < 20)])
    show("HR >= 60%  n>=20 (near-misses)", df[(df["hr"] >= 0.60) & (df["hr"] < 0.65) & (df["n"] >= 20)])
    show("HR >= 65%  n>=5 (thin)", df[(df["hr"] >= 0.65) & (df["n"] >= 5) & (df["n"] < 10)])

    # Per prop: best n>=20, else n>=10, if >=65
    print("BEST >=65% PER PROP (prefer n>=20, else n>=10)  Std+Goblin")
    play = df[df["slice"] == "Std+Goblin"]
    for prop, sub in play.groupby("prop"):
        a = sub[sub["n"] >= 20]
        b = sub[sub["n"] >= 10]
        pool = a if not a.empty else b
        if pool.empty:
            continue
        best = pool.sort_values(["hr", "n"], ascending=[False, False]).iloc[0]
        mark = "CLEARS 65" if best["hr"] >= 0.65 else f"best {best['pct']:.1f}%"
        print(
            f"  {prop:<20} {best['gate']:<28} "
            f"{int(best['hits'])}/{int(best['n'])}={best['pct']:.1f}%  {mark}"
        )


if __name__ == "__main__":
    main()
