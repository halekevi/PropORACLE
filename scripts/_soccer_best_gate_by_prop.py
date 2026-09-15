#!/usr/bin/env python3
"""Best-hitting gate per soccer prop from soccer_combo_gates_latest.xlsx."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
SRC = _REPO / "data" / "reports" / "soccer_combo_gates_latest.xlsx"
SKIP = {"all"}


def gate_names(df: pd.DataFrame) -> list[str]:
    names = []
    for c in df.columns:
        if c.endswith("_hr") and c[:-3] + "_n" in df.columns:
            g = c[:-3]
            if g not in SKIP:
                names.append(g)
    return names


def pick_row(row: pd.Series, names: list[str], min_n: int):
    best = None
    for g in names:
        n = row.get(g + "_n")
        hr = row.get(g + "_hr")
        hits = row.get(g + "_hits")
        if pd.isna(n) or pd.isna(hr) or int(n) < min_n:
            continue
        n_i = int(n)
        hr_f = float(hr)
        hits_i = int(hits) if pd.notna(hits) else None
        key = (hr_f, n_i)
        if best is None or key > (best[0], best[1]):
            best = (hr_f, n_i, hits_i, g)
    return best


def table(df: pd.DataFrame, slice_name: str) -> pd.DataFrame:
    names = gate_names(df)
    rows = []
    for _, r in df.iterrows():
        b20 = pick_row(r, names, 20)
        b8 = pick_row(r, names, 8)
        b1 = pick_row(r, names, 1)
        thin = b1 if b1 and (not b8 or b1[3] != b8[3]) else None
        rows.append(
            {
                "slice": slice_name,
                "prop": r["prop"],
                "n_pool": int(r["n_board"]),
                "best_n20_gate": b20[3] if b20 else None,
                "best_n20_hits": b20[2] if b20 else None,
                "best_n20_n": b20[1] if b20 else None,
                "best_n20_hr": b20[0] if b20 else None,
                "best_n8_gate": b8[3] if b8 else None,
                "best_n8_hits": b8[2] if b8 else None,
                "best_n8_n": b8[1] if b8 else None,
                "best_n8_hr": b8[0] if b8 else None,
                "thin_gate": thin[3] if thin else None,
                "thin_hits": thin[2] if thin else None,
                "thin_n": thin[1] if thin else None,
                "thin_hr": thin[0] if thin else None,
                "baseline_hr": r.get("all_hr"),
                "baseline_n": r.get("all_n"),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    parts = []
    for sheet, sl in [
        ("Combo HR Std+Goblin", "Std+Goblin"),
        ("Combo HR Goblin OVER", "Goblin OVER"),
        ("Combo HR Standard", "Standard"),
    ]:
        df = pd.read_excel(SRC, sheet)
        parts.append(table(df, sl))
    best = pd.concat(parts, ignore_index=True)

    play = best[best["slice"] == "Std+Goblin"].copy()
    play["best_gate"] = play["best_n20_gate"].fillna(play["best_n8_gate"])
    play["hit_rate"] = play["best_n20_hr"].fillna(play["best_n8_hr"])
    play["n"] = play["best_n20_n"].fillna(play["best_n8_n"])
    play["hits"] = play["best_n20_hits"].fillna(play["best_n8_hits"])
    play["min_n_used"] = play["best_n20_gate"].notna().map({True: 20, False: 8})
    play = play.sort_values(["hit_rate", "n"], ascending=[False, False])

    print("BEST GATE BY PROP  Std+Goblin  (n>=20, else n>=8)")
    print("")
    for _, r in play.iterrows():
        if pd.isna(r["best_gate"]):
            print(
                f"  {r['prop']:<22} (no gated n>=8)  ungated {100 * float(r['baseline_hr']):.1f}% n={int(r['baseline_n'])}"
            )
            continue
        extra = ""
        if pd.notna(r["thin_gate"]):
            extra = (
                f"  | thin {r['thin_gate']} "
                f"{int(r['thin_hits'])}/{int(r['thin_n'])}={100 * float(r['thin_hr']):.0f}%"
            )
        print(
            f"  {r['prop']:<22} {r['best_gate']:<28} "
            f"{int(r['hits'])}/{int(r['n'])}={100 * float(r['hit_rate']):.1f}%  "
            f"vs ungated {100 * float(r['baseline_hr']):.1f}%{extra}"
        )

    print("")
    print("GOBLIN OVER")
    g = best[best["slice"] == "Goblin OVER"]
    for _, r in g.iterrows():
        gate = r["best_n20_gate"] if pd.notna(r["best_n20_gate"]) else r["best_n8_gate"]
        hr = r["best_n20_hr"] if pd.notna(r["best_n20_gate"]) else r["best_n8_hr"]
        n = r["best_n20_n"] if pd.notna(r["best_n20_gate"]) else r["best_n8_n"]
        hits = r["best_n20_hits"] if pd.notna(r["best_n20_gate"]) else r["best_n8_hits"]
        if pd.isna(gate):
            print(f"  {r['prop']:<22} (no gated n>=8)")
            continue
        print(f"  {r['prop']:<22} {gate:<28} {int(hits)}/{int(n)}={100 * float(hr):.1f}%")

    print("")
    print("STANDARD")
    s = best[best["slice"] == "Standard"]
    for _, r in s.iterrows():
        gate = r["best_n20_gate"] if pd.notna(r["best_n20_gate"]) else r["best_n8_gate"]
        hr = r["best_n20_hr"] if pd.notna(r["best_n20_gate"]) else r["best_n8_hr"]
        n = r["best_n20_n"] if pd.notna(r["best_n20_gate"]) else r["best_n8_n"]
        hits = r["best_n20_hits"] if pd.notna(r["best_n20_gate"]) else r["best_n8_hits"]
        if pd.isna(gate):
            print(f"  {r['prop']:<22} (no gated n>=8)")
            continue
        print(f"  {r['prop']:<22} {gate:<28} {int(hits)}/{int(n)}={100 * float(hr):.1f}%")

    compact = play[
        [
            "prop",
            "n_pool",
            "best_gate",
            "hits",
            "n",
            "hit_rate",
            "min_n_used",
            "baseline_hr",
            "thin_gate",
            "thin_n",
            "thin_hr",
        ]
    ]
    paths = [
        SRC,
        _REPO / "outputs" / "2026-09-03" / "soccer_combo_gates.xlsx",
        Path(r"H:\PropORACLE_main_cp") / "outputs" / "2026-09-03" / "soccer" / "soccer_combo_gates.xlsx",
        _REPO / "data" / "reports" / "soccer_hit_rates_gates_latest.xlsx",
    ]
    for path in paths:
        if not path.exists():
            continue
        try:
            with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
                compact.to_excel(w, sheet_name="Best gate by prop", index=False)
                best.to_excel(w, sheet_name="Best gate all slices", index=False)
            print(f"wrote {path}")
        except PermissionError:
            print(f"locked {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
