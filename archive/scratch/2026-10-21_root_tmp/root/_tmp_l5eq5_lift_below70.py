"""L5=5 only on every below-70% (base) prop — measure lift vs base.

No backfill. Thin L5=5 n still printed so we can see direction.
Fantasy excluded. Unique-game W1+W2.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_prop_gate_ledger import dedupe_unique_player_games, prepare_decided, rate

MAIN = Path(r"H:\PropORACLE_main_cp")
REPO = Path(r"H:\PropORACLE")
DATES = [
    "2026-09-09",
    "2026-09-10",
    "2026-09-13",
    "2026-09-14",
    "2026-09-20",
    "2026-09-21",
]


def load_box(d: str) -> pd.DataFrame | None:
    for root in (MAIN, REPO):
        p = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
        if not p.is_file():
            continue
        df = pd.read_excel(p, sheet_name="Box Raw")
        if df.empty or "result" not in df.columns:
            return None
        if int(df["result"].astype(str).str.upper().isin(["HIT", "MISS"]).sum()) == 0:
            return None
        out = df.copy()
        out["slate"] = d
        return out
    return None


def hn(g: pd.DataFrame) -> tuple[int, int, float | None]:
    n = len(g)
    if n == 0:
        return 0, 0, None
    h = int((g["result"] == "HIT").sum())
    return h, n, rate(h, n)


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    uniq = dedupe_unique_player_games(prepare_decided(boxes))
    uniq = uniq[~uniq["prop"].astype(str).str.lower().isin({"fantasy_score", "fantasy"})]

    rows: list[dict] = []
    for pick in ("Standard", "Goblin"):
        for side in ("UNDER", "OVER"):
            sub = uniq[(uniq["pick"] == pick) & (uniq["side"] == side)]
            for prop, g in sub.groupby("prop"):
                hb, nb, hrb = hn(g)
                if nb < 5 or hrb is None or hrb >= 70:
                    continue  # only the non-clearing base pool
                g5 = g[g["l5"] == 5]
                h5, n5, hr5 = hn(g5)
                lift = (hr5 - hrb) if hr5 is not None else None
                rows.append(
                    {
                        "pick": pick,
                        "side": side,
                        "prop": prop,
                        "base_hits": hb,
                        "base_n": nb,
                        "base_hr": hrb,
                        "l5_hits": h5,
                        "l5_n": n5,
                        "l5_hr": hr5,
                        "lift_pp": lift,
                    }
                )

    rows.sort(key=lambda r: (
        -(r["lift_pp"] if r["lift_pp"] is not None else -999),
        -(r["l5_n"] or 0),
    ))

    print(f"Base <70% props (no fantasy): {len(rows)}")
    print(f"{'Pick':8s} {'Side':5s} {'Prop':36s}  {'base':14s}  {'L5=5':14s}  {'lift':8s}  note")
    print("-" * 110)

    lifts = []
    hurts = []
    thin = []
    empty = []
    for r in rows:
        base = f"{r['base_hits']}/{r['base_n']} = {r['base_hr']:.0f}%"
        if r["l5_n"] == 0:
            l5s = "—"
            lift_s = "—"
            note = "no L5=5 rows"
            empty.append(r)
        else:
            l5s = f"{r['l5_hits']}/{r['l5_n']} = {r['l5_hr']:.0f}%"
            lift_s = f"{r['lift_pp']:+.0f}pp"
            note = ""
            if r["l5_n"] < 5:
                note = "thin"
                thin.append(r)
            if r["lift_pp"] is not None and r["lift_pp"] >= 5 and r["l5_n"] >= 5:
                note = (note + " **LIFT**").strip()
                lifts.append(r)
            elif r["lift_pp"] is not None and r["lift_pp"] <= -5 and r["l5_n"] >= 5:
                note = (note + " **HURT**").strip()
                hurts.append(r)
            elif r["l5_hr"] is not None and r["l5_hr"] >= 70 and r["l5_n"] >= 5:
                note = (note + " clears70").strip()
        print(
            f"{r['pick']:8s} {r['side']:5s} {r['prop']:36s}  "
            f"{base:14s}  {l5s:14s}  {lift_s:8s}  {note}"
        )

    print(f"\n=== LIFT (>=+5pp, L5=5 n>=5): {len(lifts)} ===")
    for r in lifts:
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:36s}  "
            f"{r['base_hr']:.0f}% -> {r['l5_hr']:.0f}%  "
            f"({r['l5_hits']}/{r['l5_n']})  {r['lift_pp']:+.0f}pp"
        )

    print(f"\n=== HURT (<=-5pp, L5=5 n>=5): {len(hurts)} ===")
    for r in hurts:
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:36s}  "
            f"{r['base_hr']:.0f}% -> {r['l5_hr']:.0f}%  "
            f"({r['l5_hits']}/{r['l5_n']})  {r['lift_pp']:+.0f}pp"
        )

    print(f"\n=== No L5=5 data: {len(empty)} | thin n<5: {len(thin)} ===")

    # Does L5=5 push any to clear 70?
    clears = [
        r for r in rows
        if r["l5_hr"] is not None and r["l5_hr"] >= 70 and r["l5_n"] >= 5
    ]
    print(f"\n=== Base <70% but L5=5 clears >=70% (n>=5): {len(clears)} ===")
    for r in sorted(clears, key=lambda x: (-x["l5_hr"], -x["l5_n"])):
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:36s}  "
            f"{r['l5_hits']}/{r['l5_n']} = {r['l5_hr']:.0f}%  "
            f"(base {r['base_hr']:.0f}%, {r['lift_pp']:+.0f}pp)"
        )

    out = REPO / "Sports" / "NFL" / "data" / "nfl_l5eq5_lift_on_below70_week1_2.csv"
    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
