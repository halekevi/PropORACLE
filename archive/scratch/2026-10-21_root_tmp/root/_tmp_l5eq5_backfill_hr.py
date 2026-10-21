"""Prop hit rates under L5=5; backfill L5>=4 then base when thin.

Unique-game W1+W2. Soft reporting only.
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
MIN_L5EQ5 = 8  # prefer L5=5 when n >= this
MIN_BACKFILL = 5  # else L5>=4 if n >= this; else base


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


def pick_gate(g: pd.DataFrame) -> tuple[str, int, int, float | None, int, int, float | None, int, int, float | None]:
    """Return (source, h, n, hr, l5_5_h, l5_5_n, l5_5_hr, l5_4_h, l5_4_n, l5_4_hr)."""
    g5 = g[g["l5"] == 5]
    g4 = g[g["l5"] >= 4]
    h5, n5, hr5 = hn(g5)
    h4, n4, hr4 = hn(g4)
    hb, nb, hrb = hn(g)

    if n5 >= MIN_L5EQ5:
        src, h, n, hr = "L5=5", h5, n5, hr5
    elif n4 >= MIN_BACKFILL:
        src, h, n, hr = "L5>=4 backfill", h4, n4, hr4
    else:
        src, h, n, hr = "base backfill", hb, nb, hrb
    return src, h, n, hr, h5, n5, hr5, h4, n4, hr4


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    uniq = dedupe_unique_player_games(prepare_decided(boxes))
    print(
        f"unique-game={len(uniq)}  gate=L5=5 (min n={MIN_L5EQ5}); "
        f"backfill L5>=4 (min n={MIN_BACKFILL}) then base\n"
    )

    rows: list[dict] = []
    for pick in ("Standard", "Goblin"):
        for side in ("UNDER", "OVER"):
            sub = uniq[(uniq["pick"] == pick) & (uniq["side"] == side)]
            if sub.empty:
                continue
            print(f"\n## {pick} {side}")
            print(
                f"{'Prop':32s}  {'gated':18s}  {'src':18s}  "
                f"{'L5=5 raw':14s}  {'L5>=4 raw':14s}  {'base':14s}"
            )
            print("-" * 120)
            prop_rows = []
            for prop, g in sub.groupby("prop"):
                src, h, n, hr, h5, n5, hr5, h4, n4, hr4 = pick_gate(g)
                hb, nb, hrb = hn(g)
                if nb < 5:
                    continue
                if str(prop).lower() in {"fantasy_score", "fantasy"}:
                    continue
                prop_rows.append(
                    {
                        "pick": pick,
                        "side": side,
                        "prop": prop,
                        "src": src,
                        "hits": h,
                        "n": n,
                        "hr": hr,
                        "l5_5": f"{h5}/{n5}" if n5 else "—",
                        "l5_5_hr": hr5,
                        "l5_4": f"{h4}/{n4}" if n4 else "—",
                        "l5_4_hr": hr4,
                        "base": f"{hb}/{nb}",
                        "base_hr": hrb,
                    }
                )
            prop_rows.sort(key=lambda r: (r["hr"] if r["hr"] is not None else -1, r["n"]))
            for r in prop_rows:
                gated = f"{r['hits']}/{r['n']} = {r['hr']:.0f}%"
                l5s = (
                    f"{r['l5_5']} = {r['l5_5_hr']:.0f}%"
                    if r["l5_5"] != "—" and r["l5_5_hr"] is not None
                    else "—"
                )
                l54 = (
                    f"{r['l5_4']} = {r['l5_4_hr']:.0f}%"
                    if r["l5_4"] != "—" and r["l5_4_hr"] is not None
                    else "—"
                )
                base = f"{r['base']} = {r['base_hr']:.0f}%"
                flag = "  <<70" if r["hr"] is not None and r["hr"] < 70 else ""
                print(
                    f"{r['prop']:32s}  {gated:18s}  {r['src']:18s}  "
                    f"{l5s:14s}  {l54:14s}  {base:14s}{flag}"
                )
                rows.append(r)

    below = [r for r in rows if r["hr"] is not None and r["hr"] < 70]
    above = [r for r in rows if r["hr"] is not None and r["hr"] >= 70]
    print(f"\n=== BELOW 70% under L5=5-or-backfill ({len(below)}) ===")
    for r in sorted(below, key=lambda x: (x["hr"], -x["n"])):
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:32s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.0f}%  [{r['src']}]"
        )
    print(f"\n=== CLEARS 70% under L5=5-or-backfill ({len(above)}) ===")
    for r in sorted(above, key=lambda x: (-x["hr"], -x["n"])):
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:32s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.0f}%  [{r['src']}]"
        )

    out = REPO / "Sports" / "NFL" / "data" / "nfl_prop_l5eq5_backfill_week1_2.csv"
    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
