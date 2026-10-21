"""Soft-split sacks_taken UNDER by QB mobility; rush/rec TDs by red zone."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_context_layers import attach_nfl_context_layers
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


def show(title: str, g: pd.DataFrame, col: str) -> None:
    print(f"\n### {title}  n={len(g)}")
    if g.empty:
        print("  (no rows)")
        return
    g = g.copy()
    g["hit"] = g["result"].astype(str).str.upper().eq("HIT")
    for tier, sg in g.groupby(col, dropna=False):
        h = int(sg["hit"].sum())
        n = len(sg)
        print(f"  {str(tier):18s}  {h:3d}/{n:<3d} = {rate(h, n):5.1f}%")


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    dec = prepare_decided(boxes)
    uniq = dedupe_unique_player_games(dec)
    uniq = attach_nfl_context_layers(uniq, REPO)
    print(
        f"unique={len(uniq)} mobility_cov="
        f"{uniq['qb_mobility_tier'].notna().mean()*100:.0f}% "
        f"rz_team_cov={uniq['own_rz_rush_rate'].notna().mean()*100:.0f}% "
        f"rz_player_cov={uniq['player_rz_rush_share'].notna().mean()*100:.0f}%"
    )

    st = uniq[(uniq.prop == "sacks_taken") & (uniq.side == "UNDER") & (uniq.pick == "Standard")]
    show("sacks_taken UNDER Standard (all)", st, "qb_mobility_tier")
    show(
        "sacks_taken UNDER Standard L5=5",
        st[st.l5 == 5],
        "qb_mobility_tier",
    )

    for prop in ("rush_tds", "rushing_tds", "rec_tds", "receiving_tds"):
        g = uniq[(uniq.prop == prop) & (uniq.side == "OVER") & (uniq.pick == "Standard")]
        if g.empty:
            continue
        show(f"{prop} OVER Standard x own_rz_rush_tier", g, "own_rz_rush_tier")
        show(f"{prop} OVER Standard x player_high_rz_rush", g, "player_high_rz_rush")
        if "target" in prop or prop.startswith("rec"):
            show(f"{prop} OVER Standard x player_high_rz_target", g, "player_high_rz_target")

    out = REPO / "Sports" / "NFL" / "data" / "nfl_context_soft_attach_week1_2.csv"
    uniq.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
