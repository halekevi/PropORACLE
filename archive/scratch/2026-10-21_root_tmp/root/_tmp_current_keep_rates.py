"""Current NFL keep-gate hit rates on W1+W2 unique-game ledger."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_keep_gates import (
    NFL_FANTASY_OFF,
    NFL_GOBLIN_OVER_KEEP,
    NFL_GOBLIN_OVER_WATCH,
    NFL_STD_OVER_KEEP,
    NFL_STD_UNDER_KEEP,
    NFL_STD_UNDER_SKIP_L5_EQ5,
    NFL_STD_UNDER_WATCH,
)
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


def load(d: str) -> pd.DataFrame | None:
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


def cell(g: pd.DataFrame | None) -> str:
    if g is None or len(g) == 0:
        return "--"
    h = int((g["result"] == "HIT").sum())
    n = len(g)
    return f"{h}/{n} = {rate(h, n):.0f}%"


def block(
    title: str,
    u: pd.DataFrame,
    pick: str,
    side: str,
    props: list[str],
    *,
    skip_l5: set[str] | frozenset[str] | None = None,
) -> None:
    skip_l5 = skip_l5 or set()
    print(f"\n## {title}")
    hdr = f"{'Prop':28s}  {'base':16s}  {'L5>=4':16s}  {'L5=5':16s}  {'G70':16s}  {'TICKET':18s}"
    print(hdr)
    print("-" * len(hdr))
    for prop in props:
        g = u[(u.prop == prop) & (u.side == side) & (u.pick == pick)]
        g4 = g[g.l5 >= 4]
        g5 = g[g.l5 == 5]
        g70 = g[g.gate70]
        if prop in skip_l5:
            tg = g[(g.l10 >= 8) & (g.dok == True)]  # noqa: E712
        else:
            tg = g70
        print(
            f"{prop:28s}  {cell(g):16s}  {cell(g4):16s}  {cell(g5):16s}  "
            f"{cell(g70):16s}  {cell(tg):18s}"
        )


def main() -> None:
    boxes = [b for d in DATES if (b := load(d)) is not None]
    dec = prepare_decided(boxes)
    u = dedupe_unique_player_games(dec)
    print(f"W1+W2 unique-game={len(u)} slates={sorted(u.slate.unique())}")
    print("Format: HITS/DECIDED (= %). TICKET = G70, or L10>=8+D when L5=5 skipped.")
    print(f"Std UNDER keep: {sorted(NFL_STD_UNDER_KEEP)}")
    print(f"Skip L5=5:      {sorted(NFL_STD_UNDER_SKIP_L5_EQ5)}")
    print(f"Std OVER keep:  {sorted(NFL_STD_OVER_KEEP)}")
    print(f"Goblin keep:    {sorted(NFL_GOBLIN_OVER_KEEP)} (master flag OFF)")
    print(f"UNDER watch:    {sorted(NFL_STD_UNDER_WATCH)}")
    print(f"Goblin watch:   {sorted(NFL_GOBLIN_OVER_WATCH)}")
    print(f"Fantasy off:    {sorted(NFL_FANTASY_OFF)}")

    under_keep = [
        "receptions",
        "longest_reception",
        "sacks",
        "tackles_ast",
        "kicking_points",
        "receiving_yards",
    ]
    block(
        "Standard UNDER KEEP",
        u,
        "Standard",
        "UNDER",
        under_keep,
        skip_l5=NFL_STD_UNDER_SKIP_L5_EQ5,
    )
    block(
        "Standard UNDER WATCH",
        u,
        "Standard",
        "UNDER",
        sorted(NFL_STD_UNDER_WATCH),
    )
    block("Standard OVER KEEP", u, "Standard", "OVER", sorted(NFL_STD_OVER_KEEP))
    block(
        "Goblin OVER KEEP (staged)",
        u,
        "Goblin",
        "OVER",
        sorted(NFL_GOBLIN_OVER_KEEP),
    )
    block(
        "Goblin OVER WATCH",
        u,
        "Goblin",
        "OVER",
        sorted(NFL_GOBLIN_OVER_WATCH),
    )

    print("\n## Gated / dead reference")
    for prop, side, pick in [
        ("sacks_taken", "UNDER", "Standard"),
        ("completion_percentage", "UNDER", "Standard"),
        ("fantasy_score", "UNDER", "Standard"),
        ("fantasy_score", "OVER", "Goblin"),
        ("passing_yards", "UNDER", "Standard"),
        ("passing_yards", "OVER", "Standard"),
        ("player_touchdowns", "OVER", "Standard"),
        ("rush_yards_per_carry", "OVER", "Standard"),
        ("longest_rush", "UNDER", "Standard"),
    ]:
        g = u[(u.prop == prop) & (u.side == side) & (u.pick == pick)]
        g5 = g[g.l5 == 5]
        g70 = g[g.gate70]
        print(
            f"  {prop:28s} {pick:8s} {side:5s}  "
            f"base {cell(g):16s}  L5=5 {cell(g5):16s}  G70 {cell(g70)}"
        )

    # Pool rollup: if we ticketed every keep row that clears TICKET gate
    print("\n## Allowlist pool rollup (TICKET-clear rows only)")
    rows = []
    for prop in under_keep:
        g = u[(u.prop == prop) & (u.side == "UNDER") & (u.pick == "Standard")]
        if prop in NFL_STD_UNDER_SKIP_L5_EQ5:
            tg = g[(g.l10 >= 8) & (g.dok == True)]  # noqa: E712
        else:
            tg = g[g.gate70]
        if len(tg):
            rows.append(tg)
    for prop in NFL_STD_OVER_KEEP:
        g = u[(u.prop == prop) & (u.side == "OVER") & (u.pick == "Standard")]
        tg = g[g.gate70]
        if len(tg):
            rows.append(tg)
    if rows:
        pool = pd.concat(rows, ignore_index=True)
        print(f"  Std keep TICKET pool: {cell(pool)}")
    # Goblin staged (even though flag off)
    grows = []
    for prop in NFL_GOBLIN_OVER_KEEP:
        g = u[(u.prop == prop) & (u.side == "OVER") & (u.pick == "Goblin")]
        tg = g[g.gate70]
        if len(tg):
            grows.append(tg)
    if grows:
        gpool = pd.concat(grows, ignore_index=True)
        print(f"  Goblin keep G70 pool (flag off): {cell(gpool)}")


if __name__ == "__main__":
    main()
