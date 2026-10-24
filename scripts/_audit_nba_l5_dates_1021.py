"""Audit early-season L5: preseason + last RS with real dates; card RS-only for contrast."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears, play_side_gap
from utils.l5_ranked_by_cell import five_games, gap_z
from utils.nba_minutes_role import _norm_name, load_nba_box_frame

_PROP_COL = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "steals": "stl",
    "blocked shots": "blk",
    "turnovers": "tov",
    "3-pt made": "fg3m",
    "pts+rebs+asts": "pra",
    "pts+rebs": "pr",
    "pts+asts": "pa",
    "rebs+asts": "ra",
}


def _col(prop: str) -> str | None:
    return _PROP_COL.get(str(prop or "").strip().lower())


def recent_games(boxes: pd.DataFrame, player: str, prop: str, *, n: int = 8):
    col = _col(prop)
    if not col:
        return None
    pn = _norm_name(player)
    g = boxes[boxes["_pn"] == pn].copy()
    if g.empty:
        return None
    usable = g[g["window"].isin(["preseason", "regular"])].sort_values(
        "game_date", ascending=False
    )
    rows = []
    for _, row in usable.iterrows():
        try:
            mins = float(row.get("minutes") or 0)
            v = float(row.get(col))
        except (TypeError, ValueError):
            continue
        if math.isnan(v) or mins <= 0:
            continue
        rows.append(
            {
                "date": str(row["game_date"])[:10],
                "window": row["window"],
                "minutes": mins,
                "val": v,
            }
        )
        if len(rows) >= n:
            break
    return rows


def l5_blend(rows, n=5):
    """Newest-first preseason+RS (already sorted)."""
    take = rows[:n]
    if len(take) < n:
        return None
    return take


def l5_rs_only(boxes, player, prop, n=5):
    col = _col(prop)
    pn = _norm_name(player)
    g = boxes[(boxes["_pn"] == pn) & (boxes["window"] == "regular")].sort_values(
        "game_date", ascending=False
    )
    rows = []
    for _, row in g.iterrows():
        try:
            mins = float(row.get("minutes") or 0)
            v = float(row.get(col))
        except (TypeError, ValueError):
            continue
        if math.isnan(v) or mins <= 0:
            continue
        rows.append(
            {
                "date": str(row["game_date"])[:10],
                "window": "regular",
                "minutes": mins,
                "val": v,
            }
        )
        if len(rows) >= n:
            break
    return rows if len(rows) >= n else None


def show(player, prop, side, line, boxes):
    print(f"\n=== {player} | {prop} {side} {line} ===")
    recent = recent_games(boxes, player, prop, n=10)
    if not recent:
        print("  no boxes")
        return
    print("  recent (date window min val):")
    for r in recent:
        print(f"    {r['date']} {r['window'][:3]:3} min={r['minutes']:4.1f}  {prop[:6]}={r['val']:g}")

    blend = l5_blend(recent, 5)
    rs = l5_rs_only(boxes, player, prop, 5)
    for label, window in (("BLEND pre+RS", blend), ("CARD-style RS-only", rs)):
        if not window:
            print(f"  {label}: incomplete")
            continue
        vals = [r["val"] for r in window]
        dates = [r["date"] for r in window]
        tags = ["P" if r["window"] == "preseason" else "R" for r in window]
        avg = sum(vals) / 5
        clears = l5_clears(vals, line, side)
        gap = play_side_gap(avg, line, side)
        print(
            f"  {label}: vals={vals} tags={''.join(tags)} dates={dates} "
            f"avg={avg:.2f} gap={gap:.2f} L5=5={clears}"
        )


def wnba_check():
    path = Path(r"H:\PropORACLE_main_cp\outputs\2026-10-21\wnba\step8_wnba_direction_clean.xlsx")
    print("\n=== WNBA Standard L5=5 singles ===")
    if not path.exists():
        print("  missing", path)
        return
    df = pd.read_excel(path, sheet_name="ALL")
    for name in ("Kaitlyn Chen", "Veronica Burton"):
        sub = df[df["Player"].astype(str) == name]
        sub = sub[sub["Pick Type"].astype(str).str.contains("Standard", case=False, na=False)]
        for _, r in sub.iterrows():
            d = r.to_dict()
            vals = five_games(d)
            side = str(r.get("Direction") or "").upper()
            try:
                line = float(r["Line"])
            except (TypeError, ValueError):
                continue
            ok = bool(vals and l5_clears(vals, line, side))
            if not ok:
                continue
            print(
                f"  {name} | {r.get('Prop')} {side} {line} | L5={vals} "
                f"tier={r.get('Tier')} team={r.get('Team')} "
                f"opp={r.get('Opp') or r.get('Opp Team')}"
            )


def main():
    boxes = load_nba_box_frame()
    checks = [
        ("Egor Dëmin", "Rebounds", "UNDER", 3.5),
        ("Egor Dëmin", "Pts+Rebs", "UNDER", 15.5),
        ("De'Anthony Melton", "3-PT Made", "UNDER", 1.5),
        ("Darius Garland", "Points", "UNDER", 23.5),
        ("Franz Wagner", "Pts+Rebs", "UNDER", 27.5),
        ("Franz Wagner", "Pts+Rebs+Asts", "UNDER", 32.5),
        ("Isaiah Jackson", "Rebounds", "UNDER", 7.5),
    ]
    for args in checks:
        show(*args, boxes)
    wnba_check()


if __name__ == "__main__":
    main()
