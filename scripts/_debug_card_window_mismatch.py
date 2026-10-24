"""Debug Mitchell/Williams/Wallace card-window vs user-pulled values."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears
from utils.nba_early_l5 import (
    card_l5_ok,
    l5_card_window,
    l5_preseason,
    preseason_flag,
    prop_col,
    window_mix,
)
from utils.nba_minutes_role import load_nba_box_frame

CHECKS = [
    ("Ajay Mitchell", "Rebs+Asts", 3.5),
    ("Jalen Williams", "Pts+Asts", 16.5),
    ("Jalen Williams", "Pts+Rebs", 16.5),
    ("VJ Edgecombe", "Points", 5.5),
    ("Cason Wallace", "3-PT Made", 0.5),
    ("Cason Wallace", "3PM", 0.5),
    ("Anfernee Simons", "Points", 3.5),
]

USER = {
    ("Ajay Mitchell", "Rebs+Asts"): [6, 2, 9, 7, 14],
    ("Jalen Williams", "Pts+Asts"): [2, 4, 29, 23, 28],
    ("Jalen Williams", "Pts+Rebs"): [1, 5, 33, 20, 29],
    ("VJ Edgecombe", "Points"): [8, 11, 17, 12, 23],
}


def main() -> int:
    boxes = load_nba_box_frame()
    for player, prop, line in CHECKS:
        print("=" * 72)
        print(f"{player} {prop} O{line}")
        col = prop_col(prop)
        print(f"  prop_col={col}")
        pn = player.lower()
        g = boxes[boxes["_pn"] == pn].sort_values("game_date", ascending=False)
        print(f"  box rows={len(g)}")
        if g.empty:
            print("  NO BOXES")
            continue
        print("  newest 15 (any window):")
        for _, r in g.head(15).iterrows():
            val = r.get(col) if col else None
            print(
                f"    {str(r['game_date'])[:10]} {str(r['window']):10} "
                f"min={r['minutes']:>5} {col}={val}"
            )
        card = l5_card_window(boxes, player, prop, n=5)
        print(f"  card: {[(x['date'], x['val'], x['tag']) for x in (card or [])]}")
        print(f"  mix={window_mix(card)}")
        if card:
            vals = [x["val"] for x in card]
            print(f"  clears O{line}? {l5_clears(vals, line, 'OVER')}")
        pre = l5_preseason(boxes, player, prop)
        print(f"  pre ({len(pre)}): {[(x['date'], x['val']) for x in pre[:8]]}")
        print(f"  pre_flag={preseason_flag(boxes, player, prop, line, 'OVER')}")
        detail = card_l5_ok(boxes, player, prop, line, "OVER", slate="2026-10-22")
        print(
            "  card_l5_ok:",
            None if detail is None else (detail["card_vals"], detail["pre_flag"]),
        )
        key = (player, prop)
        if key in USER:
            u = USER[key]
            print(f"  USER card vals: {u} clears? {l5_clears(u, line, 'OVER')}")
            if card:
                our = [x["val"] for x in card]
                print(f"  MATCH user? {our == [float(x) for x in u]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
