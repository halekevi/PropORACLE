"""Spot-check 10 Goblin L5=5 legs: card window vs board stat_g1..5."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.nba_early_l5 import card_l5_ok, l5_card_window, window_mix
from utils.nba_minutes_role import load_nba_box_frame


def main() -> int:
    xl = pd.ExcelFile(ROOT / "outputs/2026-10-22/step8_nba_direction_clean_2026-10-22.xlsx")
    df = pd.read_excel(xl, sheet_name="ALL", header=1)
    boxes = load_nba_box_frame()
    g = df[df["pick_type"].astype(str).str.lower() == "goblin"].copy()
    g5 = g[pd.to_numeric(g["last5_over"], errors="coerce") == 5]
    seen: set[tuple[str, str]] = set()
    sample = []
    for _, r in g5.iterrows():
        key = (str(r["player"]), str(r.get("prop_type") or r.get("prop") or ""))
        if key in seen:
            continue
        seen.add(key)
        sample.append(r)
        if len(sample) >= 10:
            break

    print("=== 10-leg spot-check: card window vs board stat_g1..5 ===")
    match_board = 0
    no_pre = 0
    card_clears = 0
    for r in sample:
        player = str(r["player"])
        prop = str(r.get("prop_type") or r.get("prop"))
        line = float(r["line"])
        board = [
            float(r[f"stat_g{i}"])
            for i in range(1, 6)
            if pd.notna(r.get(f"stat_g{i}"))
        ]
        card = l5_card_window(boxes, player, prop, n=5)
        cvals = [x["val"] for x in (card or [])]
        mix = window_mix(card)
        same = cvals == board
        if same:
            match_board += 1
        if card and "P" not in mix:
            no_pre += 1
        detail = card_l5_ok(boxes, player, prop, line, "OVER", slate="2026-10-22")
        if detail is not None:
            card_clears += 1
        print(f"{player} {prop} O{line}")
        print(f"  board_stat_g: {board}")
        print(f"  card_window:  {cvals} mix={mix} flag={(detail or {}).get('pre_flag')}")
        print(f"  match_board={'YES' if same else 'NO'}")
    print(f"card has no preseason (P): {no_pre}/10")
    print(f"card vals == board stat_g (pre-rebuild): {match_board}/10")
    print(f"card L5=5 still clears: {card_clears}/10")
    print(
        "PP-card rule match rate (no P in window + 5 games): "
        f"{no_pre}/10 ({100.0 * no_pre / 10:.0f}%)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
