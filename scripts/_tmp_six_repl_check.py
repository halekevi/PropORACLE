#!/usr/bin/env python3
"""Live-check LAL replacements + card dump for off-board legs."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.cushion_z import l5_clears
from utils.nba_early_l5 import l5_card_window, preseason_flag, window_mix
from utils.nba_minutes_role import load_nba_box_frame
from utils.pp_manual_slip import react_search

SLATE = "2026-10-23"
OFF_BOARD = [
    ("Kel'el Ware", 4.5),
    ("Sam Hauser", 3.5),
    ("Jamal Shead", 0.5),
]
REPL = [
    ("Ziaire Williams", 3.5),
    ("Quentin Grimes", 4.5),
]
KEEP = [
    ("Brandon Williams", 4.5),
    ("Collin Sexton", 5.5),
]


def check_one(page, boxes, player: str, want_line: float) -> dict | None:
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    try:
        react_search(page, "")
        page.wait_for_timeout(250)
        react_search(page, player)
        page.wait_for_timeout(900)
    except Exception:
        pass
    frame = P.find_prizepicks_frame(page)
    P._switch_board_filter(frame, page, "Points")
    page.wait_for_timeout(450)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
    card = l5_card_window(boxes, player, "Points", n=5)
    vals = [g["val"] for g in card] if card else []
    dates = [g["date"] for g in card] if card else []
    mix = window_mix(card) if card else "?"
    vals_s = ", ".join(f"{d}:{v:g}" for d, v in zip(dates, vals)) if card else "MISSING"

    if not hit:
        print(f"{player}  want Goblin O{want_line}")
        print(f"  live: NOT ON POINTS BOARD")
        print(f"  card[{mix}] [{vals_s}]  (vs built O{want_line})")
        if card and vals:
            clears = bool(l5_clears(vals, want_line, "OVER"))
            hits = sum(1 for v in vals if v > want_line)
            print(f"  box-only vs O{want_line} → {hits}/5 clears={clears}")
        print(f"  → OUT_OFF_BOARD")
        print()
        return None

    exact = P.cycle_card_to_pick_type(
        frame,
        hit[0].get("more_btn"),
        player=player,
        prop="Points",
        want_pick="goblin",
        want_line=want_line,
        require_line=True,
        more_index=hit[0].get("more_index"),
    )
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
    any_g = P.cycle_card_to_pick_type(
        frame,
        hit[0].get("more_btn") if hit else None,
        player=player,
        prop="Points",
        want_pick="goblin",
        want_line=None,
        require_line=False,
        more_index=hit[0].get("more_index") if hit else None,
    )
    gob_line = any_g.get("line") if any_g else None
    live_line = hit[0].get("line") if hit else None
    live_pick = str(hit[0].get("pick_type") or "").lower() if hit else ""
    moved = exact is None
    test_line = float(want_line) if not moved else (
        float(gob_line) if gob_line is not None else None
    )
    if test_line is None or not card:
        print(f"{player}  want Goblin O{want_line}")
        print(f"  live Goblin: {gob_line}  (showing {live_pick} {live_line})")
        print(f"  card[{mix}] [{vals_s}]")
        print("  → OUT")
        print()
        return None
    clears = bool(l5_clears(vals, test_line, "OVER"))
    hits = sum(1 for v in vals if v > test_line)
    pre = preseason_flag(boxes, player, "Points", test_line, "OVER", as_of=SLATE)
    status = (
        "OK_UNCHANGED" if not moved and clears else
        "OK_MOVED_CLEARS" if moved and clears else
        "OUT_MOVED_FAILS" if moved and not clears else "OUT_FAILS"
    )
    print(f"{player}  want Goblin O{want_line}")
    print(
        f"  live Goblin: {gob_line}  (board showing {live_pick} {live_line})  "
        f"exact O{want_line}? {exact is not None}"
    )
    print(f"  card[{mix}] [{vals_s}]  vs O{test_line} → {hits}/5 clears={clears}  pre={pre}")
    print(f"  → {status}")
    print()
    if clears and gob_line is not None:
        return {
            "player": player,
            "line": float(gob_line if moved else want_line),
            "moved": moved,
            "mix": mix,
            "vals": vals,
        }
    return None


def main() -> int:
    print("Loading boxes…")
    boxes = load_nba_box_frame()
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=60_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "online-metrix" not in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("ERROR: no PrizePicks tab")
            return 1
        page.bring_to_front()
        P.navigate_board_for_sport(page, "NBA", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)

        print()
        print("=== OFF-BOARD CARD DUMP (Ware / Hauser / Shead) ===")
        for player, want in OFF_BOARD:
            check_one(page, boxes, player, want)

        print("=== LAL REPLACEMENTS ===")
        survivors = []
        # keep original two as already-OK; reconfirm quickly
        for player, want in KEEP + REPL:
            s = check_one(page, boxes, player, want)
            if s:
                survivors.append(s)

        print("=" * 72)
        print(f"SLIP CANDIDATES n={len(survivors)}")
        for s in survivors:
            tag = "MOVED" if s["moved"] else "same"
            print(f"  {s['player']} Goblin O{s['line']} ({tag})  mix={s['mix']}  vals={s['vals']}")

        out = _REPO / f"outputs/{SLATE}/nba/six_points_live_survivors.txt"
        out.write_text(
            "\n".join(
                f"NBA|{s['player']}|Points|OVER|{s['line']}|goblin" for s in survivors
            ),
            encoding="utf-8",
        )
        print(f"Wrote {out}")
        return 0 if len(survivors) >= 2 else 2


if __name__ == "__main__":
    raise SystemExit(main())
