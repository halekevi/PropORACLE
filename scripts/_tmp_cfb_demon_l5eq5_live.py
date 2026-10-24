#!/usr/bin/env python3
"""Live-check CFB Demon L5=5 faces for Saturday."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.cushion_z import l5_clears
from utils.pp_manual_slip import react_search

# From l5eq5 dump — Demon OVER only, L5=5
CANDS = [
    ("TJ Thomas Jr.", "Receptions", 1.5, [4, 2, 2, 3, 4]),
    ("Conner Weigman", "Pass Yards", 237.5, [343, 242, 239, 239, 283]),
]


def main() -> int:
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
        if not page:
            print("ERROR: no PP tab")
            return 1
        page.bring_to_front()
        P.navigate_board_for_sport(page, "CFB", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        # leave current slip alone until we decide

        survivors = []
        print("CFB Demon L5=5 live check")
        print("=" * 64)
        for player, prop, want, vals in CANDS:
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            try:
                react_search(page, "")
                page.wait_for_timeout(200)
                react_search(page, player)
                page.wait_for_timeout(900)
            except Exception:
                pass
            frame = P.find_prizepicks_frame(page)
            P._switch_board_filter(frame, page, prop)
            page.wait_for_timeout(500)
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            if not hit:
                print(f"{player} Demon O{want} {prop} → NOT ON BOARD")
                print()
                continue
            exact = P.cycle_card_to_pick_type(
                frame,
                hit[0].get("more_btn"),
                player=player,
                prop=prop,
                want_pick="demon",
                want_line=want,
                require_line=True,
                more_index=hit[0].get("more_index"),
            )
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            hit = [c for c in cards if player.lower() in str(c.get("player") or "").lower()]
            any_d = P.cycle_card_to_pick_type(
                frame,
                hit[0].get("more_btn") if hit else None,
                player=player,
                prop=prop,
                want_pick="demon",
                want_line=None,
                require_line=False,
                more_index=hit[0].get("more_index") if hit else None,
            )
            live = float(any_d["line"]) if any_d and any_d.get("line") is not None else None
            test = want if exact else live
            print(f"{player}  Demon O{want}  {prop}")
            print(f"  live Demon: {live}  exact? {exact is not None}")
            print(f"  L5={vals}")
            if test is None:
                print("  → OUT (no Demon face)")
                print()
                continue
            clears = bool(l5_clears(vals, float(test), "OVER"))
            hits = sum(1 for v in vals if v > float(test))
            status = (
                "OK" if clears else "OUT_FAILS"
            )
            print(f"  vs O{test} → {hits}/5 clears={clears}  → {status}")
            print()
            if clears and live is not None:
                survivors.append((player, prop, float(live)))

        print("=" * 64)
        print(f"DEMON SURVIVORS n={len(survivors)}")
        for s in survivors:
            print(f"  {s[0]} {s[1]} Demon O{s[2]}")
        out = _REPO / "outputs/2026-10-24/cfb/demon_live_survivors.txt"
        out.write_text(
            "\n".join(f"CFB|{p}|{prop}|OVER|{ln}|demon" for p, prop, ln in survivors),
            encoding="utf-8",
        )
        print(f"Wrote {out}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
