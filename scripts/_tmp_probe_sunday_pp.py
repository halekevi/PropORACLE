#!/usr/bin/env python3
"""Probe PrizePicks for sports that might have Sunday 10-25 boards."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=20_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "online-metrix" not in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if not page:
            print("NO_PP_TAB")
            return 1
        page.bring_to_front()
        for sport in ["NFL", "NBA", "NHL", "SOCCER", "TENNIS", "MLB", "CFB"]:
            try:
                P.navigate_board_for_sport(page, sport, settle_ms=1400)
                frame = P.find_prizepicks_frame(page)
                P.dismiss_modal(frame, page)
                cards = P.get_all_cards(frame)
                print(f"{sport}: cards={len(cards)}")
                for c in cards[:4]:
                    print(
                        f"  {c.get('player')} | {c.get('line')} | "
                        f"{c.get('prop') or c.get('stat_type')} | {c.get('pick_type')}"
                    )
            except Exception as e:
                print(f"{sport}: ERR {e}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
