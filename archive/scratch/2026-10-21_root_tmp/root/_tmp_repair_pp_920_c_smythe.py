#!/usr/bin/env python3
"""Add Durham Smythe Rec U8.5 to ticket C (Bateman/Wilson/Purdy already on)."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import importlib.util

spec = importlib.util.spec_from_file_location("gated", _REPO / "_tmp_rebuild_pp_920_gated.py")
gated = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gated)
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
LEG = {
    "sport": "NFL",
    "player": "Durham Smythe",
    "search": "Smythe",
    "prop": "Rec Yards",
    "line": 8.5,
    "pick": "standard",
    "dir": "UNDER",
}


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "")]
        page = boards[2]
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        P.set_ticket_type(frame, "flex")
        ok = gated.add_leg(page, LEG)
        print("add", ok)
        try:
            P.find_prizepicks_frame(page).get_by_text("Flex Play", exact=True).first.click(timeout=1500)
        except Exception:
            pass
        raw = gated.slip_raw(page)
        n = int((P.read_slip(P.find_prizepicks_frame(page), n_legs=4, ticket_type="flex") or {}).get("n_selected") or 0)
        print(f"C FINAL {n}/4\n{raw[:900]}")
        ready = n >= 4 and "Smythe" in raw and "8.5" in raw and "Bateman" in raw
        print("READY" if ready else "INCOMPLETE")
        return 0 if ready else 2


if __name__ == "__main__":
    raise SystemExit(main())
