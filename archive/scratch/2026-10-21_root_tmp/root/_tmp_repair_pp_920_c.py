#!/usr/bin/env python3
"""Repair ticket C only — no Longest Rec (click_leg contaminates to Rush/Rec Yards)."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

# Import helpers from gated rebuild
import importlib.util

spec = importlib.util.spec_from_file_location("gated", _REPO / "_tmp_rebuild_pp_920_gated.py")
gated = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gated)

CDP = "http://127.0.0.1:9222"

TICKET = {
    "id": "C",
    "mode": "flex",
    "legs": [
        {"sport": "NFL", "player": "Rashod Bateman", "search": "Bateman", "prop": "Rec Yards", "line": 35.5, "pick": "standard", "dir": "UNDER"},
        {"sport": "NFL", "player": "Roman Wilson", "search": "Roman Wilson", "prop": "Rec Yards", "line": 33.5, "pick": "standard", "dir": "UNDER"},
        {"sport": "NFL", "player": "Durham Smythe", "search": "Smythe", "prop": "Rec Yards", "line": 7.5, "pick": "standard", "dir": "UNDER"},
        {"sport": "NFL", "player": "Brock Purdy", "search": "Purdy", "prop": "Rush Yards", "line": 14.5, "pick": "standard", "dir": "OVER"},
    ],
}


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        boards = [pg for pg in ctx.pages if "prizepicks.com" in (pg.url or "")]
        # C is 3rd ticket (index 2) if A,B,C,D order
        if len(boards) < 3:
            print(f"need >=3 PP tabs, have {len(boards)}")
            return 2
        page = boards[2]
        page.bring_to_front()
        r = gated.build(page, TICKET)
        print("C", "READY" if r["ready"] else "INCOMPLETE", r)
        return 0 if r["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
