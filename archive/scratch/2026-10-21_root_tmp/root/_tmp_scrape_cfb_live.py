"""Scrape live CFB named-player yard faces (Goblin + Standard)."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"


def main() -> int:
    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        ctx = b.contexts[0]
        page = None
        for pg in ctx.pages:
            u = pg.url or ""
            if "prizepicks.com" in u and "board" in u:
                page = pg
                break
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(2000)
        page.bring_to_front()
        ok = P.navigate_board_for_sport(page, "CFB", settle_ms=2800)
        print("nav", ok, page.url, flush=True)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        out = []
        for filt in ["Popular", "Pass Yards", "Rush Yards", "Rec Yards"]:
            P._switch_board_filter(frame, page, filt)
            page.wait_for_timeout(1000)
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            for c in cards:
                pl = str(c.get("player") or "")
                if not pl or " - " in pl:
                    continue
                pt = str(c.get("prop_type") or "")
                pick = str(c.get("pick_type") or "").lower()
                if pick not in ("goblin", "standard"):
                    continue
                low = pt.lower()
                if not any(
                    x in low for x in ("pass yard", "rush yard", "rec yard", "receiving")
                ):
                    continue
                if "longest" in low:
                    continue
                out.append((pl, pick, pt, c.get("line"), filt))
        seen = set()
        uniq = []
        for r in out:
            k = (r[0], r[1], r[2], str(r[3]))
            if k in seen:
                continue
            seen.add(k)
            uniq.append(r)
        g = [x for x in uniq if x[1] == "goblin"]
        s = [x for x in uniq if x[1] == "standard"]
        print("named yard goblin", len(g), "standard", len(s), flush=True)
        print("--- GOBLIN ---", flush=True)
        for x in g:
            print(f"  G {x[0]} | {x[3]} {x[2]}", flush=True)
        print("--- STANDARD ---", flush=True)
        for x in s:
            print(f"  S {x[0]} | {x[3]} {x[2]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
