#!/usr/bin/env python3
"""Add Calvert SOT O0.5 onto the open Yamal+Ionescu slip. No submit."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

CDP = "http://127.0.0.1:9222"


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        pages = [
            p
            for p in ctx.pages
            if "prizepicks.com" in (p.url or "") and "board" in (p.url or "")
        ]
        if not pages:
            print("no board", flush=True)
            return 2
        page = pages[-1]
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
        print(f"before n={slip.get('n_selected')}\n{(slip.get('raw_slip_section') or '')[:300]}", flush=True)

        switched = P.switch_sport_tab(frame, page, "Soccer", settle_ms=1400)
        if switched is None:
            P.navigate_board_for_sport(page, "Soccer", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        try:
            P._switch_board_filter(frame, page, "SOT")
        except Exception:
            pass
        react_search(page, "Dominic Calvert-Lewin")
        page.wait_for_timeout(1500)
        frame = P.find_prizepicks_frame(page)
        cards = [
            c
            for c in P.get_all_cards(frame)
            if "+" not in str(c.get("player") or "")
            and (
                _norm("calvert") in _norm(c.get("player"))
                or _norm(c.get("player")) in _norm("dominic calvert-lewin")
            )
            and (
                _norm(c.get("prop_type")) in {"sot", "shotsontarget"}
                or prop_ok("SOT", str(c.get("prop_type") or ""))
            )
        ]
        print("cards", [(c.get("player"), c.get("prop_type"), c.get("line"), c.get("pick_type"), bool(c.get("more_btn"))) for c in cards], flush=True)
        gob = [
            c
            for c in cards
            if str(c.get("pick_type") or "").lower() == "goblin"
            and P._line_key(c.get("line")) == P._line_key(0.5)
        ]
        card = gob[0] if gob else (cards[0] if cards else None)
        if card is None:
            print("no calvert card", flush=True)
            return 2
        # Force More via more_btn only
        btn = card.get("more_btn")
        if btn is None:
            print("no more_btn", flush=True)
            return 2
        try:
            btn.click(timeout=2000, no_wait_after=True)
        except Exception:
            btn.click(force=True, timeout=2500, no_wait_after=True)
        print("clicked More on Calvert", flush=True)
        page.wait_for_timeout(800)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or "")
        n = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, n if n else 3)
        print(f"after n={n} N-correct={px}", flush=True)
        print(raw[:500], flush=True)
        return 0 if n >= 3 else 2


if __name__ == "__main__":
    raise SystemExit(main())
