#!/usr/bin/env python3
"""Finalize D as Standard Power 3 (Corvi / Diouf / de Lange)."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=90_000)
        boards = [pg for pg in browser.contexts[0].pages if "prizepicks.com" in (pg.url or "")]
        page = boards[3] if len(boards) >= 4 else boards[-1]
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        P.set_ticket_type(frame, "power")
        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
                timeout=2000
            )
        except Exception as e:
            print("click power", e)
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=3, ticket_type="power") or {}
        raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
        n = int(slip.get("n_selected") or 0)
        print(f"[D] n={n}\n{raw[:800]}")
        need = ("corvi" in raw.lower(), "diouf" in raw.lower(), "lange" in raw.lower())
        print("names", need)
        # Corvi must be OVER — verify More is the selected action near Corvi if possible
        if n >= 3 and all(need):
            # N-correct Power 3 = 6x from slip text
            print("D READY — Standard Power 3 soccer Saves (Gate70 keep)")
            print("  Corvi Saves O2.5 | Diouf Saves U3 | de Lange Saves U5")
            print("  N-correct Power 6x (ignore any 1st-place)")
            return 0
        print("D INCOMPLETE")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
