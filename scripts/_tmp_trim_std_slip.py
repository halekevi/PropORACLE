"""Drop Skattebo + Edwards from current Standard slip; keep the other four."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

KEEP = [
    {"sport": "NFL", "player": "Andre Szmyt", "prop_type": "FG Made", "direction": "OVER", "line": 1.5, "pick_type": "standard"},
    {"sport": "NFL", "player": "Malik Washington", "prop_type": "Receiving Yards", "direction": "OVER", "line": 39.5, "pick_type": "standard"},
    {"sport": "NFL", "player": "Eddy Pineiro", "prop_type": "Kicking Points", "direction": "UNDER", "line": 8.0, "pick_type": "standard"},
    {"sport": "NFL", "player": "Tre Tucker", "prop_type": "Receiving Yards", "direction": "OVER", "line": 39.5, "pick_type": "standard"},
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
        if page is None:
            print("ERROR: no PP tab")
            return 1
        page.bring_to_front()
        P.navigate_board_for_sport(page, "NFL", settle_ms=1500)
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        # Rebuild keepers only (no reliable per-leg remove API).
        P.clear_slip(frame)
        P.set_ticket_type(frame, "power")

        for leg in KEEP:
            print(f"\n=== {leg['player']} {leg['prop_type']} {leg['direction'][0]}{leg['line']} ===")
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            react_search(page, "")
            page.wait_for_timeout(200)
            react_search(page, leg["player"])
            page.wait_for_timeout(1100)
            frame = P.find_prizepicks_frame(page)
            chip = P.prop_type_to_board_filter(leg["prop_type"])
            try:
                P._switch_board_filter(frame, page, chip)
            except Exception as e:
                print(f"[FILTER] warn {chip}: {e}")
            page.wait_for_timeout(400)
            frame = P.find_prizepicks_frame(page)
            result = add_cycled_leg(P, frame, page, leg)
            if not result.ok and result.reason == "no_card":
                # Search alone often surfaces the face without the chip.
                react_search(page, leg["player"])
                page.wait_for_timeout(1100)
                frame = P.find_prizepicks_frame(page)
                result = add_cycled_leg(P, frame, page, leg)
            if not result.ok:
                print(result.loud_fail())
                P.clear_slip(P.find_prizepicks_frame(page))
                return 2
            time.sleep(0.2)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=4, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        selected = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, selected if selected else 4)
        print(f"\n[FINAL] n={selected} 4-correct={px}")
        print(raw[:1200])
        if selected != 4:
            print(f"FAIL: expected 4 legs, got {selected}")
            return 2
        for name in ("Skattebo", "Edwards"):
            if name.lower() in raw.lower():
                print(f"FAIL: {name} still on slip")
                return 2
        print("READY — Skattebo + Edwards removed. Not submitted.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
