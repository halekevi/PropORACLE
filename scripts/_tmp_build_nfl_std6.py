"""Build Sunday NFL + Edwards CFB Standard 6 on PrizePicks. Does not submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

# Edwards live line moved 10.5 -> 11.5; L5 17/24/30/19/18 still clears.
LEGS = [
    {"sport": "NFL", "player": "Andre Szmyt", "prop_type": "FG Made", "direction": "OVER", "line": 1.5, "pick_type": "standard"},
    {"sport": "NFL", "player": "Malik Washington", "prop_type": "Receiving Yards", "direction": "OVER", "line": 39.5, "pick_type": "standard"},
    {"sport": "NFL", "player": "Eddy Pineiro", "prop_type": "Kicking Points", "direction": "UNDER", "line": 8.0, "pick_type": "standard"},
    {"sport": "NFL", "player": "Cam Skattebo", "prop_type": "Rush Yards", "direction": "OVER", "line": 28.5, "pick_type": "standard"},
    {"sport": "NFL", "player": "Tre Tucker", "prop_type": "Receiving Yards", "direction": "OVER", "line": 39.5, "pick_type": "standard"},
    {"sport": "CFB", "player": "Dylan Edwards", "prop_type": "Longest Reception", "direction": "OVER", "line": 11.5, "pick_type": "standard"},
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
        P.clear_slip(frame)
        P.set_ticket_type(frame, "power")

        current = ""
        added: list[str] = []
        for leg in LEGS:
            sport = leg["sport"]
            print(f"\n=== {sport} {leg['player']} {leg['prop_type']} {leg['direction'][0]}{leg['line']} ===")
            frame = P.find_prizepicks_frame(page)
            P.dismiss_modal(frame, page)
            if sport != current:
                if current == "":
                    P.navigate_board_for_sport(page, sport, settle_ms=1200)
                else:
                    switched = P.switch_sport_tab(frame, page, sport, settle_ms=1200)
                    if switched is None:
                        print(f"DROPPED: sport tab miss {sport}")
                        P.clear_slip(P.find_prizepicks_frame(page))
                        return 2
                frame = P.find_prizepicks_frame(page)
                current = sport
            chip = P.prop_type_to_board_filter(leg["prop_type"])
            try:
                P._switch_board_filter(frame, page, chip)
            except Exception as e:
                print(f"[FILTER] warn {chip}: {e}")
            page.wait_for_timeout(500)
            react_search(page, "")
            page.wait_for_timeout(250)
            react_search(page, leg["player"])
            page.wait_for_timeout(1100)
            frame = P.find_prizepicks_frame(page)
            result = add_cycled_leg(P, frame, page, leg)
            if not result.ok:
                print(result.loud_fail())
                P.clear_slip(P.find_prizepicks_frame(page))
                return 2
            added.append(leg["player"])
            time.sleep(0.25)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=6, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        selected = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, selected if selected else 6)
        print(f"\n[FINAL] n={selected} added={added} 6-correct={px}")
        print(raw[:1400])
        if selected != 6:
            print(f"FAIL: slip count {selected} != 6")
            P.clear_slip(P.find_prizepicks_frame(page))
            return 2
        print("READY on the board. I did not submit.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
