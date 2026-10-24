"""Add Skattebo Rush Att Demon + Tucker Recs Demon to current slip. No clear/submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

ADD = [
    {
        "sport": "NFL",
        "player": "Cam Skattebo",
        "prop_type": "Rush Attempts",
        "direction": "OVER",
        "line": 9.5,
        "pick_type": "demon",
    },
    {
        "sport": "NFL",
        "player": "Tre Tucker",
        "prop_type": "Receptions",
        "direction": "OVER",
        "line": 3.5,
        "pick_type": "demon",
    },
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
        frame = P.find_prizepicks_frame(page)
        before = P.read_slip(frame, n_legs=8, ticket_type="power") or {}
        n0 = int(before.get("n_selected") or 0)
        print(f"[SLIP] starting n={n0}")
        if n0 < 1:
            print("ERROR: slip empty — expected current 4-leg ticket")
            return 1

        # Stay on NFL; do not clear.
        switched = P.switch_sport_tab(frame, page, "NFL", settle_ms=1000)
        if switched is not None:
            frame = switched

        for leg in ADD:
            print(
                f"\n=== ADD {leg['player']} Demon "
                f"{leg['prop_type']} O{leg['line']} ==="
            )
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
                react_search(page, leg["player"])
                page.wait_for_timeout(1100)
                frame = P.find_prizepicks_frame(page)
                result = add_cycled_leg(P, frame, page, leg)
            if not result.ok:
                print(result.loud_fail())
                return 2
            time.sleep(0.25)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
                timeout=2000
            )
        except Exception:
            pass
        slip = P.read_slip(P.find_prizepicks_frame(page), n_legs=6, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        selected = int(slip.get("n_selected") or 0)
        px = n_correct_power(raw, selected if selected else 6)
        print(f"\n[FINAL] n={selected} (was {n0}) 6-correct={px}")
        print(raw[:1400])
        if selected != n0 + 2:
            print(f"FAIL: expected {n0 + 2} legs, got {selected}")
            return 2
        if "9.5" not in raw or "Skattebo" not in raw:
            print("FAIL: Skattebo Demon rush atts missing")
            return 2
        if "3.5" not in raw or "Tucker" not in raw:
            print("FAIL: Tucker Demon recs missing")
            return 2
        print("READY — Demons added. Not submitted.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
