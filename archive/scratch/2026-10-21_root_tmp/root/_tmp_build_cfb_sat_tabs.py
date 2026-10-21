"""Build Sat 2026-09-26 CFB L5=5 G70 slips in separate PrizePicks tabs. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
REPORT = _REPO / "data" / "reports" / "goblin70_tickets_2026-09-26_l5eq5.json"
KEEP_PREFIX = ("P3-", "F4-")


def load_tickets() -> list[dict]:
    d = json.loads(REPORT.read_text(encoding="utf-8"))
    out = []
    for t in d.get("tickets") or []:
        tid = str(t.get("id") or "")
        if not tid.startswith(KEEP_PREFIX):
            continue
        mode = "power" if str(t.get("product") or "").lower().startswith("power") else "flex"
        legs = []
        for leg in t.get("legs") or []:
            legs.append(
                {
                    "sport": "CFB",
                    "player": leg["player"],
                    "search": leg["player"],
                    "prop": leg["prop"],
                    "prop_type": leg["prop"],
                    "direction": str(leg.get("side") or "OVER").upper(),
                    "line": float(leg["line"]),
                    "pick_type": "goblin",
                    "team": leg.get("team"),
                    "l5": leg.get("l5"),
                    "l10": leg.get("l10"),
                }
            )
        out.append({"id": tid, "mode": mode, "legs": legs})
    return out


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} n={len(legs)} ========", flush=True)
    for leg in legs:
        print(
            f"  L5={leg.get('l5')} L10={leg.get('l10')} "
            f"{leg['player']} ({leg.get('team')}) {leg['prop']} {leg['line']}",
            flush=True,
        )
    page.bring_to_front()
    P.navigate_board_for_sport(page, "CFB", settle_ms=2200)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P._switch_board_filter(frame, page, P.prop_type_to_board_filter(leg["prop"]))
        page.wait_for_timeout(700)
        react_search(page, "")
        page.wait_for_timeout(80)
        react_search(page, leg["search"])
        page.wait_for_timeout(1300)
        frame = P.find_prizepicks_frame(page)
        r = add_cycled_leg(P, frame, page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(frame)
            except Exception:
                pass
            return {
                "id": tid,
                "ready": False,
                "n": 0,
                "px": None,
                "fails": [leg["player"]],
            }
        time.sleep(0.25)
    try:
        label = "Flex Play" if mode == "flex" else "Power Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=len(legs), ticket_type=mode
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else len(legs))
    ready = n == len(legs)
    print(f"[{tid}] ready={ready} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    shot = _REPO / "data" / "reports" / f"pp_{tid.lower()}_2026-09-26.png"
    try:
        page.screenshot(path=str(shot), full_page=False)
    except Exception:
        pass
    return {"id": tid, "ready": ready, "n": n, "px": px, "fails": []}


def main() -> int:
    tickets = load_tickets()
    print(f"tickets={len(tickets)}: {[t['id'] for t in tickets]}", flush=True)
    if not tickets:
        print("No P3/F4 tickets in report")
        return 2
    results: list[dict] = []
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=120_000)
        ctx = browser.contexts[0]
        boards: list = []
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                boards.append(pg)
        if not boards:
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(1200)
            boards.append(pg)
        while len(boards) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            boards.append(pg)
            print(f"opened tab {len(boards)}", flush=True)
        for page, ticket in zip(boards, tickets):
            try:
                results.append(build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "n": 0, "fails": [str(e)]}
                )
        for page, r in zip(boards, results):
            if r.get("ready"):
                page.bring_to_front()
                print(f"\nBrought {r['id']} to front", flush=True)
                break
    print("\nSUMMARY", flush=True)
    for r in results:
        print(
            f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
            f"N-correct={r.get('px')} fails={r.get('fails')}",
            flush=True,
        )
    print("All CFB L5=5 Goblin. No submit.", flush=True)
    return 0 if all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
