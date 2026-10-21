#!/usr/bin/env python3
"""Continue NFL 9/27 PP tabs from leftover live faces. No submit / no READY."""
from __future__ import annotations

import json
import math
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
PREV = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27.json"
RETRY = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27_retry.json"
OUT = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27_cont.json"


def _num(v):
    try:
        x = float(v)
        return None if math.isnan(x) else x
    except (TypeError, ValueError):
        return None


def board_prop(prop: str) -> str:
    p = str(prop or "")
    if p.lower() in ("receptions", "recs"):
        return "Recs"
    return p


def leg_from(r: dict) -> dict:
    prop = board_prop(r["prop"])
    return {
        "player": r["player"],
        "team": r["team"],
        "prop": prop,
        "prop_type": prop,
        "line": float(r["line"]),
        "pick_type": "goblin",
        "direction": "OVER",
        "side": "OVER",
        "sport": "NFL",
        "search": r["player"],
        "l5": r.get("l5"),
        "l10": r.get("l10"),
        "d": r.get("d"),
    }


def load_ok_players() -> set[str]:
    used: set[str] = set()
    for path in (PREV, RETRY):
        if not path.is_file():
            continue
        d = json.loads(path.read_text(encoding="utf-8"))
        for block in (d.get("results") or []):
            if not block.get("ready"):
                continue
            for row in block.get("legs") or []:
                used.add(row[0] if isinstance(row, (list, tuple)) else row.get("player"))
    return used


def pack(live: list[dict]) -> list[dict]:
    rem = list(live)
    tickets: list[dict] = []

    def take(n: int, mode: str, tid: str) -> bool:
        nonlocal rem
        chunk, used_p, used_t = [], set(), set()
        # Prefer real L10 then yards over Recs
        ordered = sorted(
            rem,
            key=lambda z: (
                0 if _num(z.get("l10")) and _num(z["l10"]) >= 8 else 1,
                0 if "yard" in str(z["prop"]).lower() else 1,
                -(_num(z.get("l10")) or 0),
                float(z["line"]),
            ),
        )
        for r in ordered:
            if r["player"] in used_p:
                continue
            if r["team"] in used_t and len(chunk) < n - 1:
                continue
            chunk.append(r)
            used_p.add(r["player"])
            used_t.add(r["team"])
            if len(chunk) == n:
                break
        if len(chunk) < n:
            for r in ordered:
                if r["player"] in used_p:
                    continue
                chunk.append(r)
                used_p.add(r["player"])
                if len(chunk) == n:
                    break
        if len(chunk) < n or len({c["team"] for c in chunk}) < 2:
            return False
        tickets.append({"id": tid, "mode": mode, "legs": [leg_from(c) for c in chunk]})
        taken = {c["player"] for c in chunk}
        rem = [r for r in rem if r["player"] not in taken]
        return True

    i = 1
    while i <= 3 and len(rem) >= 3:
        if not take(3, "power", f"P3-C{i}"):
            break
        i += 1
    j = 1
    while j <= 2 and len(rem) >= 4:
        if not take(4, "flex", f"F4-C{j}"):
            break
        j += 1
    return tickets


def search_player(page, player: str) -> None:
    react_search(page, player)
    page.wait_for_timeout(1400)


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for x in legs:
        print(f"  {x['player']} {x['prop']} {x['line']} ({x['team']})", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, "NFL", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for x in legs:
        search_player(page, x["player"])
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, x)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [x["player"]], "n": 0, "legs": None}
        time.sleep(0.2)
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
    print(f"[{tid}] slip_ok={n == len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:450], flush=True)
    return {
        "id": tid,
        "ready": n == len(legs),
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"], x["team"]) for x in legs],
        "note": "NFL Goblin engine off — slip built, not READY-labeled",
    }


def main() -> int:
    prev = json.loads(PREV.read_text(encoding="utf-8"))
    used = load_ok_players()
    live_all = prev.get("live") or []
    leftover = [r for r in live_all if r["player"] not in used]
    # one face per player (deepest yard already preferred in prior probe order)
    best: dict[str, dict] = {}
    for r in leftover:
        p = r["player"]
        if p not in best:
            best[p] = r
            continue
        # prefer yards + lower line (deeper goblin)
        cur = best[p]
        cur_y = "yard" in str(cur["prop"]).lower()
        new_y = "yard" in str(r["prop"]).lower()
        if new_y and not cur_y:
            best[p] = r
        elif new_y == cur_y and float(r["line"]) < float(cur["line"]):
            best[p] = r
    pool = list(best.values())
    print(f"prior_ok_players={len(used)} leftover_players={len(pool)}", flush=True)
    for r in pool:
        print(
            f"  {r['player']:22} {r['prop']:12} {r['line']} "
            f"L10={r.get('l10')} D={str(r.get('d') or '')[:12]} ({r['team']})",
            flush=True,
        )
    tickets = pack(pool)
    print(f"packed {[t['id'] for t in tickets]}", flush=True)
    for t in tickets:
        print(
            f"  {t['id']}: "
            + " | ".join(f"{l['player']} {l['prop']} {l['line']}" for l in t["legs"]),
            flush=True,
        )
    if not tickets:
        print("Not enough leftover faces", flush=True)
        return 2

    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (
                pg
                for pg in ctx.pages
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or "")
            ),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(1200)
        results = []
        for t in tickets:
            try:
                results.append(build_one(page, t))
            except Exception as e:
                print(f"{t['id']} EXC {e}", flush=True)
                results.append(
                    {"id": t["id"], "ready": False, "fails": [str(e)], "n": 0}
                )
        OUT.write_text(
            json.dumps(
                {"used_prior": sorted(used), "pool": pool, "tickets": tickets, "results": results},
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} slip_ok={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT}", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
