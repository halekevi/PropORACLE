#!/usr/bin/env python3
"""Probe live PP for G70 goblin pool faces; pack+build Power 3 tabs. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, react_search, _norm, prop_ok

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
G70 = _REPO / "data" / "reports" / "goblin70_tickets_2026-09-27.json"
OUT = _REPO / "data" / "reports" / "pp_g70_livepack_2026-09-27.json"


def pool_rows() -> list[dict]:
    d = json.loads(G70.read_text(encoding="utf-8"))
    rows = []
    for r in d.get("goblin_pool") or d.get("pool") or []:
        if str(r.get("pick_type") or "").lower() not in {"goblin", ""}:
            # pool may already be goblin-only
            pass
        side = str(r.get("side") or r.get("direction") or "OVER").upper()
        if side != "OVER":
            continue
        pick = str(r.get("pick_type") or "Goblin")
        if "goblin" not in pick.lower() and pick:
            continue
        player = str(r.get("player") or "").strip()
        if not player or "+" in player:
            continue
        prop = str(r.get("prop") or r.get("prop_type") or "").strip()
        try:
            line = float(r.get("line"))
        except (TypeError, ValueError):
            continue
        sport = str(r.get("sport") or "").strip().upper()
        if sport not in {"MLB", "SOCCER", "TENNIS", "NFL", "CFB"}:
            continue
        rows.append(
            {
                "sport": sport,
                "player": player,
                "team": str(r.get("team") or "").upper(),
                "prop_type": prop,
                "prop": prop,
                "line": line,
                "pick_type": "goblin",
                "direction": "OVER",
                "side": "OVER",
                "search": player,
                "p": float(r.get("p") or r.get("est_p") or 0),
            }
        )
    # unique player+prop keep deepest (lowest) line
    best: dict[tuple, dict] = {}
    for r in rows:
        k = (_norm(r["player"]), _norm(r["prop"]))
        if k not in best or r["line"] < best[k]["line"]:
            best[k] = r
    return sorted(best.values(), key=lambda z: (-z["p"], z["line"], z["player"]))


def probe(page, rows: list[dict], limit: int = 40) -> list[dict]:
    live = []
    by_sport: dict[str, list[dict]] = {}
    for r in rows[:limit]:
        by_sport.setdefault(r["sport"], []).append(r)
    for sport, chunk in by_sport.items():
        P.navigate_board_for_sport(page, sport, settle_ms=1600)
        for rec in chunk:
            react_search(page, rec["player"])
            page.wait_for_timeout(1100)
            frame = P.find_prizepicks_frame(page)
            cards = P.get_all_cards(frame)
            pn = _norm(rec["player"])
            hits = [
                c
                for c in cards
                if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
            ]
            if not hits:
                print(f"MISS {rec['sport']} {rec['player']} {rec['prop']} {rec['line']}", flush=True)
                continue
            start = next(
                (c for c in hits if prop_ok(rec["prop"], str(c.get("prop_type") or ""))),
                hits[0],
            )
            exact = any(
                prop_ok(rec["prop"], str(c.get("prop_type") or ""))
                and str(c.get("pick_type") or "").lower() == "goblin"
                and P._line_key(c.get("line")) == P._line_key(rec["line"])
                for c in hits
            )
            if not exact:
                P.cycle_card_to_pick_type(
                    frame,
                    start.get("more_btn"),
                    player=rec["player"],
                    prop=start.get("prop_type") or rec["prop"],
                    want_pick="goblin",
                    want_line=rec["line"],
                    require_line=True,
                    more_index=start.get("more_index"),
                    max_clicks=12,
                )
                page.wait_for_timeout(300)
                react_search(page, rec["player"])
                page.wait_for_timeout(900)
                hits = P.get_all_cards(P.find_prizepicks_frame(page))
                hits = [
                    c
                    for c in hits
                    if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
                ]
                exact = any(
                    prop_ok(rec["prop"], str(c.get("prop_type") or ""))
                    and str(c.get("pick_type") or "").lower() == "goblin"
                    and P._line_key(c.get("line")) == P._line_key(rec["line"])
                    for c in hits
                )
            if exact:
                print(
                    f"LIVE {rec['sport']:6} {rec['player']:22} {rec['prop']:22} {rec['line']}",
                    flush=True,
                )
                live.append(rec)
            else:
                # accept deepest live goblin for same prop if still goblin
                gob = [
                    c
                    for c in hits
                    if prop_ok(rec["prop"], str(c.get("prop_type") or ""))
                    and str(c.get("pick_type") or "").lower() == "goblin"
                ]
                if gob:
                    # prefer lowest line
                    gob.sort(key=lambda c: float(c.get("line") or 99))
                    c0 = gob[0]
                    live_line = float(c0.get("line"))
                    print(
                        f"ALT  {rec['sport']:6} {rec['player']:22} {rec['prop']:22} "
                        f"{rec['line']} -> {live_line}",
                        flush=True,
                    )
                    live.append({**rec, "line": live_line})
                else:
                    print(
                        f"MOVED {rec['sport']} {rec['player']} want={rec['line']}",
                        flush=True,
                    )
    return live


def pack(live: list[dict], n_tickets: int = 5) -> list[dict]:
    rem = list(live)
    tickets = []
    for i in range(1, n_tickets + 1):
        chunk, used_p, used_t, sports = [], set(), set(), set()
        for r in rem:
            if r["player"] in used_p:
                continue
            if r["team"] and r["team"] in used_t and len(chunk) < 2:
                continue
            # prefer mix sports when possible
            chunk.append(r)
            used_p.add(r["player"])
            if r["team"]:
                used_t.add(r["team"])
            sports.add(r["sport"])
            if len(chunk) == 3:
                break
        if len(chunk) < 3:
            break
        tickets.append({"id": f"P3-L{i}", "mode": "power", "legs": chunk})
        taken = {c["player"] for c in chunk}
        rem = [r for r in rem if r["player"] not in taken]
    return tickets


def build_one(page, ticket: dict) -> dict:
    tid, legs = ticket["id"], ticket["legs"]
    print(f"\n======== {tid} ========", flush=True)
    for x in legs:
        print(f"  {x['sport']} {x['player']} {x['prop']} {x['line']}", flush=True)
    page.bring_to_front()
    first = legs[0]
    P.navigate_board_for_sport(page, first["sport"], settle_ms=1600)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, "power")
    for i, leg in enumerate(legs):
        frame = P.find_prizepicks_frame(page)
        if i == 0:
            landed = frame
        else:
            landed = P.switch_sport_tab(frame, page, leg["sport"], settle_ms=1400)
            if landed is None:
                P.navigate_board_for_sport(page, leg["sport"], settle_ms=1600)
                landed = P.find_prizepicks_frame(page)
        frame = landed
        P.dismiss_modal(frame, page)
        chip = P.prop_type_to_board_filter(leg["prop_type"])
        P._switch_board_filter(frame, page, chip)
        page.wait_for_timeout(600)
        react_search(page, "")
        page.wait_for_timeout(100)
        react_search(page, leg["player"])
        page.wait_for_timeout(1200)
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ok": False, "fails": [leg["player"]], "n": 0}
        time.sleep(0.2)
    try:
        P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
            timeout=2000
        )
    except Exception:
        pass
    slip = (
        P.read_slip(
            P.find_prizepicks_frame(page), n_legs=3, ticket_type="power"
        )
        or {}
    )
    raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
    n = int(slip.get("n_selected") or 0)
    px = n_correct_power(raw, n if n else 3)
    print(f"[{tid}] slip_ok={n==3} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {
        "id": tid,
        "ok": n == 3,
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["sport"], x["player"], x["prop"], x["line"]) for x in legs],
    }


def main() -> int:
    rows = pool_rows()
    print(f"goblin pool candidates={len(rows)}", flush=True)
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
            page.wait_for_timeout(1000)
        live = probe(page, rows, limit=45)
        print(f"\nlive={len(live)}", flush=True)
        tickets = pack(live, n_tickets=5)
        print(f"packed {[t['id'] for t in tickets]}", flush=True)
        for t in tickets:
            print(
                "  "
                + t["id"]
                + ": "
                + " | ".join(f"{l['player']} {l['line']}" for l in t["legs"]),
                flush=True,
            )
        if not tickets:
            OUT.write_text(json.dumps({"live": live, "tickets": []}, indent=2, default=str), encoding="utf-8")
            return 2
        pages = [page]
        while len(pages) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(700)
            pages.append(pg)
        results = []
        for pg, t in zip(pages, tickets):
            try:
                results.append(build_one(pg, t))
            except Exception as e:
                print(f"{t['id']} EXC {e}", flush=True)
                results.append({"id": t["id"], "ok": False, "fails": [str(e)], "n": 0})
        OUT.write_text(
            json.dumps({"live": live, "tickets": tickets, "results": results}, indent=2, default=str),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        ok = 0
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
            if r.get("ok"):
                ok += 1
        print(f"wrote {OUT}. Built {ok}/{len(results)}. No submit.", flush=True)
        return 0 if ok == len(results) and results else 2


if __name__ == "__main__":
    raise SystemExit(main())
