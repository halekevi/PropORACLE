#!/usr/bin/env python3
"""Build NFL 2026-09-27 PP tabs from live L5=5 Goblin yard clears. No submit / no READY claim."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pandas as pd

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, prop_ok, _norm, react_search
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-27/nfl/step8_nfl_direction_clean.xlsx"
)
OUT = _REPO / "data" / "reports" / "nfl_pp_tabs_2026-09-27.json"

LABEL = {
    "passing_yards": "Pass Yards",
    "rushing_yards": "Rush Yards",
    "receiving_yards": "Rec Yards",
    "receptions": "Receptions",
}


def d_ok_over(od: str) -> bool:
    o = str(od).lower()
    return ("weak" in o) or ("below" in o)


def load_clears() -> list[dict]:
    df = pd.read_excel(STEP8)
    d = df["Direction"].astype(str).str.upper()
    l5 = pd.to_numeric(df["L5 Over"], errors="coerce")
    l10 = pd.to_numeric(df["L10 Over"], errors="coerce")
    rows = []
    for i, r in df.iterrows():
        if str(r.get("Direction") or "").upper() != "OVER":
            continue
        if "goblin" not in str(r.get("Pick Type") or "").lower():
            continue
        if float(l5.loc[i] or 0) < 5 or float(l10.loc[i] or 0) < 8:
            continue
        prop = canon_prop("NFL", str(r.get("Prop") or ""))
        if prop not in LABEL:
            continue
        if not d_ok_over(str(r.get("Def Tier") or "")):
            continue
        # skip combo / multi-player
        if "+" in str(r.get("Player") or ""):
            continue
        label = LABEL[prop]
        need = 15 if prop == "passing_yards" else (8 if "yards" in prop else 0.5)
        # no Last 5 Avg on this sheet — use L5=5+L10 as primary; soft cover via line depth preference
        rows.append(
            {
                "player": str(r["Player"]),
                "team": str(r.get("Team") or "").upper(),
                "prop": label,
                "prop_type": label,
                "line": float(r["Line"]),
                "l5": float(l5.loc[i]),
                "l10": float(l10.loc[i]),
                "d": str(r.get("Def Tier") or ""),
                "pick_type": "goblin",
                "direction": "OVER",
                "side": "OVER",
                "sport": "NFL",
                "search": str(r["Player"]),
                "need": need,
            }
        )
    best = {}
    for r in rows:
        k = (r["player"], r["prop"])
        if k not in best or r["line"] < best[k]["line"]:
            best[k] = r
    return sorted(best.values(), key=lambda z: (-z["l10"], z["line"], z["player"]))


def yard_ok(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    w, g = _norm(want), _norm(got)
    mapping = {
        "pass": "pass",
        "rush": "rush",
        "rec": "rec",
        "reception": "rec",
    }
    for a, b in mapping.items():
        if a in w and b in g and ("yard" in w or "reception" in w):
            if "yard" in g or "reception" in g or "rec" in g:
                return True
    return False


def hidden_search(page, player: str) -> list[dict]:
    page.evaluate(
        """(q) => {
          const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
          if (!inp) return false;
          const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
          setter.call(inp, '');
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          setter.call(inp, q);
          inp.dispatchEvent(new Event('input', {bubbles:true}));
          return true;
        }""",
        player,
    )
    page.wait_for_timeout(1300)
    frame = P.find_prizepicks_frame(page)
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    return [
        c
        for c in cards
        if pn in _norm(c.get("player")) or _norm(c.get("player")) in pn
    ]


def probe(page, clears: list[dict]) -> list[dict]:
    live = []
    P.navigate_board_for_sport(page, "NFL", settle_ms=2000)
    for rec in clears:
        hits = hidden_search(page, rec["player"])
        if not hits:
            print(f"MISS {rec['player']} {rec['prop']} {rec['line']}", flush=True)
            continue
        start = next(
            (c for c in hits if yard_ok(rec["prop"], str(c.get("prop_type") or ""))),
            hits[0],
        )
        exact = any(
            yard_ok(rec["prop"], str(c.get("prop_type") or ""))
            and str(c.get("pick_type") or "").lower() == "goblin"
            and P._line_key(c.get("line")) == P._line_key(rec["line"])
            for c in hits
        )
        if not exact:
            P.cycle_card_to_pick_type(
                P.find_prizepicks_frame(page),
                start.get("more_btn"),
                player=rec["player"],
                prop=start.get("prop_type") or rec["prop"],
                want_pick="goblin",
                want_line=rec["line"],
                require_line=True,
                more_index=start.get("more_index"),
                max_clicks=14,
            )
            page.wait_for_timeout(350)
            hits = hidden_search(page, rec["player"])
            exact = any(
                yard_ok(rec["prop"], str(c.get("prop_type") or ""))
                and str(c.get("pick_type") or "").lower() == "goblin"
                and P._line_key(c.get("line")) == P._line_key(rec["line"])
                for c in hits
            )
        if exact:
            print(
                f"LIVE {rec['player']:22} {rec['prop']:12} {rec['line']} "
                f"L10={rec['l10']:.0f} D={rec['d'][:14]} ({rec['team']})",
                flush=True,
            )
            live.append(rec)
        else:
            print(f"MOVED {rec['player']} want={rec['line']}", flush=True)
    return live


def pack(live: list[dict]) -> list[dict]:
    rem = list(live)
    tickets = []

    def take(n: int, mode: str, tid: str) -> bool:
        nonlocal rem
        chunk, used_p, used_t = [], set(), set()
        for r in rem:
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
            for r in rem:
                if r["player"] in used_p:
                    continue
                chunk.append(r)
                used_p.add(r["player"])
                if len(chunk) == n:
                    break
        if len(chunk) < n or len({c["team"] for c in chunk}) < 2:
            return False
        tickets.append({"id": tid, "mode": mode, "legs": chunk})
        taken = {c["player"] for c in chunk}
        rem = [r for r in rem if r["player"] not in taken]
        return True

    i = 1
    while i <= 4 and len(rem) >= 3:
        if not take(3, "power", f"P3-{i}"):
            break
        i += 1
    j = 1
    while j <= 3 and len(rem) >= 4:
        if not take(4, "flex", f"F4-{j}"):
            break
        j += 1
    return tickets


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(f"  {leg['player']} {leg['prop']} {leg['line']} ({leg['team']})", flush=True)
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, "NFL", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    for leg in legs:
        hits = hidden_search(page, leg["player"])
        if not hits:
            print(f"NO HITS {leg['player']}", flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": 0}
        start = next(
            (c for c in hits if yard_ok(leg["prop"], str(c.get("prop_type") or ""))),
            hits[0],
        )
        exact = any(
            yard_ok(leg["prop"], str(c.get("prop_type") or ""))
            and str(c.get("pick_type") or "").lower() == "goblin"
            and P._line_key(c.get("line")) == P._line_key(leg["line"])
            for c in hits
        )
        if not exact:
            P.cycle_card_to_pick_type(
                P.find_prizepicks_frame(page),
                start.get("more_btn"),
                player=leg["player"],
                prop=start.get("prop_type") or leg["prop"],
                want_pick="goblin",
                want_line=leg["line"],
                require_line=True,
                more_index=start.get("more_index"),
            )
            page.wait_for_timeout(400)
            hidden_search(page, leg["player"])
        r = add_cycled_leg(P, P.find_prizepicks_frame(page), page, leg)
        if not r.ok:
            print(r.loud_fail(), flush=True)
            try:
                P.clear_slip(P.find_prizepicks_frame(page))
            except Exception:
                pass
            return {"id": tid, "ready": False, "fails": [leg["player"]], "n": 0}
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
    print(f"[{tid}] slip_ok={n==len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
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
    clears = load_clears()
    print(f"D-ok Goblin L5=5 L10>=8 yard clears={len(clears)}", flush=True)
    for c in clears[:20]:
        print(
            f"  {c['player']:22} {c['prop']:12} {c['line']} L10={c['l10']:.0f} "
            f"D={c['d'][:14]} ({c['team']})",
            flush=True,
        )
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
        page.bring_to_front()
        live = probe(page, clears[:40])  # probe top pool
        print(f"\nlive={len(live)}", flush=True)
        tickets = pack(live)
        print(f"packed {[t['id'] for t in tickets]}", flush=True)
        for t in tickets:
            print(
                f"  {t['id']}: "
                + " | ".join(f"{l['player']} {l['line']}" for l in t["legs"]),
                flush=True,
            )
        if not tickets:
            print("Not enough live faces")
            return 2
        pages = [page]
        while len(pages) < len(tickets):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(900)
            pages.append(pg)
        results = []
        for pg, ticket in zip(pages, tickets):
            try:
                results.append(build_one(pg, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append(
                    {"id": ticket["id"], "ready": False, "fails": [str(e)], "n": 0}
                )
        OUT.write_text(
            json.dumps(
                {"live": live, "tickets": tickets, "results": results},
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
        print(
            f"wrote {OUT}. No submit. NFL Goblin G70 still disabled — slips not READY-labeled.",
            flush=True,
        )
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
