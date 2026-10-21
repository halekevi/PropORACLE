#!/usr/bin/env python3
"""Quick CFB live L5=5 tickets with Sam Leavitt flash Goblin Pass 169.5. No submit."""
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
from utils.cfb_keep_gates import (
    cfb_ticket_fade,
    cfb_pass_yards_goblin_skip_d,
    cfb_list_eligible,
)
from utils.pp_manual_slip import add_cycled_leg, n_correct_power, prop_ok, _norm, react_search
from utils.prop_norm import canon_prop

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board?league_id=15"
STEP8 = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.parquet")
OUT = _REPO / "data" / "reports" / "cfb_leavitt_flash_tabs_2026-09-26.json"

# Flash-sale face observed live (deeper goblin between 199.5 and demons)
LEAVITT_FLASH = {
    "player": "Sam Leavitt",
    "team": "LSU",
    "prop": "Pass Yards",
    "prop_type": "Pass Yards",
    "line": 169.5,
    "l5": 5.0,
    "l10": 9.0,
    "cover": 290.8 - 169.5,
    "pick_type": "goblin",
    "direction": "OVER",
    "side": "OVER",
    "sport": "CFB",
    "search": "Sam Leavitt",
    "flash": True,
}


def d_ok(side: str, od: str) -> bool:
    o = str(od).lower()
    if side == "OVER":
        return ("weak" in o) or ("below" in o)
    return ("elite" in o) or ("above" in o)


def yard_ok(want: str, got: str) -> bool:
    if prop_ok(want, got):
        return True
    w, g = _norm(want), _norm(got)
    for a in ("pass", "rush", "rec"):
        if a in w and "yard" in w and a in g and "yard" in g:
            return True
    return False


def load_clears() -> list[dict]:
    df = pd.read_parquet(STEP8)
    d = df["Direction"].astype(str).str.upper()
    l5 = pd.to_numeric(df["L5 Over"], errors="coerce").where(
        d == "OVER", pd.to_numeric(df["L5 Under"], errors="coerce")
    )
    l10 = pd.to_numeric(df["L10 Over"], errors="coerce").where(
        d == "OVER", pd.to_numeric(df["L10 Under"], errors="coerce")
    )
    l5avg = pd.to_numeric(df["Last 5 Avg"], errors="coerce")
    line = pd.to_numeric(df["Line"], errors="coerce")
    rows = []
    for i, r in df.iterrows():
        if "goblin" not in str(r["Pick Type"]).lower():
            continue
        if str(r["Direction"]).upper() != "OVER":
            continue
        if float(l5.loc[i] or 0) < 5 or float(l10.loc[i] or 0) < 8:
            continue
        prop = canon_prop("CFB", str(r["Prop"]))
        if prop not in {"pass_yds", "rush_yds", "rec_yds"}:
            continue
        rec = {
            "sport": "CFB",
            "Direction": "OVER",
            "side": "OVER",
            "pick_type": "Goblin",
            "Pick Type": "Goblin",
            "Prop": r["Prop"],
            "prop": r["Prop"],
        }
        if cfb_ticket_fade(rec) or not cfb_list_eligible(rec):
            continue
        skip = cfb_pass_yards_goblin_skip_d(rec)
        if not skip and not d_ok("OVER", str(r.get("Def Tier") or "")):
            continue
        avg, ln = l5avg.loc[i], line.loc[i]
        if pd.isna(avg) or pd.isna(ln):
            continue
        cover = float(avg) - float(ln)
        need = 15 if prop == "pass_yds" else 8
        if cover < need:
            continue
        label = {"pass_yds": "Pass Yards", "rush_yds": "Rush Yards", "rec_yds": "Rec Yards"}[
            prop
        ]
        player = str(r["Player"])
        # Skip stale Leavitt faces — we seed the live flash line separately
        if "leavitt" in player.lower() and label == "Pass Yards":
            continue
        rows.append(
            {
                "player": player,
                "team": str(r["Team"]).upper(),
                "prop": label,
                "prop_type": label,
                "line": float(r["Line"]),
                "l5": float(l5.loc[i]),
                "l10": float(l10.loc[i]),
                "cover": cover,
                "pick_type": "goblin",
                "direction": "OVER",
                "side": "OVER",
                "sport": "CFB",
                "search": player,
                "flash": False,
            }
        )
    best = {}
    for r in rows:
        k = (r["player"], r["prop"])
        if k not in best or r["line"] < best[k]["line"]:
            best[k] = r
    out = sorted(best.values(), key=lambda z: (-z["l10"], -z["cover"], z["player"]))
    out.insert(0, LEAVITT_FLASH)
    return out


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
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
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
            frame = P.find_prizepicks_frame(page)
            P.cycle_card_to_pick_type(
                frame,
                start.get("more_btn"),
                player=rec["player"],
                prop=start.get("prop_type") or rec["prop"],
                want_pick="goblin",
                want_line=rec["line"],
                require_line=True,
                more_index=start.get("more_index"),
                max_clicks=16,
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
            tag = "FLASH" if rec.get("flash") else "LIVE"
            print(
                f"{tag} L5=5 {rec['player']:22} {rec['prop']:12} {rec['line']} "
                f"L10={rec['l10']:.0f} cover={rec['cover']:.0f}",
                flush=True,
            )
            live.append(rec)
        else:
            print(f"MOVED {rec['player']} want={rec['line']}", flush=True)
    return live


def pack(live: list[dict]) -> list[dict]:
    # Prefer tickets that include Leavitt flash first
    rem = list(live)
    tickets = []

    def take(n: int, mode: str, prefer_leavitt: bool) -> dict | None:
        nonlocal rem
        chunk = []
        used_p = set()
        used_t = set()
        ordered = rem
        if prefer_leavitt:
            ordered = sorted(rem, key=lambda z: (0 if "leavitt" in z["player"].lower() else 1, -z["l10"]))
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
            for r in rem:
                if r["player"] in used_p:
                    continue
                chunk.append(r)
                used_p.add(r["player"])
                if len(chunk) == n:
                    break
        if len(chunk) < n or len({c["team"] for c in chunk}) < 2:
            return None
        taken = {c["player"] for c in chunk}
        rem = [r for r in rem if r["player"] not in taken]
        return {"mode": mode, "legs": chunk}

    # F4 with Leavitt if pool allows, else P3 with Leavitt, then more
    if len(rem) >= 4:
        t = take(4, "flex", True)
        if t:
            t["id"] = "F4-1"
            tickets.append(t)
    if len(rem) >= 3:
        t = take(3, "power", True)
        if t:
            t["id"] = "P3-1"
            tickets.append(t)
    while len(rem) >= 4:
        t = take(4, "flex", False)
        if not t:
            break
        t["id"] = f"F4-{len([x for x in tickets if x['id'].startswith('F4')]) + 1}"
        tickets.append(t)
    while len(rem) >= 3:
        t = take(3, "power", False)
        if not t:
            break
        t["id"] = f"P3-{len([x for x in tickets if x['id'].startswith('P3')]) + 1}"
        tickets.append(t)
    return tickets


def build_one(page, ticket: dict) -> dict:
    tid, mode, legs = ticket["id"], ticket["mode"], ticket["legs"]
    print(f"\n======== {tid} {mode} ========", flush=True)
    for leg in legs:
        print(
            f"  {'FLASH' if leg.get('flash') else 'L5=5'} {leg['player']} "
            f"{leg['prop']} {leg['line']}",
            flush=True,
        )
    page.bring_to_front()
    page.set_viewport_size({"width": 1400, "height": 900})
    P.navigate_board_for_sport(page, "CFB", settle_ms=1800)
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
                max_clicks=16,
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
    print(f"[{tid}] ready={n==len(legs)} n={n} N-correct={px}", flush=True)
    print(raw[:400], flush=True)
    return {
        "id": tid,
        "ready": n == len(legs),
        "n": n,
        "px": px,
        "fails": [],
        "legs": [(x["player"], x["prop"], x["line"]) for x in legs],
    }


def main() -> int:
    clears = load_clears()
    print(f"clears (incl Leavitt flash)={len(clears)}", flush=True)
    print(
        f"Leavitt flash Pass Goblin {LEAVITT_FLASH['line']}: "
        f"L5={LEAVITT_FLASH['l5']} L10={LEAVITT_FLASH['l10']} "
        f"cover=+{LEAVITT_FLASH['cover']:.0f} (L5avg 290.8) — GOOD for G70",
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
        live = probe(page, clears)
        print(f"\nlive={len(live)}", flush=True)
        tickets = pack(live)
        print(f"packed {[t['id'] for t in tickets]}", flush=True)
        for t in tickets:
            print(
                f"  {t['id']}: "
                + " | ".join(
                    f"{'*' if l.get('flash') else ''}{l['player']} {l['line']}"
                    for l in t["legs"]
                ),
                flush=True,
            )
        if not tickets:
            print("Not enough live L5=5 for a ticket")
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
                {
                    "leavitt_flash": LEAVITT_FLASH,
                    "live": [
                        {
                            "player": x["player"],
                            "prop": x["prop"],
                            "line": x["line"],
                            "flash": x.get("flash"),
                        }
                        for x in live
                    ],
                    "tickets": [
                        {
                            "id": t["id"],
                            "mode": t["mode"],
                            "legs": [
                                {
                                    "player": l["player"],
                                    "prop": l["prop"],
                                    "line": l["line"],
                                    "flash": l.get("flash"),
                                }
                                for l in t["legs"]
                            ],
                        }
                        for t in tickets
                    ],
                    "results": results,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("\nSUMMARY", flush=True)
        for r in results:
            print(
                f"  {r.get('id')} ready={r.get('ready')} n={r.get('n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')} legs={r.get('legs')}",
                flush=True,
            )
        print(f"wrote {OUT} No submit.", flush=True)
        return 0 if results and all(r.get("ready") for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
