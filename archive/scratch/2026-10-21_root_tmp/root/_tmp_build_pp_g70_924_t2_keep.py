#!/usr/bin/env python3
"""9/24 T2: frame-clear, then Wilson RA / Mahle PO / Mertens TG. No submit."""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P

CDP = "http://127.0.0.1:9222"

PROP_ALIASES = {
    "pitchingouts": ("pitchingouts", "po"),
    "po": ("pitchingouts", "po"),
    "totalgames": ("totalgames",),
    "rebsasts": ("rebsasts", "rebasts"),
    "rebasts": ("rebsasts", "rebasts"),
}


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
    }""",
        q,
    )


def _norm(s: str) -> str:
    return "".join(ch for ch in str(s or "").casefold() if ch.isalnum())


def prop_ok(want: str, got: str) -> bool:
    w = _norm(want)
    g = _norm(got)
    aliases = PROP_ALIASES.get(w, (w,))
    return g in aliases or w in g or g in w


def find_card(frame, player: str, prop: str) -> dict | None:
    cards = P.get_all_cards(frame)
    pn = _norm(player)
    hits = []
    for c in cards:
        cn = _norm(c.get("player"))
        if pn not in cn and cn not in pn:
            continue
        if "+" in str(c.get("player") or ""):
            continue
        if prop_ok(prop, str(c.get("prop_type") or "")):
            hits.append(c)
    return hits[0] if hits else None


def frame_clear(frame) -> None:
    frame.locator("body").evaluate(
        """() => {
      const btns = Array.from(document.querySelectorAll('button'));
      const b = btns.find(x => (x.innerText || '').trim() === 'Clear');
      if (b) b.click();
    }"""
    )
    time.sleep(0.8)
    P.clear_slip(frame)


def js_sport(frame, sport: str) -> bool:
    return bool(
        frame.locator("body").evaluate(
            """(sport) => {
          const want = String(sport || '').trim().toUpperCase();
          const els = Array.from(document.querySelectorAll('button, [role="tab"], a, div, span'));
          const el = els.find(e => (e.innerText || '').trim().split('\\n')[0].trim().toUpperCase() === want);
          if (!el) return false;
          el.click();
          return true;
        }""",
            sport,
        )
    )


def add_cycled(frame, page, player: str, prop: str, line: float) -> bool:
    card = find_card(frame, player, prop)
    if card is None:
        print("NO CARD", player, prop)
        cards = P.get_all_cards(frame)
        for c in cards[:12]:
            print(f"  {c.get('player')} | {c.get('line')} {c.get('prop_type')} | {c.get('pick_type')}")
        return False
    print(f"FOUND {card.get('player')} {card.get('line')} {card.get('prop_type')} {card.get('pick_type')}")
    need = P._line_key(card.get("line")) != P._line_key(line) or str(
        card.get("pick_type") or ""
    ).lower() != "goblin"
    if need:
        cycled = P.cycle_card_to_pick_type(
            frame,
            card.get("more_btn"),
            player=player,
            prop=card.get("prop_type") or prop,
            want_pick="goblin",
            want_line=line,
            require_line=True,
            more_index=card.get("more_index"),
        )
        if not cycled:
            print("CYCLE FAIL", player)
            return False
        page.wait_for_timeout(400)
        frame = P.find_prizepicks_frame(page)
        card = find_card(frame, player, prop)
        if card is None:
            print("CYCLE LOST", player)
            return False
        print(f"CYCLED {card.get('line')} {card.get('pick_type')}")
    btn = card.get("more_btn")
    if btn is None:
        print("NO MORE", player)
        return False
    try:
        btn.click(timeout=1500, no_wait_after=True)
    except Exception:
        btn.click(force=True, timeout=2500, no_wait_after=True)
    page.wait_for_timeout(500)
    print("CLICK", player)
    return True


def read_raw(frame, page) -> str:
    slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
    return str(slip.get("raw_slip_section") or slip.get("raw_text") or "")


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        page = None
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("ERROR: no board")
            return 1
        page.bring_to_front()
        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        frame_clear(frame)
        _, frame = P.verify_slip_empty(frame, page)
        raw0 = read_raw(frame, page)
        print("AFTER CLEAR n-snippet", raw0[:180].replace("\n", " | "))
        P.set_ticket_type(frame, "power")

        # 1 Wilson
        print("\n=== WNBA Wilson RA 9.5 ===")
        js_sport(frame, "WNBA")
        page.wait_for_timeout(1200)
        frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, "Rebs+Asts")
        page.wait_for_timeout(800)
        react_search(page, "Wilson")
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        add_cycled(frame, page, "A'ja Wilson", "Rebs+Asts", 9.5)

        # 2 Mahle
        print("\n=== MLB Mahle PO 14.5 ===")
        frame = P.find_prizepicks_frame(page)
        js_sport(frame, "MLB")
        page.wait_for_timeout(1200)
        frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, "Pitching Outs")
        page.wait_for_timeout(800)
        react_search(page, "Tyler Mahle")
        page.wait_for_timeout(1600)
        frame = P.find_prizepicks_frame(page)
        add_cycled(frame, page, "Tyler Mahle", "PO", 14.5)

        # 3 Mertens via JS tab (no goto)
        print("\n=== TENNIS Mertens TG 19.5 ===")
        frame = P.find_prizepicks_frame(page)
        ok_tab = js_sport(frame, "TENNIS")
        print("tennis tab js", ok_tab)
        page.wait_for_timeout(1500)
        frame = P.find_prizepicks_frame(page)
        P._switch_board_filter(frame, page, "Total Games")
        page.wait_for_timeout(800)
        react_search(page, "Mertens")
        page.wait_for_timeout(1400)
        frame = P.find_prizepicks_frame(page)
        add_cycled(frame, page, "Elise Mertens", "Total Games", 19.5)

        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=2000)
        except Exception:
            pass
        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
        raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")
        n = int(slip.get("n_selected") or 0)
        m = re.search(r"Power Play.*?3 correct pays\s*(\d+(?:\.\d+)?)\s*X", raw, flags=re.I | re.S)
        px = float(m.group(1)) if m else None
        shot = _REPO / "data" / "reports" / "pp_g70_924_t2.png"
        page.screenshot(path=str(shot), full_page=False)
        print("\n[FINAL] n", n, "3-correct", px)
        print(raw[:1400])
        if n != 3:
            return 2
        if "Wilson" not in raw or "Mahle" not in raw or "Mertens" not in raw:
            print("WRONG LEGS")
            return 2
        if px is not None and px + 1e-9 < 1.4:
            print("UNDER 1.4x")
            return 3
        print("READY. I did not submit.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
