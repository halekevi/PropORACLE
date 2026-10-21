#!/usr/bin/env python3
"""Power-3 via DOM text match: Calvert SOT O0.5 + Ionescu PRA U25 + Ionescu/Courtney Ast U9.5.

Yamal Goblin 1.5 left the board (only Standard SOT 2.0). No submit.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import n_correct_power, react_search

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_s3_calvert_ionescu_combo_2026-09-29.json")


CLICK_JS = """
([playerSub, propSub, lineSub, which]) => {
  const norm = (s) => (s || '').toLowerCase().replace(/[^a-z0-9.+]/g, '');
  const wantP = norm(playerSub);
  const wantProp = norm(propSub);
  const wantLine = String(lineSub);
  const btnWant = which === 'less' ? 'less' : 'more';
  const wantCombo = wantP.includes('+');
  let best = null;
  for (const el of document.querySelectorAll('div')) {
    const t = el.innerText || '';
    if (t.length < 20 || t.length > 900) continue;
    const nt = norm(t);
    if (!nt.includes(wantP)) continue;
    if (!wantCombo && t.includes('+')) continue;
    if (wantProp && !nt.includes(wantProp)) continue;
    if (wantLine && !t.includes(wantLine) && !t.includes(wantLine.replace(/\\.0$/, ''))) continue;
    const btns = Array.from(el.querySelectorAll('button')).map(b => (b.innerText || '').trim().toLowerCase());
    if (!btns.some(b => b === 'more' || b === 'less' || b.includes('more') || b.includes('less'))) continue;
    // Prefer smallest container that still has More/Less
    if (!best || t.length < best.t.length) best = { el, t, btns };
  }
  if (!best) return { ok: false, reason: 'no_container' };
  const buttons = Array.from(best.el.querySelectorAll('button'));
  for (const b of buttons) {
    const lab = (b.innerText || '').trim().toLowerCase();
    if (lab === btnWant || lab === ('↑ ' + btnWant) || lab.includes(btnWant)) {
      b.click();
      return { ok: true, how: 'btn', text: best.t.slice(0, 140) };
    }
  }
  return { ok: false, reason: 'no_btn', text: best.t.slice(0, 180), btns: best.btns.slice(0, 10) };
}
"""


def dom_click(page, player: str, prop: str, line, which: str) -> bool:
    frame = P.find_prizepicks_frame(page)
    # try main frame and child
    for fr in [frame, page.main_frame, *page.frames]:
        try:
            res = fr.evaluate(CLICK_JS, [player, prop, str(line), which])
        except Exception:
            continue
        print(f"dom_click {player} {prop} {line} {which} -> {res}", flush=True)
        if isinstance(res, dict) and res.get("ok"):
            page.wait_for_timeout(550)
            return True
    return False


def slip_n(page) -> tuple[int, str]:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=3, ticket_type="power") or {}
    raw = str(slip.get("raw_slip_section") or "")
    return int(slip.get("n_selected") or 0), raw


def main() -> int:
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = ctx.new_page()
        page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
        page.wait_for_timeout(900)
        page.set_viewport_size({"width": 1400, "height": 900})

        frame = P.find_prizepicks_frame(page)
        P.dismiss_modal(frame, page)
        P.clear_slip(frame)
        _, frame = P.verify_slip_empty(frame, page)
        P.set_ticket_type(frame, "power")

        # 1 Calvert SOT 0.5 More
        P.navigate_board_for_sport(page, "Soccer", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        try:
            P._switch_board_filter(frame, page, "SOT")
        except Exception:
            pass
        react_search(page, "Dominic Calvert-Lewin")
        page.wait_for_timeout(1300)
        if not dom_click(page, "Dominic Calvert-Lewin", "SOT", "0.5", "more"):
            if not dom_click(page, "Calvert-Lewin", "SOT", "0.5", "more"):
                print("FAIL Calvert", flush=True)
                return 2
        n, _ = slip_n(page)
        print(f"after Calvert n={n}", flush=True)

        # 2 Ionescu PRA 25 Less
        switched = P.switch_sport_tab(
            P.find_prizepicks_frame(page), page, "WNBA", settle_ms=1400
        )
        if switched is None:
            P.navigate_board_for_sport(page, "WNBA", settle_ms=1600)
        frame = P.find_prizepicks_frame(page)
        try:
            P._switch_board_filter(frame, page, "PRA")
        except Exception:
            pass
        react_search(page, "Sabrina Ionescu")
        page.wait_for_timeout(1300)
        if not dom_click(page, "Sabrina Ionescu", "PRA", "25", "less"):
            print("FAIL Ionescu PRA", flush=True)
            return 2
        n, _ = slip_n(page)
        print(f"after Ionescu n={n}", flush=True)

        # 3 Combo Assists 9.5 Less (from screenshots; Yamal Goblin 1.5 gone)
        react_search(page, "Sabrina Ionescu + Courtney Williams")
        page.wait_for_timeout(1400)
        # filter Assists
        try:
            P._switch_board_filter(P.find_prizepicks_frame(page), page, "Assists")
        except Exception:
            pass
        page.wait_for_timeout(400)
        react_search(page, "Courtney Williams")
        page.wait_for_timeout(1000)
        ok = dom_click(page, "Sabrina Ionescu + Courtney Williams", "Assists", "9.5", "less")
        if not ok:
            ok = dom_click(page, "Ionescu + Courtney", "Assists", "9.5", "less")
        if not ok:
            # fallback Yamal standard SOT 2.0 if combo fails
            print("combo miss — trying Yamal Standard SOT 2.0 as 3rd", flush=True)
            switched = P.switch_sport_tab(
                P.find_prizepicks_frame(page), page, "Soccer", settle_ms=1400
            )
            if switched is None:
                P.navigate_board_for_sport(page, "Soccer", settle_ms=1500)
            try:
                P._switch_board_filter(P.find_prizepicks_frame(page), page, "SOT")
            except Exception:
                pass
            react_search(page, "Lamine Yamal")
            page.wait_for_timeout(1300)
            ok = dom_click(page, "Lamine Yamal", "SOT", "2", "more")
            if not ok:
                ok = dom_click(page, "Lamine Yamal", "SOT", "2.0", "more")
        if not ok:
            print("FAIL third leg", flush=True)
            return 2

        n, raw = slip_n(page)
        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(
                timeout=2000
            )
        except Exception:
            pass
        n, raw = slip_n(page)
        px = n_correct_power(raw, n if n else 3)
        OUT.write_text(
            json.dumps({"ok": n == 3, "n": n, "px": px, "raw_head": raw[:800]}, indent=2),
            encoding="utf-8",
        )
        print(f"FINAL ok={n==3} n={n} N-correct={px}", flush=True)
        print(raw[:600], flush=True)
        print(f"wrote {OUT}. No submit.", flush=True)
        return 0 if n == 3 else 2


if __name__ == "__main__":
    raise SystemExit(main())
