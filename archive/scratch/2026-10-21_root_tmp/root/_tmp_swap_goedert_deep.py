#!/usr/bin/env python3
"""Swap Goedert Rec Goblin to deep/consistent face (~18.5) on D and E. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
# Prefer deepest (easiest) Goblin OVER near this anchor
WANT_DEEP = 18.5


def slip_raw(page) -> str:
    frame = P.find_prizepicks_frame(page)
    slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
    return slip.get("raw_slip_section") or slip.get("raw_text") or ""


def react_search(page, q: str) -> None:
    page.evaluate(
        """(q) => {
      const inp = document.querySelector("input[aria-label='search'], input[placeholder*='Search']");
      if (!inp) return;
      const proto = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value');
      if (proto && proto.set) proto.set.call(inp, q); else inp.value = q;
      inp.dispatchEvent(new Event('input', {bubbles:true}));
      inp.dispatchEvent(new Event('change', {bubbles:true}));
    }""",
        q,
    )


def remove_goedert(page) -> bool:
    """Click the remove/X on Goedert slip row if present."""
    return bool(
        page.evaluate(
            """() => {
          const rows = Array.from(document.querySelectorAll('*')).filter(el => {
            const t = (el.innerText || '');
            return t.includes('Goedert') && t.includes('Rec') && t.length < 400;
          });
          for (const row of rows) {
            // look for X / remove buttons nearby
            const btns = row.querySelectorAll('button, [role=button], svg');
            for (const b of btns) {
              const label = ((b.getAttribute('aria-label') || '') + (b.innerText || '')).toLowerCase();
              if (label.includes('remove') || label.includes('clear') || label === 'x' || label.includes('close')) {
                b.click(); return 'aria';
              }
            }
            // parent walk for trash
            let p = row;
            for (let i=0;i<6 && p;i++) {
              const xs = p.querySelectorAll('button');
              for (const b of xs) {
                const t = (b.innerText || '').trim();
                const a = (b.getAttribute('aria-label') || '').toLowerCase();
                if (t === '×' || t === 'X' || a.includes('remove')) { b.click(); return 'x'; }
              }
              p = p.parentElement;
            }
          }
          return '';
        }"""
        )
    )


def deepest_goblin_rec(frame, card) -> float | None:
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen, goblins = set(), []
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        if pt == "goblin" and line is not None:
            try:
                goblins.append(float(line))
            except Exception:
                pass
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.4)
    if not goblins:
        return None
    # deepest = lowest line for OVER
    best = min(goblins)
    print(f"  deepest goblin={best} (all={goblins})")
    # cycle to best
    seen = set()
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "").lower()
        line = st.get("line")
        if pt == "goblin" and line is not None and abs(float(line) - best) < 0.01:
            return best
        key = (pt, line)
        if key in seen and i > 0:
            break
        seen.add(key)
        P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
        time.sleep(0.35)
    return best


def add_deep_goedert(page) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.switch_sport_tab(frame, page, "NFL", settle_ms=1000)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "Dallas Goedert")
    page.wait_for_timeout(1500)
    cards = P.get_all_cards(frame)
    hits = [
        c
        for c in cards
        if "goedert" in str(c.get("player") or "").lower()
        and P._norm(c.get("prop_type")) in ("rec yards", "receiving yards")
        and "+" not in str(c.get("player") or "")
    ]
    if not hits:
        print("  no Goedert Rec card")
        return False
    card = hits[0]
    line = deepest_goblin_rec(frame, card)
    if line is None:
        return False
    msg = page.evaluate(
        """(line) => {
          const want = String(line);
          const btns = Array.from(document.querySelectorAll('button')).filter(b => (b.innerText||'').trim()==='More');
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<12 && root;i++) {
              const t = root.innerText || '';
              const tl = t.toLowerCase();
              if (tl.includes('goedert') && t.includes(want) && (tl.includes('rec yards')||tl.includes('receiving')) && !tl.includes('combo') && t.length < 500) {
                btn.click(); return 'ok';
              }
              root = root.parentElement;
            }
          }
          return 'no';
        }""",
        line,
    )
    print(f"  click {msg} @ {line}")
    return str(msg).startswith("ok")


def fix_page(page, tag: str) -> bool:
    page.bring_to_front()
    raw0 = slip_raw(page)
    if "goedert" not in raw0.lower():
        print(f"[{tag}] no Goedert — skip")
        return False
    print(f"[{tag}] before:\n{raw0[:500]}")
    # Try remove then re-add. If remove fails, clear whole slip is too destructive — abort.
    removed = remove_goedert(page)
    print(f"[{tag}] remove -> {removed!r}")
    page.wait_for_timeout(800)
    raw1 = slip_raw(page)
    if "goedert" in raw1.lower():
        # fallback: click Less/More area swap isn't available; try library remove via clear of that player in lineup
        print(f"[{tag}] still has Goedert after remove attempt — try JS lineup remove")
        page.evaluate(
            """() => {
              const all = Array.from(document.querySelectorAll('button'));
              for (const b of all) {
                const row = b.closest('[class*="lineup"], [class*="Lineup"], li, div');
                if (!row) continue;
                const t = (row.innerText || '');
                if (!t.includes('Goedert')) continue;
                const a = (b.getAttribute('aria-label') || '').toLowerCase();
                if (a.includes('remove') || a.includes('delete') || (b.innerText||'').trim() === '×') {
                  b.click(); return true;
                }
              }
              // last resort: buttons with only an svg inside near Goedert text nodes
              const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_ELEMENT);
              let n;
              while (n = walker.nextNode()) {
                if (!(n.innerText || '').includes('Goedert')) continue;
                if ((n.innerText || '').length > 500) continue;
                for (const b of n.querySelectorAll('button')) {
                  if (b.querySelector('svg') && (b.innerText || '').trim().length <= 1) { b.click(); return true; }
                }
              }
              return false;
            }"""
        )
        page.wait_for_timeout(800)
        raw1 = slip_raw(page)
    if "goedert" in raw1.lower():
        print(f"[{tag}] could not remove Goedert — leave as-is")
        return False
    ok = add_deep_goedert(page)
    page.wait_for_timeout(600)
    raw2 = slip_raw(page)
    print(f"[{tag}] after:\n{raw2[:700]}")
    # success if Goedert back and line is deep (<=22.5 preferred)
    low = raw2.lower()
    if "goedert" not in low:
        return False
    # parse a rec yards number near goedert
    import re

    m = re.search(r"goedert.*?(\d+\.5)\s*rec", raw2, re.I | re.S)
    line = float(m.group(1)) if m else None
    print(f"[{tag}] goedert line parsed={line}")
    return ok and line is not None and line <= 22.5


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=60_000)
        pages = []
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com/board" in (pg.url or ""):
                    pages.append(pg)
        print(f"board tabs: {len(pages)}")
        fixed = 0
        for i, pg in enumerate(pages):
            try:
                raw = slip_raw(pg)
            except Exception as e:
                print(f"[{i}] read fail {e}")
                continue
            if "goedert" not in raw.lower():
                continue
            # D or E style
            tag = "E" if "cousins" in raw.lower() else ("D" if "wicks" in raw.lower() or "croskey" in raw.lower() or "merritt" in raw.lower() else f"tab{i}")
            if fix_page(pg, tag):
                fixed += 1
                # re-assert mode
                try:
                    mode = "flex" if tag == "E" else "power"
                    P.set_ticket_type(P.find_prizepicks_frame(pg), mode)
                    label = "Flex Play" if mode == "flex" else "Power Play"
                    P.find_prizepicks_frame(pg).get_by_text(label, exact=True).first.click(timeout=1200)
                except Exception:
                    pass
        print(f"\nfixed={fixed}")
        return 0 if fixed else 2


if __name__ == "__main__":
    raise SystemExit(main())
