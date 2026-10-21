#!/usr/bin/env python3
"""Find Wicks+Goedert tab and add Croskey Goblin. No submit."""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"


def slip_raw(page) -> str:
    try:
        frame = P.find_prizepicks_frame(page)
        slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
        return (slip.get("raw_slip_section") or slip.get("raw_text") or "")
    except Exception as e:
        return f"ERR:{e}"


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


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        boards = []
        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "prizepicks.com/board" in (pg.url or ""):
                    boards.append(pg)
        print(f"board tabs: {len(boards)}")

        # Inventory slips
        targets = []
        for i, pg in enumerate(boards):
            try:
                pg.bring_to_front()
                time.sleep(0.3)
                raw = slip_raw(pg)
            except Exception as e:
                print(f"[{i}] read fail {e}")
                continue
            low = raw.lower()
            tag = []
            if "bondar" in low and "sakkari" in low:
                tag.append("A")
            if "tjen" in low and "kudermetova" in low:
                tag.append("B")
            if "marks" in low and "hollins" in low and "vele" in low:
                tag.append("C")
            if "wicks" in low and "goedert" in low and ("croskey" in low or "merritt" in low) and "henry" not in low:
                tag.append("D-full")
            elif "wicks" in low and "goedert" in low and "henry" not in low:
                tag.append("D-partial")
            if "marks" in low and "hollins" in low and "goedert" in low and "cousins" in low:
                tag.append("E")
            n = raw.count("More")  # rough
            print(f"[{i}] tags={tag or '-'} snippet={raw.replace(chr(10),' ')[:120]}")
            if "D-partial" in tag:
                targets.append(pg)

        if not targets:
            print("No D-partial tab found")
            return 2

        page = targets[-1]
        page.bring_to_front()
        print("Adding Croskey to D-partial…")
        frame = P.find_prizepicks_frame(page)
        P.switch_sport_tab(frame, page, "NFL", settle_ms=1000)
        frame = P.find_prizepicks_frame(page)
        react_search(page, "Croskey-Merritt")
        page.wait_for_timeout(1600)
        cards = P.get_all_cards(frame)
        hits = [
            c
            for c in cards
            if "croskey" in str(c.get("player") or "").lower()
            and P._norm(c.get("prop_type")) == "rush yards"
            and "+" not in str(c.get("player") or "")
        ]
        print("candidates", len(hits))
        if not hits:
            return 2
        card = hits[0]
        btn = card.get("more_btn")
        idx = card.get("more_index")
        # cycle to nearest goblin
        seen = set()
        best = None
        for i in range(12):
            st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
            pt = str(st.get("pickType") or "").lower()
            line = st.get("line")
            print(f" face {pt} {line}")
            key = (pt, line)
            if key in seen and i > 0:
                break
            seen.add(key)
            if pt == "goblin" and line is not None:
                if best is None or abs(float(line) - 49.5) < abs(float(best) - 49.5):
                    best = float(line)
                    if abs(float(line) - 49.5) < 0.01:
                        break
            if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
                break
            time.sleep(0.4)
        if best is None:
            print("no goblin face")
            return 2
        # land on best
        seen = set()
        for i in range(12):
            st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
            pt = str(st.get("pickType") or "").lower()
            line = st.get("line")
            if pt == "goblin" and line is not None and abs(float(line) - best) < 0.01:
                break
            key = (pt, line)
            if key in seen and i > 0:
                break
            seen.add(key)
            P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop="")
            time.sleep(0.35)
        print("clicking More at", best)
        msg = page.evaluate(
            """(line) => {
              const want = String(line);
              const btns = Array.from(document.querySelectorAll('button')).filter(b => (b.innerText||'').trim()==='More');
              for (const btn of btns) {
                let root = btn;
                for (let i=0;i<12 && root;i++) {
                  const t = (root.innerText||'');
                  const tl = t.toLowerCase();
                  if (tl.includes('croskey') && t.includes(want) && tl.includes('rush yards') && !tl.includes('henry') && !tl.includes('combo') && t.length < 550) {
                    btn.click(); return 'ok';
                  }
                  root = root.parentElement;
                }
              }
              return 'no';
            }""",
            best,
        )
        print("click", msg)
        time.sleep(0.7)
        raw = slip_raw(page)
        print(raw[:1500])
        ok = (
            "wicks" in raw.lower()
            and "goedert" in raw.lower()
            and ("croskey" in raw.lower() or "merritt" in raw.lower())
            and "henry" not in raw.lower()
        )
        print("D READY" if ok else "D STILL INCOMPLETE")
        try:
            P.find_prizepicks_frame(page).get_by_text("Power Play", exact=True).first.click(timeout=1200)
        except Exception:
            pass
        return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
