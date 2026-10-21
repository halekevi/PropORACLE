#!/usr/bin/env python3
"""Build NFL tickets E–J from mid-board gaps. Handcheck source. No submit.

Criteria:
  - mid gaps only (~10–45 yd overs / ~8–40 yd unders with real W1 avg)
  - no C players, no sit/outlier tops (Shough/Monangai/Walker/Watson/Brissett/…)
  - Standard keep props only; Goblin OVER only
  - Goblin-only, Standard-only, and mixed slips
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.nfl_keep_gates import assert_nfl_ready_source, assert_nfl_ticket_l5_source

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
TICKET_SOURCE = "handcheck"
assert_nfl_ticket_l5_source("handcheck")
assert_nfl_ready_source(TICKET_SOURCE)

# Mid-board hand-picked. Lines = step8 faces; exact-match only (no nearest).
TICKETS = [
    {
        "id": "E",
        "mode": "power",
        "kind": "goblin_only",
        "legs": [
            {"player": "Xavier Hutchinson", "search": "Hutchinson", "prop": "Rec Yards", "line": 9.5, "pick": "goblin", "dir": "OVER", "avg": 32, "note": "mid +22.5 D Below"},
            {"player": "Pat Bryant", "search": "Pat Bryant", "prop": "Rec Yards", "line": 19.5, "pick": "goblin", "dir": "OVER", "avg": 42, "note": "mid +22.5 D Below"},
            {"player": "Evan Engram", "search": "Engram", "prop": "Rec Yards", "line": 19.5, "pick": "goblin", "dir": "OVER", "avg": 43, "note": "mid +23.5 D Below"},
        ],
    },
    {
        "id": "F",
        "mode": "flex",
        "kind": "standard_only",
        "legs": [
            {"player": "Brock Purdy", "search": "Purdy", "prop": "Rush Yards", "line": 14.5, "pick": "standard", "dir": "OVER", "avg": 29, "note": "Std rush keep +14.5 D Below"},
            {"player": "Jayden Reed", "search": "Jayden Reed", "prop": "Rec Yards", "line": 35.5, "pick": "standard", "dir": "UNDER", "avg": 20, "note": "mid U +15.5"},
            {"player": "Omarion Hampton", "search": "Hampton", "prop": "Rush Yards", "line": 62.5, "pick": "standard", "dir": "UNDER", "avg": 43, "note": "mid U +19.5"},
            {"player": "Wil Lutz", "search": "Wil Lutz", "prop": "Kicking Points", "line": 8.0, "pick": "standard", "dir": "UNDER", "avg": 4, "note": "kick U D Elite"},
        ],
    },
    {
        "id": "G",
        "mode": "flex",
        "kind": "mixed",
        "legs": [
            {"player": "Deebo Samuel Sr.", "search": "Deebo", "prop": "Rush+Rec Yds", "line": 39.5, "pick": "goblin", "dir": "OVER", "avg": 60, "note": "G mid +20.5"},
            {"player": "Hunter Henry", "search": "Hunter Henry", "prop": "Rec Yards", "line": 13.5, "pick": "goblin", "dir": "OVER", "avg": 26, "note": "G mid +12.5"},
            {"player": "RJ Harvey", "search": "RJ Harvey", "prop": "Rush Yards", "line": 27.5, "pick": "standard", "dir": "UNDER", "avg": 18, "note": "S U D Elite"},
            {"player": "DeMario Douglas", "search": "Douglas", "prop": "Rec Yards", "line": 34.5, "pick": "standard", "dir": "UNDER", "avg": 20, "note": "S U mid"},
        ],
    },
    {
        "id": "H",
        "mode": "power",
        "kind": "goblin_only",
        "legs": [
            {"player": "Rhamondre Stevenson", "search": "Rhamondre", "prop": "Rec Yards", "line": 14.5, "pick": "goblin", "dir": "OVER", "avg": 44, "note": "G mid +29.5 D Weak"},
            {"player": "Noah Fant", "search": "Noah Fant", "prop": "Rec Yards", "line": 9.5, "pick": "goblin", "dir": "OVER", "avg": 33, "note": "G mid +23.5 D Weak"},
            {"player": "Dalton Schultz", "search": "Schultz", "prop": "Rec Yards", "line": 19.5, "pick": "goblin", "dir": "OVER", "avg": 35, "note": "G mid +15.5 D Below"},
        ],
    },
    {
        "id": "I",
        "mode": "power",
        "kind": "standard_only",
        "legs": [
            {"player": "Xavier Worthy", "search": "Worthy", "prop": "Rec Yards", "line": 37.5, "pick": "standard", "dir": "UNDER", "avg": 18, "note": "S U mid"},
            {"player": "Kayshon Boutte", "search": "Boutte", "prop": "Rec Yards", "line": 35.5, "pick": "standard", "dir": "UNDER", "avg": 21, "note": "S U mid"},
            {"player": "Braelon Allen", "search": "Braelon", "prop": "Rush Yards", "line": 24.5, "pick": "standard", "dir": "OVER", "avg": 40, "note": "S rush keep +15.5"},
        ],
    },
    {
        "id": "J",
        "mode": "power",
        "kind": "mixed",
        "legs": [
            {"player": "Jayden Daniels", "search": "Jayden Daniels", "prop": "Rush Yards", "line": 16.5, "pick": "goblin", "dir": "OVER", "avg": 31, "note": "G mid +14.5"},
            {"player": "Chris Bell", "search": "Chris Bell", "prop": "Rec Yards", "line": 5.5, "pick": "goblin", "dir": "OVER", "avg": 25, "note": "G mid +19.5"},
            {"player": "Bucky Irving", "search": "Bucky", "prop": "Rush Yards", "line": 60.5, "pick": "standard", "dir": "UNDER", "avg": 45, "note": "S U mid"},
        ],
    },
]


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


def prop_match(card_prop: str, want: str) -> bool:
    cp, wp = P._norm(card_prop or ""), P._norm(want or "")
    if wp in ("rec yards", "receiving yards"):
        return cp in ("rec yards", "receiving yards")
    if wp in ("rush yards", "rushing yards"):
        return cp in ("rush yards", "rushing yards")
    if "rush" in wp and "rec" in wp:
        return ("rush" in cp and "rec" in cp) or cp in ("rush+rec yds", "rush rec yds")
    if "kick" in wp:
        return "kick" in cp
    return wp == cp


def cycle_exact(frame, card, want_pick: str, want_line: float):
    btn, idx = card.get("more_btn"), card.get("more_index")
    seen = set()
    for i in range(16):
        st = P._read_card_state_from_more(btn, frame=frame, more_index=idx) or {}
        pt = str(st.get("pickType") or "standard").lower()
        line = st.get("line")
        key = (pt, line)
        print(f"  face[{i}] {pt} {line}")
        if key in seen and i > 0:
            break
        seen.add(key)
        try:
            lf = float(line) if line is not None else None
        except (TypeError, ValueError):
            lf = None
        if pt == want_pick and lf is not None and abs(lf - want_line) < 0.01:
            return lf
        if not P._click_alt_line_swap(btn, frame=frame, more_index=idx, player="", prop=""):
            break
        time.sleep(0.3)
    return None


def tight_click(page, player: str, prop: str, line: float, direction: str) -> str:
    action = "Less" if direction.upper() in ("UNDER", "LESS") else "More"
    last = player.split()[-1].replace(".", "").replace("Sr", "").replace("Jr", "").strip()
    if last.lower() in ("sr", "jr"):
        parts = player.replace(".", "").split()
        last = parts[-2] if len(parts) >= 2 else parts[-1]
    return page.evaluate(
        """({last, prop, line, action}) => {
          const want = String(line);
          const pn = prop.toLowerCase();
          const btns = [...document.querySelectorAll('button')].filter(
            b => ((b.innerText||'').trim()===action)
          );
          for (const btn of btns) {
            let root = btn;
            for (let i=0;i<8 && root;i++) {
              const t = (root.innerText||'');
              const tl = t.toLowerCase();
              const moreCount = (t.match(/\\bMore\\b/g)||[]).length;
              if (moreCount !== 1 || t.length > 420) { root = root.parentElement; continue; }
              if (!tl.includes(last.toLowerCase()) || !t.includes(want)) { root = root.parentElement; continue; }
              if (tl.includes('+') || tl.includes('combo')) { root = root.parentElement; continue; }
              let ok = false;
              if (pn.includes('kick')) ok = tl.includes('kick');
              else if (pn.includes('rush') && pn.includes('rec'))
                ok = (tl.includes('rush') && tl.includes('rec')) || tl.includes('rush+rec');
              else if (pn.includes('rush'))
                ok = tl.includes('rush yards') && !tl.includes('rec');
              else if (pn.includes('rec'))
                ok = (tl.includes('rec yards') || tl.includes('receiving')) && !tl.includes('longest') && !tl.includes('rush');
              if (!ok) { root = root.parentElement; continue; }
              btn.click();
              return 'ok ' + t.replace(/\\s+/g,' ').slice(0,130);
            }
          }
          return 'no';
        }""",
        {"last": last, "prop": prop, "line": line, "action": action},
    )


def add_leg(page, leg) -> bool:
    frame = P.find_prizepicks_frame(page)
    P.switch_sport_tab(frame, page, "NFL", settle_ms=900)
    frame = P.find_prizepicks_frame(page)
    P.dismiss_modal(frame, page)
    react_search(page, "")
    page.wait_for_timeout(200)
    react_search(page, leg["search"])
    page.wait_for_timeout(1500)
    needle = leg["player"].split()[-1].replace(".", "").lower()
    if needle in ("sr", "jr"):
        needle = leg["player"].split()[-2].replace(".", "").lower()
    frame = P.find_prizepicks_frame(page)
    hits = [
        c
        for c in P.get_all_cards(frame)
        if needle in str(c.get("player") or "").lower().replace(".", "")
        and "+" not in str(c.get("player") or "")
        and prop_match(str(c.get("prop_type") or ""), leg["prop"])
    ]
    print(f"  candidates={len(hits)}")
    if not hits:
        return False
    card = hits[0]
    line = cycle_exact(frame, card, leg["pick"], float(leg["line"]))
    if line is None:
        print("  exact face miss")
        return False
    msg = tight_click(page, leg["player"], leg["prop"], line, leg["dir"])
    print(f"  click {msg}")
    return str(msg).startswith("ok")


def build(page, ticket) -> dict:
    tid, mode = ticket["id"], ticket["mode"]
    print(f"\n======== {tid} [{ticket['kind']}] {mode} ========")
    page.bring_to_front()
    P.navigate_board_for_sport(page, "NFL", settle_ms=1800)
    frame = P.find_prizepicks_frame(page)
    P.clear_slip(frame)
    _, frame = P.verify_slip_empty(frame, page)
    P.set_ticket_type(frame, mode)
    fails = []
    for leg in ticket["legs"]:
        print(f"\n--- {leg['player']} {leg['pick']} {leg['dir']} {leg['line']} ({leg['note']}) ---")
        ok = add_leg(page, leg)
        print("  result", ok)
        if not ok:
            fails.append(leg["player"])
        try:
            P.set_ticket_type(P.find_prizepicks_frame(page), mode)
        except Exception:
            pass
        time.sleep(0.35)
    try:
        label = "Power Play" if mode == "power" else "Flex Play"
        P.find_prizepicks_frame(page).get_by_text(label, exact=True).first.click(timeout=1500)
    except Exception:
        pass
    slip = P.read_slip(
        P.find_prizepicks_frame(page), n_legs=len(ticket["legs"]), ticket_type=mode
    ) or {}
    raw = slip.get("raw_slip_section") or slip.get("raw_text") or ""
    n = int(slip.get("n_selected") or 0)
    print(f"[{tid} FINAL] {n}/{len(ticket['legs'])} fails={fails}\n{raw[:700]}")
    ready = n >= len(ticket["legs"]) and not fails
    if ready:
        assert_nfl_ready_source(TICKET_SOURCE)
        print(f"[{tid}] READY source={TICKET_SOURCE}")
    else:
        print(f"[{tid}] INCOMPLETE")
    return {
        "id": tid,
        "kind": ticket["kind"],
        "mode": mode,
        "ready": ready,
        "n": n,
        "need": len(ticket["legs"]),
        "fails": fails,
        "legs": ticket["legs"],
    }


def main() -> int:
    results = []
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        # New tabs so A–D stay untouched
        pages = []
        for _ in range(len(TICKETS)):
            pg = ctx.new_page()
            pg.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            pg.wait_for_timeout(800)
            pages.append(pg)
        for page, ticket in zip(pages, TICKETS):
            try:
                results.append(build(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}")
                results.append(
                    {
                        "id": ticket["id"],
                        "kind": ticket["kind"],
                        "mode": ticket["mode"],
                        "ready": False,
                        "n": 0,
                        "need": len(ticket["legs"]),
                        "fails": [str(e)],
                        "legs": ticket["legs"],
                    }
                )
        if pages:
            pages[0].bring_to_front()

    print("\n======== NFL CARD SUMMARY ========")
    for r in results:
        tag = "READY" if r["ready"] else "INCOMPLETE"
        print(
            f"  {r['id']} {r['kind']:<14} {r['mode']:<5} {tag} "
            f"{r['n']}/{r['need']} fails={r['fails'] or '-'}"
        )
    print("No submit.")
    return 0 if any(r["ready"] for r in results) else 2


if __name__ == "__main__":
    raise SystemExit(main())
