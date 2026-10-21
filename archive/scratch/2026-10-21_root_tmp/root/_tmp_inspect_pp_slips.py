"""Inspect slip state on every PrizePicks board tab."""
from __future__ import annotations
import sys
from pathlib import Path
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P

CDP = "http://127.0.0.1:9222"
with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp(CDP, timeout=120_000)
    i = 0
    for ctx in browser.contexts:
        for page in ctx.pages:
            url = page.url or ""
            if "prizepicks.com" not in url:
                continue
            i += 1
            print(f"\n--- TAB {i} {url[:80]} ---")
            try:
                page.bring_to_front()
                page.wait_for_timeout(400)
                if "board" not in url:
                    print("  (not board)")
                    continue
                frame = P.find_prizepicks_frame(page)
                slip = P.read_slip(frame, n_legs=6, ticket_type="power") or {}
                n = slip.get("n_selected")
                raw = str(slip.get("raw_slip_section") or slip.get("raw_text") or "")[:400]
                print(f"  n_selected={n}")
                print(f"  raw={raw!r}")
            except Exception as e:
                print(f"  ERR {e}")
print("\nDONE inspect")
