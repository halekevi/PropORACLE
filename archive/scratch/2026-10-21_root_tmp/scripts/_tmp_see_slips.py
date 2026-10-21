"""Show every PrizePicks tab lineup so we can rebuild on the visible board."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

with sync_playwright() as pwy:
    browser = pwy.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=30_000)
    n = 0
    for ctx in browser.contexts:
        for pg in ctx.pages:
            url = pg.url or ""
            if "prizepicks.com" not in url or "online-metrix" in url:
                continue
            n += 1
            text = pg.evaluate("() => (document.body && document.body.innerText) || ''")
            i = text.find("Current Lineup")
            j = text.find("Submit Lineup")
            slip = text[i : j + 20] if i >= 0 else "(no Current Lineup)"
            print(f"=== tab {n} {url} ===")
            print(slip[:900])
            print()
    print("tabs", n)
