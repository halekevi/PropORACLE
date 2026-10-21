"""Read the open lineup. Does not click Submit."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from playwright.sync_api import sync_playwright

import scripts.collect_payout_data as P

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=15000)
    page = None
    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "prizepicks.com" in (pg.url or "") and "online-metrix" not in (pg.url or ""):
                page = pg
                break
    if page is None:
        raise SystemExit("no page")
    frame = P.find_prizepicks_frame(page)
    text = frame.evaluate("() => document.body.innerText") or ""
    start = text.find("Current Lineup")
    section = text[start : start + 1800] if start >= 0 else "NO LINEUP"
    Path("scripts/_tmp_slip.txt").write_text(section, encoding="utf-8")
    print("url", (page.url or "")[:90])
    print("len", len(section))
