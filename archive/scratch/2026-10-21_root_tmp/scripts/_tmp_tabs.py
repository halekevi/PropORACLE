import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from utils.prizepicks_cdp import pick_cdp_warmed_page

with sync_playwright() as p:
    browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=15000)
    page = pick_cdp_warmed_page(browser.contexts[0], browser=browser)
    frame = None
    for fr in page.frames:
        if "prizepicks" in (fr.url or ""):
            frame = fr
            break
    labels = frame.evaluate(
        """() => Array.from(document.querySelectorAll('button, [role="tab"]'))
          .map(e => (e.innerText || '').trim().split('\\n')[0].trim())
          .filter(t => t && t.length < 28)"""
    )
    Path("scripts/_tmp_tabs.txt").write_text("\n".join(labels), encoding="utf-8")
    print(len(labels))
