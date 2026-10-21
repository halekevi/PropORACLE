#!/usr/bin/env python3
"""Probe live PrizePicks board for Power 3 #1 players."""
from playwright.sync_api import sync_playwright

QUERIES = {
    "MLB": (2, ["Schlittler", "Cam Schlittler"]),
    "WNBA": (3, ["Ogunbowale", "Arike"]),
    "CFB": (15, ["Dampier", "Devon Dampier"]),
}


def main() -> None:
    with sync_playwright() as p:
        b = p.chromium.connect_over_cdp("http://127.0.0.1:9222", timeout=30_000)
        page = None
        for ctx in b.contexts:
            for pg in ctx.pages:
                if "prizepicks.com" in (pg.url or "") and "board" in (pg.url or ""):
                    page = pg
                    break
            if page:
                break
        if page is None:
            print("no board tab")
            return
        page.bring_to_front()
        for league, (lid, queries) in QUERIES.items():
            page.goto(
                f"https://app.prizepicks.com/board?league_id={lid}",
                wait_until="domcontentloaded",
                timeout=45_000,
            )
            page.wait_for_timeout(2500)
            box = page.locator("input[placeholder*='Search'], input[aria-label*='search' i]").first
            for q in queries:
                try:
                    box.click(timeout=2000)
                    box.fill("")
                    box.fill(q)
                    page.wait_for_timeout(1800)
                    body = page.inner_text("body")
                    hit = q.lower() in body.lower()
                    print(f"{league} q={q!r} hit={hit}")
                    if hit:
                        idx = body.lower().find(q.lower())
                        snip = body[max(0, idx - 30) : idx + 100].replace("\n", " | ")
                        print(f"  snip: {snip!r}")
                except Exception as e:
                    print(f"{league} q={q!r} ERR {e}")


if __name__ == "__main__":
    main()
