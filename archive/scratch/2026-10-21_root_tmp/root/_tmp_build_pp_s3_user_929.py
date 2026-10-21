#!/usr/bin/env python3
"""Build one Power-3 from user screenshots. No submit."""
from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

spec = importlib.util.spec_from_file_location(
    "pp929", _REPO / "_tmp_build_pp_tickets_929.py"
)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_user_s3_calvert_yamal_ionescu_2026-09-29.json")

TICKET = {
    "id": "S3-Calvert-Yamal-IonescuPRA",
    "legs": [
        {
            "sport": "Soccer",
            "player": "Dominic Calvert-Lewin",
            "prop": "Shots On Target",
            "prop_type": "Shots On Target",
            "direction": "OVER",
            "line": 0.5,
            "pick_type": "goblin",
        },
        {
            "sport": "Soccer",
            "player": "Lamine Yamal",
            "prop": "Shots On Target",
            "prop_type": "Shots On Target",
            "direction": "OVER",
            "line": 1.5,
            "pick_type": "goblin",
        },
        {
            "sport": "WNBA",
            "player": "Sabrina Ionescu",
            "prop": "Pts+Rebs+Asts",
            "prop_type": "Pts+Rebs+Asts",
            "direction": "UNDER",
            "line": 25.0,
            "pick_type": "standard",
        },
    ],
}


def main() -> int:
    print(f"Building {TICKET['id']}", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        page = next(
            (
                p
                for p in ctx.pages
                if "prizepicks.com" in (p.url or "") and "board" in (p.url or "")
            ),
            None,
        )
        if page is None:
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(900)
        else:
            # Fresh tab so we don't overwrite other lined-up slips
            page = ctx.new_page()
            page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
            page.wait_for_timeout(700)

        result = mod.build_one(page, TICKET)
        OUT.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
        print(
            f"ok={result.get('ok')} n={result.get('n')}/{result.get('want_n')} "
            f"N-correct={result.get('px')} fails={result.get('fails')}",
            flush=True,
        )
        print(f"wrote {OUT}. No submit.", flush=True)
        return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
