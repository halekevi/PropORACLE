#!/usr/bin/env python3
"""Retry priority still-live slips for 2026-09-29. No submit."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from playwright.sync_api import sync_playwright

import collect_payout_data as P
import utils.pp_manual_slip as M
from utils.pp_manual_slip import n_correct_power, prop_ok, react_search, _norm

# Reuse helpers from the main builder
import importlib.util

spec = importlib.util.spec_from_file_location(
    "pp929", _REPO / "_tmp_build_pp_tickets_929.py"
)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(mod)

CDP = "http://127.0.0.1:9222"
BOARD = "https://app.prizepicks.com/board"
OUT = Path(r"H:/PropORACLE_main_cp/data/reports/pp_priority_tickets_2026-09-29.json")

# Priority slips from short card — only faces that should still be board-live
TICKETS = [
    {
        "id": "S3-Ionescu-unders",
        "legs": [
            {"sport": "WNBA", "player": "Sabrina Ionescu", "prop": "Pts+Asts", "prop_type": "Pts+Asts", "direction": "UNDER", "line": 21.0, "pick_type": "standard"},
            {"sport": "WNBA", "player": "Sabrina Ionescu", "prop": "Pts+Rebs", "prop_type": "Pts+Rebs", "direction": "UNDER", "line": 19.5, "pick_type": "standard"},
            {"sport": "WNBA", "player": "Sabrina Ionescu", "prop": "Points", "prop_type": "Points", "direction": "UNDER", "line": 15.5, "pick_type": "standard"},
        ],
    },
    {
        "id": "S3-Boston-Courtney-IonescuPts",
        "legs": [
            {"sport": "WNBA", "player": "Aliyah Boston", "prop": "Defensive Rebounds", "prop_type": "Defensive Rebounds", "direction": "UNDER", "line": 6.5, "pick_type": "standard"},
            {"sport": "WNBA", "player": "Courtney Williams", "prop": "Pts+Rebs", "prop_type": "Pts+Rebs", "direction": "UNDER", "line": 16.5, "pick_type": "standard"},
            {"sport": "WNBA", "player": "Sabrina Ionescu", "prop": "Points", "prop_type": "Points", "direction": "UNDER", "line": 15.5, "pick_type": "standard"},
        ],
    },
    {
        "id": "G2-Schlittler-BoydOuts",
        "legs": [
            {"sport": "MLB", "player": "Cam Schlittler", "prop": "Pitcher Strikeouts", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 4.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Matthew Boyd", "prop": "Pitching Outs", "prop_type": "Pitching Outs", "direction": "OVER", "line": 8.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "G2-BoydKs-CalvertLewin",
        "legs": [
            {"sport": "MLB", "player": "Matthew Boyd", "prop": "Pitcher Strikeouts", "prop_type": "Pitcher Strikeouts", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "Soccer", "player": "Dominic Calvert-Lewin", "prop": "Shots On Target", "prop_type": "Shots On Target", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "G2-Yamal-BoydERA",
        "legs": [
            {"sport": "Soccer", "player": "Lamine Yamal", "prop": "Shots On Target", "prop_type": "Shots On Target", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Matthew Boyd", "prop": "Earned Runs Allowed", "prop_type": "Earned Runs Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
    {
        "id": "G2-Hornicek-BoydERA",
        "legs": [
            {"sport": "Soccer", "player": "Lukas Hornicek", "prop": "Goalie Saves", "prop_type": "Goalie Saves", "direction": "OVER", "line": 2.5, "pick_type": "goblin"},
            {"sport": "MLB", "player": "Matthew Boyd", "prop": "Earned Runs Allowed", "prop_type": "Earned Runs Allowed", "direction": "OVER", "line": 0.5, "pick_type": "goblin"},
        ],
    },
]


def main() -> int:
    print(f"Building {len(TICKETS)} priority slips", flush=True)
    with sync_playwright() as pwy:
        browser = pwy.chromium.connect_over_cdp(CDP, timeout=90_000)
        ctx = browser.contexts[0]
        results = []
        for i, ticket in enumerate(TICKETS):
            if i == 0:
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
                page = ctx.new_page()
                page.goto(BOARD, wait_until="domcontentloaded", timeout=60_000)
                page.wait_for_timeout(700)
            try:
                results.append(mod.build_one(page, ticket))
            except Exception as e:
                print(f"{ticket['id']} EXC {e}", flush=True)
                results.append({"id": ticket["id"], "ok": False, "fails": [str(e)], "n": 0})
            time.sleep(0.4)

        OUT.write_text(json.dumps({"results": results}, indent=2, default=str), encoding="utf-8")
        print("\nSUMMARY", flush=True)
        ok_n = 0
        for r in results:
            print(
                f"  {r.get('id')} ok={r.get('ok')} n={r.get('n')}/{r.get('want_n')} "
                f"N-correct={r.get('px')} fails={r.get('fails')}",
                flush=True,
            )
            if r.get("ok"):
                ok_n += 1
        print(f"wrote {OUT}. Built {ok_n}/{len(results)}. No submit.", flush=True)
        return 0 if ok_n else 2


if __name__ == "__main__":
    raise SystemExit(main())
