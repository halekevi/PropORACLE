#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

import rank_best_props_today as R

ROOT = Path(r"H:/PropORACLE_main_cp")
DATE = "2026-09-19"
BADGE_OK = {"Diamond", "Platinum", "Gold", "Gold*"}


def fmt(r: dict) -> str:
    return (
        f"  {r.get('tier')}/{r.get('badge'):10} "
        f"{str(r.get('player') or ''):24} "
        f"{str(r.get('prop') or ''):22} "
        f"{(r.get('side') or '?')[0]}{r.get('line')}  "
        f"L5 {r.get('l5')} cov {r.get('cover')}  "
        f"D={r.get('d')}  miss={r.get('misses')}  {r.get('matchup')}"
    )


def main() -> None:
    all_rows: list[dict] = []
    print(f"Best Standards {DATE} root={ROOT}")
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(ROOT, DATE, sport, folder, fname)
        if df.empty:
            print(f"\n===== {sport} =====\n  (no step8)")
            continue
        all_rows.extend(R.recs(df))
        so, su, gob = R.bucket(all_rows, sport)
        so_g = [r for r in so if str(r.get("badge") or "").replace("*", "") in {"Diamond", "Platinum", "Gold"} or str(r.get("badge") or "").startswith("Gold")]
        su_g = [r for r in su if str(r.get("badge") or "").replace("*", "") in {"Diamond", "Platinum", "Gold"} or str(r.get("badge") or "").startswith("Gold")]
        print(f"\n===== {sport} =====  stdO {len(so)} (Gold+ {len(so_g)})  stdU {len(su)} (Gold+ {len(su_g)})")
        print("Standard OVER Gold+")
        for r in so_g[:12] or so[:5]:
            print(fmt(r))
        if not so_g and not so:
            print("  (none)")
        print("Standard UNDER Gold+")
        for r in su_g[:12] or su[:5]:
            print(fmt(r))
        if not su_g and not su:
            print("  (none)")

    df = R.load_cfb(ROOT, DATE)
    all_rows.extend(R.recs(df))
    so, su, gob = R.bucket(all_rows, "CFB")
    so_g = [r for r in so if str(r.get("badge") or "").startswith(("Diamond", "Platinum", "Gold"))]
    su_g = [r for r in su if str(r.get("badge") or "").startswith(("Diamond", "Platinum", "Gold"))]
    print(f"\n===== CFB =====  stdO {len(so)} Gold+ {len(so_g)}  stdU {len(su)} Gold+ {len(su_g)}")
    print("Standard OVER Gold+")
    for r in so_g[:15]:
        print(fmt(r))
    print("Standard UNDER Gold+")
    for r in su_g[:20]:
        prop = str(r.get("prop") or "").lower()
        note = ""
        if "rec" in prop and "yard" in prop and (r.get("side") or "").upper() == "UNDER":
            note = "  [FADE cover-stack / list-only]"
        print(fmt(r) + note)
    # also top Silver UNDER Rush for ticket path
    rush_u = [
        r
        for r in su
        if "rush" in str(r.get("prop") or "").lower() and "yard" in str(r.get("prop") or "").lower()
    ]
    print(f"\nCFB Standard Rush Yards UNDER (ticket-path family) n={len(rush_u)}")
    for r in rush_u[:12]:
        print(fmt(r))


if __name__ == "__main__":
    main()
