"""Standards for tomorrow (2026-09-20): multi-sport + honest NFL caveat."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, r"H:\PropORACLE")
from scripts.rank_best_props_today import (  # noqa: E402
    bucket,
    load_cfb,
    load_nfl,
    load_sport,
    recs,
)

ROOT = Path(r"H:\PropORACLE_main_cp")
TOMORROW = "2026-09-20"
TODAY = "2026-09-19"


def fmt(r: dict) -> str:
    side = str(r.get("side") or "").upper()
    letter = "O" if side == "OVER" else "U" if side == "UNDER" else "?"
    badge = r.get("promo") or r.get("badge") or "—"
    miss = f"  miss {r['miss_s']}" if r.get("miss_s") else ""
    return (
        f"  {r.get('prop_tier', '?')}/{str(badge):10} "
        f"{str(r.get('player', '')):26} {str(r.get('prop', '')):22} "
        f"{letter}{r.get('line')}  L5 {r.get('l5_over')}/{r.get('l5_under')}  "
        f"cov {r.get('cover')}  {r.get('def')}  {r.get('matchup')}{miss}"
    )


def print_std(sport: str, so: list, su: list, n: int = 15) -> None:
    print(f"\n===== {sport} Standard OVER (n={len(so)}) =====")
    if not so:
        print("  (none that clear L5 4+)")
    for r in so[:n]:
        print(fmt(r))
    print(f"\n===== {sport} Standard UNDER (n={len(su)}) =====")
    if not su:
        print("  (none that clear L5 4+)")
    for r in su[:n]:
        print(fmt(r))


def main() -> None:
    print(f"Standards for {TOMORROW}  root={ROOT}")
    print("NOTE: NFL L5 still pads prior season — board unusable for picks.\n")

    # Core four from tomorrow's date folder (likely empty except via spill)
    for sport, folder, fname in [
        ("WNBA", "wnba", "step8_wnba_direction.csv"),
        ("MLB", "mlb", "step8_mlb_direction.csv"),
        ("SOCCER", "soccer", "step8_soccer_direction.csv"),
        ("TENNIS", "tennis", "step8_tennis_direction.csv"),
    ]:
        df = load_sport(ROOT, TOMORROW, sport, folder, fname)
        if df.empty:
            # try today's folder; tennis/soccer windows often spill
            df = load_sport(ROOT, TODAY, sport, folder, fname)
            tag = f"{sport} (from {TODAY} board, slate-filtered)"
        else:
            tag = sport
        if df.empty:
            print(f"\n===== {sport} =====\n  (no step8)")
            continue
        rows = recs(df)
        # keep rows whose matchup/start looks like tomorrow when possible
        so, su, _ = bucket(rows, sport)
        print_std(tag, so, su)

    # CFB tomorrow unlikely; print if any
    df_cfb = load_cfb(ROOT, TOMORROW)
    if df_cfb.empty:
        df_cfb = load_cfb(ROOT, TODAY)
        if not df_cfb.empty:
            print("\n(CFB step8 is today's board only — Standards after cutoff:)")
    so, su, _ = bucket(recs(df_cfb), "CFB") if len(df_cfb) else ([], [], [])
    print_std("CFB", so, su)

    # NFL — list but mark dead
    df_nfl = load_nfl(ROOT, TOMORROW)
    so, su, _ = bucket(recs(df_nfl), "NFL") if len(df_nfl) else ([], [], [])
    print("\n***** NFL Standards gated by L5>=4 but PRIOR-SEASON PADDED — DO NOT PLAY *****")
    print_std("NFL (UNTRUSTWORTHY)", so, su, n=12)


if __name__ == "__main__":
    main()
