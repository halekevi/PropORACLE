"""Print L5=5-only best props for slate date (reply format)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
import rank_best_props_today as R

DATE = "2026-10-20"
ROOTS = [_REPO, _REPO.parent / "PropORACLE_main_cp"]


def l5_is_five(r: dict) -> bool:
    side = str(r.get("side") or "").upper()
    try:
        o = int(r["l5_over"]) if r.get("l5_over") is not None else None
        u = int(r["l5_under"]) if r.get("l5_under") is not None else None
    except (TypeError, ValueError):
        return False
    if side == "OVER":
        return o == 5 and u in (0, None)
    if side == "UNDER":
        return u == 5 and o in (0, None)
    return False


def load_sport_any(sport: str) -> tuple[Path | None, pd.DataFrame | None, str]:
    for root in ROOTS:
        if not root.is_dir():
            continue
        try:
            if sport == "NBA":
                df = R.load_nba(root, DATE)
            elif sport == "NFL":
                df = R.load_nfl(root, DATE)
            elif sport == "CFB":
                df = R.load_cfb(root, DATE)
            elif sport == "GOLF":
                df = R.load_golf(root, DATE)
            elif sport == "WNBA":
                df = R.load_sport(root, DATE, "WNBA", "wnba", "step8_wnba_direction_clean.xlsx")
            elif sport == "MLB":
                df = R.load_sport(root, DATE, "MLB", "mlb", "step8_mlb_direction_clean.xlsx")
            elif sport == "SOCCER":
                df = R.load_sport(root, DATE, "SOCCER", "soccer", "step8_soccer_direction_clean.xlsx")
            elif sport == "TENNIS":
                df = R.load_sport(root, DATE, "TENNIS", "tennis", "step8_tennis_direction_clean.xlsx")
            else:
                continue
        except Exception as exc:
            continue
        if df is not None and not df.empty:
            return root, df, ""
    return None, None, "no step8 / empty — offseason or no slate"


def main() -> None:
    all_rows: list[dict] = []
    print(f"Best props {DATE}  L5=5 only")
    for sport in ("WNBA", "MLB", "SOCCER", "TENNIS", "NBA", "NFL", "CFB", "GOLF"):
        root, df, note = load_sport_any(sport)
        if df is None or df.empty:
            print(f"\n===== {sport} =====\n  ({note})")
            continue
        rows = R.recs(df)
        all_rows.extend(rows)
        so, su, gob = R.bucket(all_rows, sport)
        so5 = [r for r in so if l5_is_five(r)]
        su5 = [r for r in su if l5_is_five(r)]
        gob5 = [r for r in gob if l5_is_five(r) and not R.skip_earned_runs(r)]
        print(f"\n===== {sport} =====  root={root.name}  stdO={len(so5)} stdU={len(su5)} gob={len(gob5)}")
        print("Standard OVER")
        if not so5:
            print("  (none)")
        else:
            for r in so5[:35]:
                print(R._fmt(r, "OVER"))
            if len(so5) > 35:
                print(f"  … {len(so5) - 35} more")
        print("Standard UNDER")
        if not su5:
            print("  (none)")
        else:
            for r in su5[:35]:
                print(R._fmt(r, "UNDER"))
            if len(su5) > 35:
                print(f"  … {len(su5) - 35} more")
        print("Goblin OVER")
        if not gob5:
            print("  (none)")
        else:
            hot = [r for r in gob5 if r.get("prop_tier") in ("S", "A")]
            other = [r for r in gob5 if r.get("prop_tier") not in ("S", "A")]
            show = hot + other
            for r in show[:45]:
                print(R._fmt(r, "OVER"))
            if len(show) > 45:
                print(f"  … {len(show) - 45} more")


if __name__ == "__main__":
    main()
