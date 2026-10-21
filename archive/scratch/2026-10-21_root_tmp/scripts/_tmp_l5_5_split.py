"""Write L5=5 Standards vs Goblins lists for today."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
import rank_best_props_today as R

DATE = "2026-10-20"
ROOTS = [_REPO, _REPO.parent / "PropORACLE_main_cp"]
OUT = _REPO / "outputs" / DATE / "l5_5_standards_vs_goblins.txt"


def l5_5(r: dict) -> bool:
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


def load(sport: str):
    for root in ROOTS:
        if not root.is_dir():
            continue
        try:
            if sport == "NBA":
                df = R.load_nba(root, DATE)
            elif sport == "SOCCER":
                df = R.load_sport(
                    root, DATE, "SOCCER", "soccer", "step8_soccer_direction_clean.xlsx"
                )
            elif sport == "TENNIS":
                df = R.load_sport(
                    root, DATE, "TENNIS", "tennis", "step8_tennis_direction_clean.xlsx"
                )
            else:
                continue
        except Exception:
            continue
        if df is not None and not df.empty:
            return df
    return None


def main() -> None:
    lines: list[str] = [f"L5=5 props {DATE}", ""]
    all_rows: list[dict] = []
    for sport in ("WNBA", "MLB", "SOCCER", "TENNIS", "NBA"):
        if sport in {"WNBA", "MLB"}:
            lines += [f"===== {sport} =====", "(no slate)", ""]
            continue
        df = load(sport)
        if df is None:
            lines += [f"===== {sport} =====", "(no slate)", ""]
            continue
        all_rows.extend(R.recs(df))
        so, su, gob = R.bucket(all_rows, sport)
        so = [r for r in so if l5_5(r)]
        su = [r for r in su if l5_5(r)]
        gob = [r for r in gob if l5_5(r) and not R.skip_earned_runs(r)]
        lines.append(f"===== {sport} =====")
        lines.append("")
        lines.append(f"=== STANDARDS ({len(so) + len(su)}) ===")
        lines.append(f"Standard OVER ({len(so)})")
        if not so:
            lines.append("  (none)")
        else:
            for r in so:
                lines.append(R._fmt(r, "OVER"))
        lines.append(f"Standard UNDER ({len(su)})")
        if not su:
            lines.append("  (none)")
        else:
            for r in su:
                lines.append(R._fmt(r, "UNDER"))
        lines.append("")
        lines.append(f"=== GOBLINS ({len(gob)}) ===")
        lines.append(f"Goblin OVER ({len(gob)})")
        if not gob:
            lines.append("  (none)")
        else:
            hot = [r for r in gob if r.get("prop_tier") in ("S", "A")]
            other = [r for r in gob if r.get("prop_tier") not in ("S", "A")]
            for r in hot + other:
                lines.append(R._fmt(r, "OVER"))
        lines.append("")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"wrote {OUT}")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
