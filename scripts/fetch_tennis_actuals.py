#!/usr/bin/env python3
"""
Build actuals_tennis_YYYY-MM-DD.csv for combined_ticket_grader (ESPN ATP/WTA finals).

Columns: player, team, prop_type, actual
(team left blank so grader matches on player + prop_norm; optional future use.)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
_TENNIS_SCRIPTS = ROOT / "Sports" / "Tennis" / "scripts"
if str(_TENNIS_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_TENNIS_SCRIPTS))

from tennis_shared import iter_scoreboard_matches  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD (UTC date of match start from ESPN)")
    ap.add_argument("--output", required=True, help="Path to actuals_tennis_YYYY-MM-DD.csv")
    args = ap.parse_args()

    target = str(args.date).strip()[:10]
    rows: list[dict[str, object]] = []

    for tour in ("ATP", "WTA"):
        for m in iter_scoreboard_matches(tour):
            dt = str(m.get("match_date_utc") or "")[:10]
            if dt != target:
                continue
            pl = str(m.get("player") or "").strip()
            if not pl:
                continue
            def _opt_f(key: str):
                v = m.get(key)
                if v is None or v == "":
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            gw = _opt_f("games_won")
            mt = _opt_f("match_total_games")
            ac = _opt_f("aces")
            dfa = _opt_f("double_faults")
            sw = _opt_f("sets_won")
            # Always emit score props when present; serve stats only when known
            # (missing aces must stay absent — 0 was grading every Goblin Aces as MISS).
            for prop_type, actual in (
                ("games_won", gw if gw is not None else 0.0),
                ("match_total_games", mt if mt is not None else 0.0),
                ("sets won", sw if sw is not None else 0.0),
            ):
                rows.append(
                    {"player": pl, "team": "", "prop_type": prop_type, "actual": actual}
                )
            if ac is not None:
                rows.append(
                    {"player": pl, "team": "", "prop_type": "aces", "actual": ac}
                )
            if dfa is not None:
                rows.append(
                    {
                        "player": pl,
                        "team": "",
                        "prop_type": "double faults",
                        "actual": dfa,
                    }
                )

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"OK [tennis-actuals] date={target} rows={len(rows)} -> {out}")


if __name__ == "__main__":
    main()
