"""Why WNBA 3-PT / Pts+Asts is 79 and 75 in the price file and ~1,575 in the master."""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from all_ranked_prop_hit_rates import graded_paths, pick_book  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402


def _hit(row: dict) -> bool | None:
    res = str(row.get("result") or row.get("grade") or "").upper()
    if res in {"HIT", "WIN", "W"}:
        return True
    if res in {"MISS", "LOSS", "L"}:
        return False
    return None


def main() -> None:
    targets = {"threes", "ptsast"}
    # (prop, month) -> counts
    rows = Counter()
    hits = Counter()
    players = defaultdict(set)
    player_days = defaultdict(set)
    day_lines = defaultdict(set)
    by_month_players = defaultdict(set)
    for path in graded_paths():
        day = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if not str(row.get("sport") or "").upper().startswith("WNBA"):
                continue
            if str(row.get("sport") or "").upper() not in {"WNBA"}:
                # WNBA1H / WNBA1Q stay out of the WNBA cell
                if str(row.get("sport") or "").upper() != "WNBA":
                    continue
            book = pick_book(str(row.get("pick_type") or ""), str(row.get("direction") or ""))
            if book != "Goblin OVER":
                continue
            prop = canon_prop("WNBA", str(row.get("prop") or ""))
            if prop not in targets:
                continue
            hit = _hit(row)
            if hit is None:
                continue
            month = day[:7]
            key = (prop, month)
            rows[key] += 1
            hits[key] += int(hit)
            player = str(row.get("player") or "").casefold()
            players[prop].add(player)
            player_days[prop].add((player, day))
            try:
                line = round(float(row.get("line")), 3)
            except (TypeError, ValueError):
                line = None
            day_lines[prop].add((player, day, line))
            by_month_players[(prop, month)].add(player)

    print("graded files", len(graded_paths()))
    for prop in ("threes", "ptsast"):
        print(f"\n{prop}")
        total_n = total_h = 0
        for month in sorted({m for p, m in rows if p == prop}):
            n = rows[(prop, month)]
            h = hits[(prop, month)]
            total_n += n
            total_h += h
            print(
                f"  {month}: {h}/{n} ({h/n:.1%}) players {len(by_month_players[(prop, month)])}"
            )
        print(
            f"  ALL {total_h}/{total_n} ({(total_h/total_n if total_n else 0):.1%}) "
            f"unique players {len(players[prop])} "
            f"player-days {len(player_days[prop])} "
            f"player-day-lines {len(day_lines[prop])}"
        )

    # Price-file cells
    price = json.loads(
        (ROOT / "data/reports/pp_prop_hit_rates_by_sport.json").read_text(encoding="utf-8")
    )
    print("\nprice file source", price.get("source"), price.get("note"))
    for block in price["sports"]["WNBA"]["props"]:
        if block["prop"] not in {"3-PT Made", "Pts+Asts"}:
            continue
        for cell in block["cells"]:
            if cell["pick_type"] == "Goblin" and cell["direction"] == "OVER":
                print(" price", block["prop"], cell)


if __name__ == "__main__":
    main()
