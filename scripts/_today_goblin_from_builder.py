"""Builder board: Goblin legs that survive the season block and the keep gate."""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.postseason_slate import season_blocks_ticket, season_class  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

DATE = "2026-10-08"
INDEX = load_category_hr_index()


def _cell(row: dict) -> bool:
    sport = str(row.get("sport") or "")
    prop = canon_prop(sport, str(row.get("prop") or ""))
    if sport.upper() == "TENNIS" and prop in {"games_won", "match_total_games"}:
        return True
    found = lookup_category_hr(
        INDEX,
        sport=sport,
        prop=row.get("prop"),
        pick_type="Goblin",
        direction=row.get("side") or "OVER",
    )
    return bool(found and found.get("hr") is not None and found.get("n"))


def main() -> None:
    rows = load_today_board(DATE)
    print("board rows", len(rows))
    by = Counter(str(r.get("sport") or "") for r in rows)
    print("sports", dict(by))
    stats: dict[str, Counter] = {}
    for row in rows:
        if str(row.get("pick_type") or "") != "Goblin" or str(row.get("side") or "").upper() != "OVER":
            continue
        sport = str(row.get("sport") or "")
        bucket = stats.setdefault(sport, Counter())
        bucket["goblin"] += 1
        kind = season_class(row) or "ungated_or_unconfirmed"
        bucket[f"season:{kind}"] += 1
        if season_blocks_ticket(row):
            bucket["blocked"] += 1
            continue
        bucket["past_block"] += 1
        if goblin_70_eligible(row):
            bucket["eligible"] += 1
            if _cell(row):
                bucket["eligible_with_cell"] += 1
    for sport in sorted(stats):
        print(sport, dict(stats[sport]))


if __name__ == "__main__":
    main()
