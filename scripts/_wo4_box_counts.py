"""Current-season game counts after the box refresh."""
import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "data" / "cache" / "proporacle_ref.db"
cfb = json.loads((ROOT / "data" / "reports" / "discover_gates_cfb_ranked.json").read_text(encoding="utf-8"))
print("cfb_unique", cfb.get("n_rows_unique"), "dates", (cfb.get("dates") or [None])[:1], (cfb.get("dates") or [None])[-1:])
conn = sqlite3.connect(REF)


def iso(day):
    text = str(day or "").strip()
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


for sport, start in (("nfl", "2026-09-01"), ("cfb", "2026-08-01")):
    by = defaultdict(set)
    for payload in conn.execute(
        "SELECT payload FROM espn_boxscore_rows WHERE sport = ?", (sport,)
    ):
        try:
            row = json.loads(payload[0])
        except (TypeError, json.JSONDecodeError):
            continue
        day = iso(row.get("game_date"))
        if not day or day < start or day >= "2026-10-08":
            continue
        by[str(row.get("player_norm") or "")].add(day)
    counts = Counter(len(days) for days in by.values())
    reached = sum(1 for n in counts.elements() if False)
    with5 = sum(n for k, n in counts.items() if k >= 5)
    print(sport, "players", len(by), "max", max(counts) if counts else None, "with5", with5, "hist", dict(sorted(counts.items())))
conn.close()
