"""Look for a per-leg PrizePicks result next to Saves / Shots / SOT legs."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path

DB = Path(r"H:\PropORACLE\data\db\MyTicketPerformance.db")


def main() -> None:
    conn = sqlite3.connect(str(DB))
    print(
        "entry_leg_results cols",
        [r[1] for r in conn.execute("PRAGMA table_info(entry_leg_results)")],
    )
    rows = conn.execute(
        """
        SELECT l.player_name, l.stat_type, l.line, l.description, l.league,
               e.status, e.raw_json, r.result, r.actual_value, r.source
        FROM entry_legs l
        LEFT JOIN user_entries e ON e.entry_id = l.entry_id
        LEFT JOIN entry_leg_results r
          ON r.entry_id = l.entry_id AND r.leg_index = l.leg_index
        WHERE lower(coalesce(l.league,'')) LIKE '%soc%'
           OR lower(coalesce(l.stat_type,'')) LIKE '%save%'
           OR lower(coalesce(l.stat_type,'')) LIKE '%shot%'
        """
    ).fetchall()
    print("joined rows", len(rows))
    our = Counter(r[8] for r in rows)
    print("our result", our)
    # Does any raw_json contain a prediction result string?
    has_pred_body = 0
    entry_result = Counter()
    for player, stat, line, desc, league, status, raw, result, actual, source in rows:
        if not raw:
            entry_result["no_raw"] += 1
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            entry_result["bad_json"] += 1
            continue
        attrs = (payload.get("attributes") or {}) if isinstance(payload, dict) else {}
        entry_result[str(attrs.get("result") or status or "")] += 1
        rel = (payload.get("relationships") or {}).get("predictions") or {}
        data = rel.get("data") if isinstance(rel, dict) else None
        if data and isinstance(data, list) and data and isinstance(data[0], dict) and "attributes" in data[0]:
            has_pred_body += 1
    print("entry-level result", entry_result)
    print("payloads with prediction attributes", has_pred_body)
    # description values
    desc = Counter((r[3] or "")[:40] for r in rows)
    print("descriptions", desc.most_common(12))
    print("sources", Counter(r[9] for r in rows))
    # how many of these have our HIT/MISS
    decided = [r for r in rows if r[8] in ("HIT", "MISS")]
    print("our decided", len(decided))
    for r in decided[:12]:
        print(" ", r[0], r[1], r[2], r[8], r[7], r[4])


if __name__ == "__main__":
    main()
