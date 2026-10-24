"""Re-parse a few past NHL games and compare the shots zero share to the ref db."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from fetch_actuals import parse_nhl_boxscore  # noqa: E402

DB = ROOT / "data" / "cache" / "proporacle_ref.db"


def _get(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "PropORACLE"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main() -> None:
    con = sqlite3.connect(DB)
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT game_date FROM nhl ORDER BY game_date"
    )]
    sample = [dates[0], dates[len(dates) // 2], dates[-1]]
    print("sample dates", sample)
    for day in sample:
        stamp = day.replace("-", "")
        board = _get(
            "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/"
            f"scoreboard?dates={stamp}"
        )
        events = [
            ev for ev in (board.get("events") or [])
            if (ev.get("status") or {}).get("type", {}).get("completed")
        ][:2]
        for ev in events:
            eid = str(ev.get("id"))
            summary = _get(
                "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/"
                f"summary?event={eid}"
            )
            fresh = [
                r for r in parse_nhl_boxscore(summary)
                if r.get("prop_type") == "Shots On Goal"
            ]
            fresh_zero = sum(1 for r in fresh if float(r["actual"]) == 0)
            stored = con.execute(
                """
                SELECT COUNT(*),
                       SUM(CASE WHEN shots_on_goal = 0 THEN 1 ELSE 0 END)
                FROM nhl WHERE event_id = ? AND (position IS NULL OR position != 'G')
                """,
                (eid,),
            ).fetchone()
            print(
                day,
                eid,
                f"fresh {fresh_zero}/{len(fresh)}",
                f"stored zero/n {stored[1]}/{stored[0]}",
            )


if __name__ == "__main__":
    main()
