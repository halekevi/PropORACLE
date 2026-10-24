#!/usr/bin/env python3
"""Inventory early-October NBA1Q leftover-board slips and whether any were placed.

  py -3.14 scripts/report_stale_nba1q_tickets.py
  py -3.14 scripts/report_stale_nba1q_tickets.py --from 2026-10-01 --to 2026-10-22
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT / "scripts"))

from ticket_leg_index import iter_ticket_json_paths, ticket_search_roots  # noqa: E402
from utils.stale_board_tickets import (  # noqa: E402
    NBA1Q_REAL_BOARD_FROM,
    annotate_stale_board_ticket,
    ticket_nba1q_legs,
    ticket_stale_board_reason,
)


def _daterange(lo: str, hi: str) -> list[str]:
    cur = date.fromisoformat(lo[:10])
    end = date.fromisoformat(hi[:10])
    out: list[str] = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _walk_tickets(payload: dict[str, Any]):
    for grp in payload.get("groups") or []:
        if not isinstance(grp, dict):
            continue
        gname = str(grp.get("group_name") or "")
        for t in grp.get("tickets") or []:
            if isinstance(t, dict):
                yield gname, t
    for t in payload.get("tickets") or []:
        if isinstance(t, dict):
            yield str(t.get("web_group_name") or t.get("group_name") or ""), t


def _accounts_placed(dates: set[str]) -> list[dict[str, Any]]:
    try:
        from ui_runner.account_store import accounts_db_path
    except Exception:
        return []
    path = accounts_db_path()
    if not path.is_file():
        return []
    try:
        con = sqlite3.connect(str(path))
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT user_id, slate_date, fingerprint, placed_at, snapshot FROM placed_slips"
        ).fetchall()
        con.close()
    except Exception:
        return []
    out = []
    for r in rows:
        ds = str(r["slate_date"] or "")[:10]
        if ds not in dates:
            continue
        snap = {}
        raw = r["snapshot"]
        if raw:
            try:
                snap = json.loads(raw) if isinstance(raw, str) else {}
            except json.JSONDecodeError:
                snap = {}
        sports = {
            str(leg.get("sport") or "").upper()
            for leg in (snap.get("legs") or [])
            if isinstance(leg, dict)
        }
        if "NBA1Q" not in sports and not any("NBA1Q" in str(x).upper() for x in sports):
            # Keep rows on those dates anyway if snapshot missing — inspect later.
            if sports:
                continue
        out.append(
            {
                "user_id": r["user_id"],
                "slate_date": ds,
                "fingerprint": r["fingerprint"],
                "placed_at": r["placed_at"],
                "sports": sorted(sports),
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="date_from", default="2026-10-01")
    ap.add_argument("--to", dest="date_to", default="2026-10-22")
    args = ap.parse_args()

    by_date: dict[str, dict[str, Any]] = {}
    pairing = Counter()
    placed_json = 0
    on_card = 0
    stale_n = 0
    files_hit: set[str] = set()

    for ds in _daterange(args.date_from, args.date_to):
        rec = {
            "date": ds,
            "nba1q_tickets": 0,
            "stale_board": 0,
            "json_placed_true": 0,
            "files": 0,
            "sample_matchups": [],
        }
        seen_tid: set[str] = set()
        for path in iter_ticket_json_paths(ds, ROOT):
            rec["files"] += 1
            files_hit.add(str(path))
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            for gname, t in _walk_tickets(payload):
                q_legs = ticket_nba1q_legs(t, group_name=gname)
                if not q_legs:
                    continue
                tid = str(t.get("ticket_id") or t.get("id") or "")
                key = tid or json.dumps(
                    [(x.get("player"), x.get("prop_type") or x.get("prop"), x.get("line")) for x in q_legs],
                    sort_keys=True,
                )
                if key in seen_tid:
                    continue
                seen_tid.add(key)
                rec["nba1q_tickets"] += 1
                on_card += 1
                reason = ticket_stale_board_reason(t, slate_date=ds, group_name=gname)
                if reason:
                    rec["stale_board"] += 1
                    stale_n += 1
                    annotate_stale_board_ticket(t, slate_date=ds, group_name=gname)
                if t.get("placed") is True:
                    rec["json_placed_true"] += 1
                    placed_json += 1
                teams = sorted(
                    {
                        str(leg.get("team") or "").strip().upper()
                        for leg in q_legs
                        if str(leg.get("team") or "").strip()
                    }
                )
                opps = sorted(
                    {
                        str(leg.get("opp") or leg.get("opp_team") or "").strip().upper()
                        for leg in q_legs
                        if str(leg.get("opp") or leg.get("opp_team") or "").strip()
                    }
                )
                if teams or opps:
                    label = f"{'/'.join(teams)}@{('/'.join(opps))}"
                    pairing[label] += 1
                    if label not in rec["sample_matchups"] and len(rec["sample_matchups"]) < 4:
                        rec["sample_matchups"].append(label)
        by_date[ds] = rec

    acc_dates = {d for d, r in by_date.items() if r["stale_board"] or d < NBA1Q_REAL_BOARD_FROM}
    account_placed = _accounts_placed(acc_dates)

    report = {
        "real_board_from": NBA1Q_REAL_BOARD_FROM,
        "invalid_reason": "stale_board",
        "not": "VOID",
        "nba1q_tickets": on_card,
        "stale_board": stale_n,
        "json_placed_true": placed_json,
        "account_placed_nba1q_rows": account_placed,
        "matchups": pairing.most_common(12),
        "search_roots": [str(p) for p in ticket_search_roots(ROOT)],
        "by_date": by_date,
        "note": (
            "json_placed_true is the Goblin-70 EV 'placed' flag (on-card), not a "
            "PrizePicks submit. account_placed_nba1q_rows is the website checkbox. "
            "If PrizePicks had no OKC@SAS 1Q lines in early October, they could not "
            "have been submitted there."
        ),
    }
    out = ROOT / "data" / "reports" / "stale_nba1q_tickets_2026-10.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"NBA1Q tickets {on_card}  stale_board {stale_n}  json placed=true {placed_json}")
    print(f"account placed rows (NBA1Q-ish on stale dates): {len(account_placed)}")
    print(f"matchups: {pairing.most_common(6)}")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
