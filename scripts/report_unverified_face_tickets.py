#!/usr/bin/env python3
"""Ladder check over the ticket archive: which Standard-labeled legs were real faces?

NFL step1 never stored pick_type before 2026-10-23, so every Goblin / Demon
rung graded as Standard. A published "Standard UNDER sacks 1.5" on such a day
may have been a Demon rung (UNDER never offered) or a Goblin rung (priced at
Standard 3x when the real payout was lower). Same shape as the stale NBA1Q
slips: tag, do not VOID.

  py -3.14 scripts/report_unverified_face_tickets.py --sport NFL
  py -3.14 scripts/report_unverified_face_tickets.py --sport NFL --from 2026-09-04 --to 2026-10-23 --stamp

--stamp rewrites the archived ticket JSONs in place, adding
invalid / invalid_reason=unverified_face on tickets with a rung leg and
face_verdict / face_unverified on every checked leg. Nothing is deleted.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "scripts"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from ticket_leg_index import iter_ticket_json_paths  # noqa: E402
from utils.face_ladder import (  # noqa: E402
    INVALID_REASON,
    RUNG_VERDICTS,
    annotate_unverified_face_ticket,
    classify_leg_face,
    face_status_by_sport_date,
    ladder_index,
)
from utils.prop_norm import canon_prop  # noqa: E402


def _daterange(lo: str, hi: str) -> list[str]:
    cur, end = date.fromisoformat(lo[:10]), date.fromisoformat(hi[:10])
    out = []
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _walk(payload: dict[str, Any]):
    for grp in payload.get("groups") or []:
        if isinstance(grp, dict):
            for t in grp.get("tickets") or []:
                if isinstance(t, dict):
                    yield str(grp.get("group_name") or grp.get("sport") or ""), t
    for t in payload.get("tickets") or []:
        if isinstance(t, dict):
            yield str(t.get("web_group_name") or t.get("group_name") or ""), t


def _sport_legs(ticket: dict[str, Any], sport: str, group_sport: str) -> list[dict[str, Any]]:
    out = []
    for leg in ticket.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        sp = str(leg.get("sport") or group_sport or "").strip().upper()
        if sp == sport:
            out.append(leg)
    return out


def _accounts_placed(sport: str, dates: set[str]) -> list[dict[str, Any]]:
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
        rows = con.execute("SELECT user_id, slate_date, fingerprint, placed_at, snapshot FROM placed_slips").fetchall()
        con.close()
    except Exception:
        return []
    out = []
    for r in rows:
        ds = str(r["slate_date"] or "")[:10]
        if ds not in dates:
            continue
        try:
            snap = json.loads(r["snapshot"]) if isinstance(r["snapshot"], str) else {}
        except json.JSONDecodeError:
            snap = {}
        legs = [l for l in (snap.get("legs") or []) if isinstance(l, dict)]
        if not any(str(l.get("sport") or "").upper() == sport for l in legs):
            continue
        out.append(
            {
                "user_id": r["user_id"],
                "slate_date": ds,
                "fingerprint": r["fingerprint"],
                "placed_at": r["placed_at"],
                "legs": [
                    f"{l.get('player')} {l.get('prop_type') or l.get('prop')} {l.get('direction')} {l.get('line')} [{l.get('pick_type')}]"
                    for l in legs
                ],
            }
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sport", default="NFL")
    ap.add_argument("--from", dest="date_from", default="2026-09-04")
    ap.add_argument("--to", dest="date_to", default="2026-10-23")
    ap.add_argument("--stamp", action="store_true", help="Rewrite archived ticket JSONs with the tags")
    args = ap.parse_args()
    sport = args.sport.strip().upper()

    from proporacle.data.leg_ledger import current_legs

    legs = current_legs()
    legs = legs[legs["sport"].astype(str).str.upper() == sport].copy()
    status = face_status_by_sport_date(legs)
    status_by_date = {str(r.slate_date): str(r.status) for r in status.itertuples(index=False)} if not status.empty else {}
    ladders: dict[str, dict] = {}
    for ds, sub in legs.groupby(legs["slate_date"].astype(str).str[:10]):
        rows = sub[["player", "prop_canon", "line"]].rename(columns={"prop_canon": "prop"}).to_dict("records")
        ladders[ds] = ladder_index(rows)

    verdict_counts: Counter = Counter()
    by_date: dict[str, dict[str, Any]] = {}
    stamped_files: set[str] = set()
    flagged_tickets: list[dict[str, Any]] = []
    seen_tid: set[str] = set()
    for ds in _daterange(args.date_from, args.date_to):
        dstat = status_by_date.get(ds, "no_graded_rows")
        rec = {"date": ds, "date_status": dstat, "tickets": 0, "legs": 0, "rung_tickets": 0, "verdicts": Counter()}
        for path in iter_ticket_json_paths(ds, ROOT):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if path.name == "tickets_latest.json":
                # Live card: the builder owns it. Archive runs carry the same tickets.
                continue
            changed = False
            for gname, t in _walk(payload):
                s_legs = _sport_legs(t, sport, str(t.get("sport") or ""))
                if not s_legs:
                    continue
                tid = str(t.get("ticket_id") or t.get("id") or "")
                key = f"{path}|{tid}" if tid else f"{path}|{json.dumps([(l.get('player'), l.get('line')) for l in s_legs])}"
                first_seen = key not in seen_tid
                seen_tid.add(key)
                verdicts = []
                for leg in t.get("legs") or []:
                    if not isinstance(leg, dict) or leg not in s_legs:
                        verdicts.append({"verdict": "other_sport"})
                        continue
                    prop = canon_prop(sport, leg.get("prop_type") or leg.get("prop")) or str(leg.get("prop_type") or "").lower()
                    from utils.face_ladder import _fold  # local helper

                    lines = ladders.get(ds, {}).get((_fold(leg.get("player")), prop))
                    v = classify_leg_face(leg, lines, date_status=dstat)
                    verdicts.append(v)
                    if first_seen:
                        verdict_counts[v["verdict"]] += 1
                        rec["verdicts"][v["verdict"]] += 1
                        rec["legs"] += 1
                if first_seen:
                    rec["tickets"] += 1
                rung = [v for v in verdicts if v.get("verdict") in RUNG_VERDICTS]
                if rung:
                    if first_seen:
                        rec["rung_tickets"] += 1
                        flagged_tickets.append(
                            {
                                "date": ds,
                                "file": str(path),
                                "group": gname,
                                "ticket_id": tid,
                                "ticket_track": t.get("ticket_track"),
                                "placed": t.get("placed"),
                                "result": t.get("result"),
                                "legs": [
                                    {
                                        "player": l.get("player"),
                                        "prop": l.get("prop_type") or l.get("prop"),
                                        "direction": l.get("direction"),
                                        "line": l.get("line"),
                                        "pick_type": l.get("pick_type"),
                                        "verdict": v.get("verdict"),
                                        "evidence": v.get("evidence"),
                                    }
                                    for l, v in zip(t.get("legs") or [], verdicts)
                                    if v.get("verdict") != "other_sport"
                                ],
                            }
                        )
                if args.stamp:
                    before = json.dumps(t, sort_keys=True, default=str)
                    annotate_unverified_face_ticket(t, verdicts)
                    if json.dumps(t, sort_keys=True, default=str) != before:
                        changed = True
            if args.stamp and changed:
                path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                stamped_files.add(str(path))
        rec["verdicts"] = dict(rec["verdicts"])
        if rec["tickets"]:
            by_date[ds] = rec

    dates = set(by_date)
    account_rows = _accounts_placed(sport, dates)
    report = {
        "sport": sport,
        "invalid_reason": INVALID_REASON,
        "not": "VOID",
        "window": [args.date_from, args.date_to],
        "verdicts": dict(verdict_counts),
        "tickets_checked": sum(r["tickets"] for r in by_date.values()),
        "legs_checked": sum(r["legs"] for r in by_date.values()),
        "rung_tickets": sum(r["rung_tickets"] for r in by_date.values()),
        "stamped": bool(args.stamp),
        "stamped_files": sorted(stamped_files),
        "account_placed_rows": account_rows,
        "date_status": {ds: status_by_date.get(ds) for ds in sorted(dates)},
        "by_date": by_date,
        "flagged_tickets": flagged_tickets,
        "note": (
            "date_status default_labels = graded set had zero Goblin/Demon that day (pick_type defaulted). "
            "Verdicts come from the ladder of lines seen for that player-prop-date: min of 3+ = Goblin rung, "
            "max of 3+ = Demon rung, 2 rungs without a cross-book line = ambiguous. A single line stays valid "
            "but face_unverified. 'placed' is the Goblin-70 EV flag, not a PrizePicks submit; "
            "account_placed_rows is the website checkbox."
        ),
    }
    out = ROOT / "data" / "reports" / f"unverified_face_tickets_{sport.lower()}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
    print(f"{sport} tickets {report['tickets_checked']} legs {report['legs_checked']} rung tickets {report['rung_tickets']}")
    print(f"verdicts {dict(verdict_counts)}")
    print(f"account placed rows with {sport} legs on those dates: {len(account_rows)}")
    if args.stamp:
        print(f"stamped {len(stamped_files)} file(s)")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
