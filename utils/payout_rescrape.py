"""Plan which tickets need a PrizePicks N-correct scrape.

Initial fetch of the day: caller Force-scrapes every MAIN/STRONG slip.
Later refreshes: re-scrape slips that are still missing live_cdp, plus slips
whose legs match line_events (line moved or pick type cut/changed).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable

from utils.bet_windows import LINE_DB, parse_et

ROOT = Path(__file__).resolve().parents[1]


def fold_name(raw: object) -> str:
    return " ".join(str(raw or "").strip().lower().split())


def prop_key(player: object, prop_type: object) -> tuple[str, str]:
    return (fold_name(player), fold_name(prop_type))


def initial_scrape_flag_path(date: str, *, repo: Path | None = None) -> Path:
    root = repo or ROOT
    return (root / "data" / "cache") / f"payout_initial_ok_{str(date or '')[:10]}.flag"


def ticket_has_live_cdp(ticket: dict[str, Any]) -> bool:
    pay = ticket.get("payout") if isinstance(ticket.get("payout"), dict) else {}
    src = str(pay.get("payout_source") or ticket.get("payout_source") or "").strip().lower()
    if src != "live_cdp":
        return False
    for raw in (
        pay.get("power_min_x"),
        pay.get("display_min_x"),
        ticket.get("display_min_x"),
    ):
        try:
            if float(raw) > 0:
                return True
        except (TypeError, ValueError):
            continue
    return False


def iter_tickets(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for group in payload.get("groups") or []:
        if not isinstance(group, dict):
            continue
        for ticket in group.get("tickets") or []:
            if isinstance(ticket, dict):
                yield ticket


def missing_live_ticket_ids(payload: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for ticket in iter_tickets(payload):
        tid = str(ticket.get("ticket_id") or "").strip()
        if not tid:
            continue
        legs = ticket.get("legs") or []
        if not isinstance(legs, list) or len(legs) < 2:
            continue
        if not ticket_has_live_cdp(ticket):
            out.append(tid)
    return out


def changed_prop_keys(
    date: str,
    *,
    since: str | None = None,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    """line_events rows for this slate whose line or pick type changed."""
    path = Path(db_path) if db_path else LINE_DB
    if not path.is_file():
        return []
    date_s = str(date or "")[:10]
    if not date_s:
        return []
    since_dt = parse_et(since) if since else None
    rows: list[dict[str, Any]] = []
    try:
        with sqlite3.connect(str(path)) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                """
                SELECT sport, player, prop_type, event, prev_pick, new_pick,
                       prev_line, new_line, fetched_at, game_date
                FROM line_events
                WHERE event IN ('moved', 'cut')
                  AND (
                    CAST(game_date AS TEXT) = ?
                    OR substr(CAST(fetched_at AS TEXT), 1, 10) = ?
                  )
                """,
                (date_s, date_s),
            )
            for rec in cur:
                fetched = str(rec["fetched_at"] or "")
                if since_dt is not None:
                    got = parse_et(fetched)
                    if got is None or got < since_dt:
                        continue
                player = str(rec["player"] or "").strip()
                prop = str(rec["prop_type"] or "").strip()
                if not player or not prop:
                    continue
                rows.append(
                    {
                        "sport": str(rec["sport"] or ""),
                        "player": player,
                        "prop_type": prop,
                        "event": str(rec["event"] or ""),
                        "prev_pick": str(rec["prev_pick"] or ""),
                        "new_pick": str(rec["new_pick"] or ""),
                        "prev_line": rec["prev_line"],
                        "new_line": rec["new_line"],
                        "fetched_at": fetched,
                    }
                )
    except sqlite3.Error:
        return []
    return rows


def ticket_ids_for_props(
    payload: dict[str, Any],
    props: Iterable[dict[str, Any]],
) -> list[str]:
    keys = {prop_key(p.get("player"), p.get("prop_type")) for p in props}
    keys.discard(("", ""))
    if not keys:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for ticket in iter_tickets(payload):
        tid = str(ticket.get("ticket_id") or "").strip()
        if not tid or tid in seen:
            continue
        for leg in ticket.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            if prop_key(leg.get("player") or leg.get("player_name"), leg.get("prop_type") or leg.get("prop")) in keys:
                seen.add(tid)
                out.append(tid)
                break
    return out


def load_tickets_payload(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def plan_rescrape(
    tickets_path: Path,
    date: str,
    *,
    since: str | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    payload = load_tickets_payload(tickets_path)
    changed = changed_prop_keys(date, since=since, db_path=db_path)
    changed_ids = ticket_ids_for_props(payload, changed)
    missing_ids = missing_live_ticket_ids(payload)
    ids: list[str] = []
    seen: set[str] = set()
    for tid in changed_ids + missing_ids:
        if tid in seen:
            continue
        seen.add(tid)
        ids.append(tid)
    return {
        "date": str(date or "")[:10],
        "tickets_path": str(tickets_path),
        "since": since or "",
        "n_changed_props": len(changed),
        "n_changed_tickets": len(changed_ids),
        "n_missing_live": len(missing_ids),
        "n_scrape": len(ids),
        "ticket_ids": ids,
        "changed_sample": changed[:12],
        "skip_scrape": len(ids) == 0,
        "reason": (
            "no line/type changes and all slips have live_cdp"
            if not ids
            else "missing_live_or_changed_props"
        ),
    }
