"""Copy a scraped N-correct payout onto the dated archive ticket.

Live ``tickets_latest.json`` already receives ``payout_source=live_cdp``.
The grade pool, goblin70 archive, and per-run snapshots did not, so a later
grade still priced the slip from the quote. Call this when the scrape is
applied and again at submit when the caller has the scraped multiple.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from utils.payout_ticket_sig import ambiguous_sig_keys, ticket_payout_sig


def _iter_tickets(data: dict) -> list[dict]:
    out: list[dict] = []
    if isinstance(data.get("tickets"), list):
        for t in data["tickets"]:
            if isinstance(t, dict):
                out.append(t)
    for g in data.get("groups") or []:
        if not isinstance(g, dict):
            continue
        for t in g.get("tickets") or []:
            if isinstance(t, dict):
                out.append(t)
    return out


def archive_ticket_paths(root: Path, date_str: str) -> list[Path]:
    """Dated copies a grade will still read after the live board moves on."""
    d = str(date_str or "").strip()[:10]
    if len(d) != 10:
        return []
    found: list[Path] = []
    g70 = root / "data" / "reports" / f"goblin70_tickets_{d}.json"
    if g70.is_file():
        found.append(g70)
    pool = root / "ui_runner" / "data" / f"combined_slate_tickets_{d}.json"
    if pool.is_file():
        found.append(pool)
    runs = root / "ui_runner" / "data" / "ticket_runs" / d
    if runs.is_dir():
        found.extend(sorted(p for p in runs.glob("*/tickets.json") if p.is_file()))
    return found


def _min_x(entry: dict) -> float | None:
    """Scraped N-correct multiple. Quotes and blank sources do not stamp."""
    if str(entry.get("payout_source") or "").strip().lower() != "live_cdp":
        return None
    try:
        val = float(entry.get("power_min_x") or entry.get("display_min_x") or 0)
    except (TypeError, ValueError):
        return None
    if val <= 0:
        return None
    return val


def _paint(ticket: dict, entry: dict, min_x: float) -> bool:
    pay = ticket.get("payout") if isinstance(ticket.get("payout"), dict) else {}
    pay = dict(pay)
    already = str(pay.get("payout_source") or ticket.get("payout_source") or "").strip().lower()
    try:
        have = float(pay.get("power_min_x") or ticket.get("power_min_x") or 0)
    except (TypeError, ValueError):
        have = 0.0
    if already == "live_cdp" and abs(have - min_x) < 1e-6:
        return False
    pay["power_min_x"] = min_x
    pay["display_min_x"] = min_x
    pay["payout_source"] = "live_cdp"
    if entry.get("captured_at"):
        pay["captured_at"] = entry["captured_at"]
    if entry.get("power_first_x") is not None:
        pay["power_first_x"] = entry["power_first_x"]
    ticket["payout"] = pay
    ticket["payout_source"] = "live_cdp"
    ticket["power_min_x"] = min_x
    ticket["display_min_x"] = min_x
    return True


def stamp_scraped_payouts_on_archives(
    date_str: str,
    patch: dict[str, Any],
    *,
    root: Path,
) -> int:
    """Write each live_cdp patch row onto matching tickets in that date's archives.

    Ticket id wins. A leg signature is used only when it is unique in that file.
    Returns the number of ticket objects updated.
    """
    by_id = patch.get("by_ticket_id") if isinstance(patch.get("by_ticket_id"), dict) else {}
    entries = [e for e in by_id.values() if isinstance(e, dict) and _min_x(e) is not None]
    if not entries:
        return 0
    n = 0
    for path in archive_ticket_paths(root, date_str):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue
        tickets = _iter_tickets(data)
        ambiguous = ambiguous_sig_keys(tickets)
        changed = False
        for entry in entries:
            min_x = _min_x(entry)
            if min_x is None:
                continue
            tid = str(entry.get("ticket_id") or "").strip()
            sig = ticket_payout_sig(
                entry,
                legs=entry.get("legs") if isinstance(entry.get("legs"), list) else None,
                product=str(entry.get("play") or entry.get("product") or ""),
                n_legs=entry.get("n_legs"),
            )
            hits: list[dict] = []
            for t in tickets:
                if tid and str(t.get("ticket_id") or "").strip() == tid:
                    hits = [t]
                    break
            if not hits and sig and sig not in ambiguous:
                hits = [t for t in tickets if ticket_payout_sig(t) == sig]
                if len(hits) != 1:
                    hits = []
            for t in hits:
                if _paint(t, entry, min_x):
                    changed = True
                    n += 1
        if changed:
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return n
