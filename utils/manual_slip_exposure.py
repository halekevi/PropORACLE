#!/usr/bin/env python3
"""Manual / CDP slip exposure — same ledger as mixer + Goblin-70.

Session builds must call ``assert_clears_exposure`` (or
``submit_lineup_guarded``) before clicking PrizePicks Submit. That closes the
Dampier/Overton gap: back-to-back manual slips seed from published
``tickets_latest`` **and** a same-day session register so the second slip
sees the first.
"""

from __future__ import annotations

import json
from datetime import date as date_cls
from pathlib import Path
from typing import Any, Iterable

from utils.slate_exposure_ledger import (
    SlateExposureLedger,
    trim_payload_slate_exposure,
)

_REPO = Path(__file__).resolve().parents[1]
_SESSION_DIR = _REPO / "data" / "cache"


class ExposureBlocked(RuntimeError):
    """Raised when a proposed slip fails the slate exposure ledger."""

    def __init__(self, reason: str):
        self.reason = str(reason or "blocked")
        super().__init__(f"BLOCKED: {self.reason}")


def session_register_path(slate_date: str) -> Path:
    d = str(slate_date or "").strip()[:10]
    return _SESSION_DIR / f"manual_slip_exposure_{d}.json"


def load_published_tickets(slate_date: str) -> dict:
    """Prefer a tickets_latest whose ``date`` matches the slate."""
    d = str(slate_date or "").strip()[:10]
    candidates = [
        _REPO.parent / "PropORACLE_main_cp" / "ui_runner" / "runtime" / "tickets_latest.json",
        _REPO / "ui_runner" / "runtime" / "tickets_latest.json",
        _REPO.parent / "PropORACLE_main_cp" / "ui_runner" / "templates" / "tickets_latest.json",
        _REPO / "ui_runner" / "templates" / "tickets_latest.json",
        _REPO / "data" / "reports" / f"goblin70_tickets_{d}.json",
        _REPO.parent / "PropORACLE_main_cp" / "data" / "reports" / f"goblin70_tickets_{d}.json",
    ]
    dated: dict | None = None
    fallback: dict | None = None
    for path in candidates:
        if not path.is_file():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        if fallback is None:
            fallback = data
        if str(data.get("date") or "")[:10] == d:
            dated = data
            break
    return dated if dated is not None else (fallback or {"groups": []})


def load_session_slips(slate_date: str) -> list[dict[str, Any]]:
    path = session_register_path(slate_date)
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        slips = raw.get("slips") or []
        return [x for x in slips if isinstance(x, dict)]
    return []


def save_session_slip(
    slate_date: str,
    legs: Iterable[dict[str, Any]],
    *,
    explicit_anchor_player: str | None = None,
    ticket_id: str = "",
) -> Path:
    """Append a submitted/accepted manual slip to the same-day session file."""
    path = session_register_path(slate_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    slips = load_session_slips(slate_date)
    slips.append(
        {
            "ticket_id": ticket_id or f"manual-{len(slips)+1}",
            "anchor": explicit_anchor_player or "",
            "legs": [dict(leg) for leg in legs if isinstance(leg, dict)],
        }
    )
    path.write_text(
        json.dumps({"date": str(slate_date)[:10], "slips": slips}, indent=2),
        encoding="utf-8",
    )
    return path


def build_seeded_ledger(slate_date: str) -> SlateExposureLedger:
    """Ledger seeded from published dual card + same-day manual session slips."""
    published = load_published_tickets(slate_date)
    seeded = trim_payload_slate_exposure(dict(published), include_goblin70=True)
    ledger = SlateExposureLedger()
    for g in seeded.get("groups") or []:
        for t in g.get("tickets") or []:
            tlegs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            if tlegs:
                ledger.register(tlegs)
    for slip in load_session_slips(slate_date):
        legs = [leg for leg in (slip.get("legs") or []) if isinstance(leg, dict)]
        if not legs:
            continue
        anchor = str(slip.get("anchor") or "").strip() or None
        ledger.register(legs, explicit_anchor_player=anchor)
    return ledger


def check_slip_exposure(
    legs: Iterable[dict[str, Any]],
    *,
    slate_date: str,
    explicit_anchor_player: str | None = None,
) -> tuple[bool, str]:
    ledger = build_seeded_ledger(slate_date)
    return ledger.can_add(
        list(legs),
        explicit_anchor_player=str(explicit_anchor_player or "").strip() or None,
    )


def assert_clears_exposure(
    legs: Iterable[dict[str, Any]],
    *,
    slate_date: str,
    explicit_anchor_player: str | None = None,
) -> None:
    """Hard gate for manual/CDP submit. Raises ``ExposureBlocked`` on fail."""
    ok, reason = check_slip_exposure(
        legs,
        slate_date=slate_date,
        explicit_anchor_player=explicit_anchor_player,
    )
    if not ok:
        raise ExposureBlocked(reason)


def register_after_submit(
    legs: Iterable[dict[str, Any]],
    *,
    slate_date: str,
    explicit_anchor_player: str | None = None,
    ticket_id: str = "",
) -> Path:
    """Call after a successful Submit so the next manual slip sees this one."""
    return save_session_slip(
        slate_date,
        legs,
        explicit_anchor_player=explicit_anchor_player,
        ticket_id=ticket_id,
    )


def default_slate_date() -> str:
    return date_cls.today().isoformat()
