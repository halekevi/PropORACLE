#!/usr/bin/env python3
"""Shared Goblin-70 rebuild / scrub health stamps under logs/."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
LOGS = _REPO / "logs"
ET = ZoneInfo("America/New_York")


def _mirror(name: str, text: str) -> None:
    for peer in _REPO.parent.glob("PropORACLE*"):
        if not peer.is_dir() or peer.resolve() == _REPO.resolve():
            continue
        try:
            peer_logs = peer / "logs"
            peer_logs.mkdir(parents=True, exist_ok=True)
            (peer_logs / name).write_text(text, encoding="utf-8")
        except OSError:
            pass


def stamp_g70_rebuild(
    *,
    ok: bool,
    slate_date: str,
    detail: dict | None = None,
    source: str = "build_goblin70_tickets",
) -> Path:
    """Write logs/LAST_G70_REBUILD.json after a full dual-card --write-web."""
    LOGS.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    payload = {
        "ok": bool(ok),
        "kind": "g70_rebuild",
        "source": source,
        "slate_date": str(slate_date or "")[:10],
        "rebuilt_at_utc": now_utc.isoformat(),
        "rebuilt_at_et": datetime.now(ET).isoformat(),
        "detail": detail or {},
    }
    text = json.dumps(payload, indent=2)
    path = LOGS / "LAST_G70_REBUILD.json"
    path.write_text(text, encoding="utf-8")
    (LOGS / "LAST_G70_REBUILD.txt").write_text(
        f"ok={payload['ok']} slate={payload['slate_date']} "
        f"et={payload['rebuilt_at_et']} source={source}\n",
        encoding="utf-8",
    )
    _mirror("LAST_G70_REBUILD.json", text)
    _mirror(
        "LAST_G70_REBUILD.txt",
        f"ok={payload['ok']} slate={payload['slate_date']} "
        f"et={payload['rebuilt_at_et']} source={source}\n",
    )
    # Successful rebuild clears the stale alert.
    if ok:
        for name in ("LAST_G70_STALE.alert", "LAST_G70_STALE.txt"):
            p = LOGS / name
            if p.is_file():
                try:
                    p.unlink()
                except OSError:
                    pass
    return path
