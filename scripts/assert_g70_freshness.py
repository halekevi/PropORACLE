#!/usr/bin/env python3
"""Alert when Goblin-70 has not successfully rebuilt within N hours.

The keep-scrub pass shrinks stale-leg exposure; this flag catches the other
failure mode — the scheduled rebuild chain going quiet with nobody noticing.

  py -3.14 scripts/assert_g70_freshness.py
  py -3.14 scripts/assert_g70_freshness.py --max-hours 6

Exit 0 = fresh enough. Exit 2 = stale / missing stamp (scheduler-visible).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
LOGS = _REPO / "logs"
ET = ZoneInfo("America/New_York")
STAMP_NAME = "LAST_G70_REBUILD.json"
ALERT_NAME = "LAST_G70_STALE.alert"


def _parse_ts(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _load_stamp(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_alert(payload: dict) -> Path:
    LOGS.mkdir(parents=True, exist_ok=True)
    path = LOGS / ALERT_NAME
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    txt = LOGS / "LAST_G70_STALE.txt"
    lines = [
        f"G70 STALE — {payload.get('reason')}",
        f"checked_at_et={payload.get('checked_at_et')}",
        f"last_rebuild_et={payload.get('last_rebuild_et')}",
        f"age_hours={payload.get('age_hours')}",
        f"max_hours={payload.get('max_hours')}",
        f"stamp={payload.get('stamp_path')}",
    ]
    txt.write_text("\n".join(lines) + "\n", encoding="utf-8")
    for peer in _REPO.parent.glob("PropORACLE*"):
        if not peer.is_dir() or peer.resolve() == _REPO.resolve():
            continue
        try:
            peer_logs = peer / "logs"
            peer_logs.mkdir(parents=True, exist_ok=True)
            (peer_logs / ALERT_NAME).write_text(json.dumps(payload, indent=2), encoding="utf-8")
            (peer_logs / "LAST_G70_STALE.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
        except OSError:
            pass
    return path


def _clear_alert() -> None:
    for name in (ALERT_NAME, "LAST_G70_STALE.txt"):
        p = LOGS / name
        if p.is_file():
            try:
                p.unlink()
            except OSError:
                pass


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--max-hours",
        type=float,
        default=6.0,
        help="Fail when last successful G70 rebuild is older than this (default 6).",
    )
    ap.add_argument(
        "--stamp",
        type=Path,
        default=None,
        help="Override path to LAST_G70_REBUILD.json",
    )
    args = ap.parse_args()

    stamp_path = args.stamp or (LOGS / STAMP_NAME)
    # Prefer sibling main_cp stamp when this worktree has never rebuilt.
    if not stamp_path.is_file():
        alt = _REPO.parent / "PropORACLE_main_cp" / "logs" / STAMP_NAME
        if alt.is_file():
            stamp_path = alt

    now = datetime.now(timezone.utc)
    checked = {
        "checked_at_utc": now.isoformat(),
        "checked_at_et": datetime.now(ET).isoformat(),
        "max_hours": float(args.max_hours),
        "stamp_path": str(stamp_path),
    }
    stamp = _load_stamp(stamp_path)
    if not stamp or not stamp.get("ok"):
        payload = {
            **checked,
            "ok": False,
            "reason": "missing_or_failed_rebuild_stamp",
            "last_rebuild_et": None,
            "age_hours": None,
            "stamp": stamp,
        }
        alert = _write_alert(payload)
        print(f"G70 STALE: {payload['reason']} -> {alert}")
        return 2

    rebuilt = _parse_ts(stamp.get("rebuilt_at_utc") or stamp.get("rebuilt_at_et"))
    if rebuilt is None:
        payload = {
            **checked,
            "ok": False,
            "reason": "unparseable_rebuild_timestamp",
            "stamp": stamp,
            "age_hours": None,
            "last_rebuild_et": stamp.get("rebuilt_at_et"),
        }
        alert = _write_alert(payload)
        print(f"G70 STALE: {payload['reason']} -> {alert}")
        return 2

    age_h = (now - rebuilt).total_seconds() / 3600.0
    last_et = rebuilt.astimezone(ET).isoformat()
    if age_h > float(args.max_hours):
        payload = {
            **checked,
            "ok": False,
            "reason": "rebuild_older_than_max_hours",
            "age_hours": round(age_h, 2),
            "last_rebuild_et": last_et,
            "slate_date": stamp.get("slate_date"),
            "stamp": stamp,
        }
        alert = _write_alert(payload)
        print(
            f"G70 STALE: last rebuild {last_et} ({age_h:.1f}h ago) "
            f"> max {args.max_hours}h -> {alert}"
        )
        return 2

    _clear_alert()
    print(
        f"G70 fresh: last rebuild {last_et} ({age_h:.1f}h ago) "
        f"<= max {args.max_hours}h slate={stamp.get('slate_date')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
