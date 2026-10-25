"""Hard gate: snap / target caches must cover the last completed NFL week.

A Tuesday reminder will be forgotten. A failing step will not.
``cache_through_week >= completed_week``. Ahead (TNF of the current week
already in nflverse) is allowed. Behind is ``NflVolumeCacheStale``.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from utils.nfl_cross_season_l5 import nfl_season_start, nfl_week1_thursday

_REPO = Path(__file__).resolve().parents[1]
_DATA = _REPO / "Sports" / "NFL" / "data"
ROUTE_META = _DATA / "nfl_route_participation_meta.json"
SNAP_CACHE = _DATA / "nfl_snap_pct_cache.json"


class NflVolumeCacheStale(RuntimeError):
    """Snap or target cache is behind the last completed regular-season week."""


def infer_season(as_of: date | None = None) -> int:
    d = as_of or datetime.now(timezone.utc).date()
    return d.year if d.month >= 9 else d.year - 1


def calendar_completed_week(as_of: date | None = None) -> int | None:
    """Last fully completed NFL regular-season week, or None in the offseason.

    A week completes on the Tuesday after that week's Monday night game.
    Sunday of week N still counts as N-1 completed.
    """
    d = as_of or datetime.now(timezone.utc).date()
    if 3 <= d.month <= 7:
        return None
    start = nfl_season_start(d)
    thu1 = nfl_week1_thursday(start)
    if d < thu1:
        return 0
    current = 1 + (d - thu1).days // 7
    current = max(1, min(int(current), 18))
    week_tue = thu1 + timedelta(days=7 * (current - 1) + 5)  # Thursday + 5 = Tuesday
    if d >= week_tue:
        return current
    return max(0, current - 1)


def _int_week(raw: object) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def snap_cache_through_week(path: Path | None = None) -> int:
    src = path or SNAP_CACHE
    if not src.is_file():
        return 0
    try:
        raw = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    meta = raw.get("meta") if isinstance(raw, dict) else {}
    if isinstance(meta, dict) and meta.get("through_week") is not None:
        return _int_week(meta.get("through_week"))
    return 0


def route_cache_through_week(path: Path | None = None) -> int:
    src = path or ROUTE_META
    if not src.is_file():
        return 0
    try:
        raw = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    return _int_week(raw.get("through_week"))


def volume_week_report(
    *,
    as_of: date | None = None,
    completed_week: int | None = None,
    root: Path | None = None,
) -> dict[str, Any]:
    expected = completed_week if completed_week is not None else calendar_completed_week(as_of)
    data = (root or _REPO) / "Sports" / "NFL" / "data"
    snap_w = snap_cache_through_week(data / "nfl_snap_pct_cache.json")
    route_w = route_cache_through_week(data / "nfl_route_participation_meta.json")
    snap_ok = expected is None or expected <= 0 or snap_w >= expected
    route_ok = expected is None or expected <= 0 or route_w >= expected
    return {
        "ok": bool(snap_ok and route_ok),
        "completed_week": expected,
        "snap_through_week": snap_w,
        "route_through_week": route_w,
        "hint": (
            "py -3.14 Sports/NFL/scripts/build_nfl_snap_pct_cache.py --season "
            f"{infer_season(as_of)} ; "
            "py -3.14 Sports/NFL/scripts/build_nfl_route_participation.py --season "
            f"{infer_season(as_of)}"
        ),
    }


def assert_nfl_volume_week_fresh(
    *,
    as_of: date | None = None,
    completed_week: int | None = None,
    root: Path | None = None,
    label: str = "NFL volume",
) -> dict[str, Any]:
    """Raise ``NflVolumeCacheStale`` when either cache is behind."""
    rep = volume_week_report(as_of=as_of, completed_week=completed_week, root=root)
    if rep["completed_week"] is None:
        print(f"[{label}] offseason — volume week check skipped")
        return rep
    if rep["completed_week"] <= 0:
        print(f"[{label}] no completed week yet — volume week check skipped")
        return rep
    print(
        f"[{label}] completed_week={rep['completed_week']} "
        f"snap={rep['snap_through_week']} route={rep['route_through_week']}"
    )
    if not rep["ok"]:
        raise NflVolumeCacheStale(
            "NFL_VOLUME_STALE "
            f"completed_week={rep['completed_week']} "
            f"snap_through_week={rep['snap_through_week']} "
            f"route_through_week={rep['route_through_week']} "
            f"hint={rep['hint']}"
        )
    return rep
