"""Freshness checks for NFL soft-context CSVs (unit / pace / route / O/D md).

Pipeline + Tuesday pack call this so we do not silently use week-old ranks.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_DATA = _REPO / "Sports" / "NFL" / "data"

# Soft context older than this during the season should be rebuilt.
DEFAULT_MAX_AGE_HOURS = 7 * 24  # one week between Tuesday refreshes

CORE_FILES = (
    "nfl_ol_ranks.csv",
    "nfl_dl_ranks.csv",
    "nfl_secondary_ranks.csv",
    "nfl_box_ranks.csv",
    "nfl_pace_ranks.csv",
    "nfl_route_participation.csv",
    "nfl_team_unit_ranks.csv",
    "defense_rankings.csv",
)


def _mtime(path: Path) -> datetime | None:
    if not path.is_file():
        return None
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def context_freshness_report(
    root: Path | None = None,
    *,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
) -> dict:
    base = (root or _REPO) / "Sports" / "NFL" / "data"
    now = datetime.now(timezone.utc)
    files: dict[str, dict] = {}
    oldest: datetime | None = None
    missing: list[str] = []
    stale: list[str] = []
    for name in CORE_FILES:
        p = base / name
        mt = _mtime(p)
        if mt is None:
            missing.append(name)
            files[name] = {"path": str(p), "exists": False}
            continue
        age_h = (now - mt).total_seconds() / 3600.0
        is_stale = age_h > float(max_age_hours)
        if is_stale:
            stale.append(name)
        if oldest is None or mt < oldest:
            oldest = mt
        files[name] = {
            "path": str(p),
            "exists": True,
            "mtime_utc": mt.isoformat(),
            "age_hours": round(age_h, 1),
            "stale": is_stale,
        }

    current_md = base / "nfl_od_rankings_current.md"
    meta_path = base / "nfl_od_rankings_meta.json"
    meta = {}
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}

    return {
        "ok": not missing and not stale,
        "missing": missing,
        "stale": stale,
        "max_age_hours": float(max_age_hours),
        "oldest_mtime_utc": oldest.isoformat() if oldest else None,
        "current_md_exists": current_md.is_file(),
        "current_md_mtime_utc": (_mtime(current_md).isoformat() if _mtime(current_md) else None),
        "od_meta": meta,
        "files": files,
        "hint": (
            "pwsh -File Sports/NFL/scripts/run_tuesday_post_week.ps1 -Season 2026 -Week N"
            if (missing or stale)
            else "context fresh"
        ),
    }


def warn_if_stale(
    root: Path | None = None,
    *,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    label: str = "NFL context",
) -> dict:
    """Print a warning; never raises (soft attach path)."""
    rep = context_freshness_report(root, max_age_hours=max_age_hours)
    if rep["missing"]:
        print(f"[{label}] WARN missing context files: {', '.join(rep['missing'])}")
    if rep["stale"]:
        print(
            f"[{label}] WARN stale context (>{rep['max_age_hours']:.0f}h): "
            f"{', '.join(rep['stale'])} — {rep['hint']}"
        )
    if rep["ok"] and rep.get("od_meta"):
        m = rep["od_meta"]
        print(
            f"[{label}] using unit context "
            f"season={m.get('season')} week={m.get('week')} "
            f"built={m.get('built_at', '')[:19]}"
        )
    return rep


def assert_context_fresh(
    root: Path | None = None,
    *,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
) -> dict:
    """Loud fail for Tuesday pack / CI when context is missing or stale."""
    rep = context_freshness_report(root, max_age_hours=max_age_hours)
    if rep["missing"] or rep["stale"]:
        raise RuntimeError(
            "NFL_CONTEXT_STALE "
            f"missing={rep['missing']} stale={rep['stale']} "
            f"hint={rep['hint']}"
        )
    return rep
