"""NHL SOG floor observe. Registered 2026-10-25. Logged only.

Not ``nhl_sog_shadow`` (Goblin 0.5). Separate log rows, separate bar.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.volume_floor_observe import FloorSpec, grade_pending as _grade
from utils.volume_floor_observe import record_floor_shadow, summarize as _summarize

TRACK = "nhl_sog_floor_shadow"
PREREG_DATE = "2026-10-25"
SPEC = FloorSpec(
    track=TRACK,
    sport="NHL",
    prereg_date=PREREG_DATE,
    props=frozenset({"sog"}),
    share_keys=("avg_l5", "season_avg", "toi", "TOI"),
    minutes_keys=("toi", "TOI", "ice_time"),
    backup_minutes_below=12.0,
    id_prefix="NHLSOG",
    prereg_json=Path(__file__).resolve().parents[1] / "ml" / "prereg" / "nhl_sog_floor_2026-10-25.json",
)


def record_nhl_sog_floor_shadow(board, *, date: str, **kw: Any) -> dict[str, Any]:
    return record_floor_shadow(SPEC, board, date=date, folder="nhl", **kw)


def grade_pending(*, path: Path | None = None, templates: Path | None = None) -> dict[str, int]:
    return _grade(SPEC, path=path, templates=templates)


def summarize(*, path: Path | None = None) -> dict[str, Any]:
    return _summarize(SPEC, path=path)
