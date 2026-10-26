"""Log-only lead / secondary / backup tags.

Does not change who qualifies on any frozen NFL track. Applied after the
row is written so Lloyd / McGowan style cheap floors can be split later.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from utils.nfl_demon_volume_shadow import snap_share
from utils.volume_floor_observe import role_tag

_ROOT = Path(__file__).resolve().parents[1]
_LOG = _ROOT / "data" / "reports" / "track_comparison.jsonl"
NFL_TRACKS = frozenset(
    {"nfl_demon_volume_shadow", "nfl_demon_rec_floor_shadow"}
)
NFL_BACKUP_SNAP = 40.0
_RANK_RE = re.compile(r"(?:rank|tgt_rank)_(\d+)")


def rank_from_source(raw: object) -> int | None:
    m = _RANK_RE.search(str(raw or ""))
    if not m:
        return None
    return int(m.group(1))


def nfl_role_tag(row: dict[str, Any], *, rank: int | None = None) -> str:
    snap = snap_share(row)
    if snap is None:
        snap = _f(row.get("snap_pct_L3"))
    rk = rank if rank is not None else rank_from_source(row.get("role_source"))
    if rk is None:
        rk = _f(row.get("rank"))
        rk = int(rk) if rk is not None else None
    return role_tag(rk, snap, backup_below=NFL_BACKUP_SNAP)


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v


def annotate_nfl_logs(
    board: list[dict[str, Any]],
    *,
    date: str,
    path: Path | None = None,
) -> int:
    """Add role_tag to today's NFL Demon log rows. Eligibility unchanged."""
    log_path = path or _LOG
    if not log_path.is_file():
        return 0
    board_snap = {}
    for row in board:
        key = re.sub(r"[^a-z0-9]+", "", str(row.get("player") or "").lower())
        if key:
            board_snap[key] = row
    rows = []
    n = 0
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            rec.get("track") in NFL_TRACKS
            and rec.get("date") == date
            and rec.get("kind") == "leg"
            and not rec.get("role_tag")
        ):
            src = board_snap.get(
                re.sub(r"[^a-z0-9]+", "", str(rec.get("player") or "").lower())
            )
            merged = {**(src or {}), **rec}
            rec["role_tag"] = nfl_role_tag(merged)
            n += 1
        rows.append(rec)
    log_path.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows),
        encoding="utf-8",
    )
    return n
