"""Pre-kickoff NFL snap / target-share snapshots.

Same idea as NBA slate minutes: train later only on what was known before
kickoff. Each game's rows lock at that game's start_time. Later rebuilds
update only games that have not started. ``frozen_at`` is per row.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from utils.nfl_player_names import norm_nfl_player_name


def _parse_tip(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

_REPO = Path(__file__).resolve().parents[1]
SNAPSHOT_LOG = _REPO / "Sports" / "NFL" / "data" / "cache" / "nfl_volume_snapshots.jsonl"

VOLUME_COLS = (
    "snap_pct_L3",
    "snap_pct_season",
    "target_share_L3",
    "target_share_season",
    "wopr_L3",
    "route_pct_L3",
    "route_pct_source",
)


def _kick(row: pd.Series) -> datetime | None:
    for col in ("start_time", "game_time", "tip_utc", "commence_time"):
        if col in row.index:
            tip = _parse_tip(row.get(col))
            if tip is not None:
                return tip
    return None


def row_still_pre_kick(row: pd.Series, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    kick = _kick(row)
    if kick is None:
        return True
    return now < kick


def _num(v: object) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def _player_key(row: pd.Series) -> str:
    return norm_nfl_player_name(row.get("player") or row.get("player_name"))


def load_pre_kick_lookup(path: Path | None = None) -> dict[tuple[str, str], dict[str, Any]]:
    """Latest pre-kick snapshot per (slate, player_norm). Post-kick rows dropped."""
    src = path or SNAPSHOT_LOG
    out: dict[tuple[str, str], dict[str, Any]] = {}
    if not src.is_file():
        return out
    with src.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not rec.get("pre_kick"):
                continue
            key = (str(rec.get("slate") or "")[:10], str(rec.get("player_norm") or ""))
            if not key[0] or not key[1]:
                continue
            prev = out.get(key)
            if prev is None or str(rec.get("frozen_at") or "") >= str(prev.get("frozen_at") or ""):
                out[key] = rec
    return out


def append_pre_kick_snapshot(
    df: pd.DataFrame,
    *,
    slate: str,
    now: datetime | None = None,
    path: Path | None = None,
) -> int:
    """Append or refresh per-player volume. Locked rows (already kicked) stay."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    dest = path or SNAPSHOT_LOG
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = load_pre_kick_lookup(dest)
    pcol = "player" if "player" in df.columns else ("player_name" if "player_name" in df.columns else None)
    if pcol is None:
        return 0
    seen: set[str] = set()
    n = 0
    with dest.open("a", encoding="utf-8") as fh:
        for _, row in df.iterrows():
            pn = _player_key(row)
            if not pn or pn in seen:
                continue
            seen.add(pn)
            key = (str(slate)[:10], pn)
            if not row_still_pre_kick(row, now):
                continue
            kick = _kick(row)
            rec = {
                "slate": str(slate)[:10],
                "player": str(row.get(pcol) or ""),
                "player_norm": pn,
                "team": str(row.get("team") or ""),
                "pre_kick": True,
                "frozen_at": now.isoformat(),
                "kick_utc": kick.isoformat() if kick else "",
                "route_pct_source": str(row.get("route_pct_source") or ""),
            }
            for col in VOLUME_COLS:
                if col == "route_pct_source":
                    continue
                rec[col] = _num(row.get(col))
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
            existing[key] = rec
            n += 1
    return n


def apply_pre_kick_volume(
    df: pd.DataFrame,
    *,
    slate: str,
    path: Path | None = None,
) -> pd.DataFrame:
    """Overwrite volume columns from the last pre-kick snapshot.

    Training path: never use a post-kick rebuild. Rows without a snapshot
    stay as-is and should be dropped by the trainer, not imputed.
    """
    lookup = load_pre_kick_lookup(path)
    if not lookup or df is None or df.empty:
        return df
    out = df.copy()
    pcol = "player" if "player" in out.columns else (
        "player_name" if "player_name" in out.columns else None
    )
    if pcol is None:
        return out
    flags: list[bool] = []
    for idx, row in out.iterrows():
        rec = lookup.get((str(slate)[:10], _player_key(row)))
        if not rec:
            flags.append(False)
            continue
        for col in VOLUME_COLS:
            if col not in rec:
                continue
            if rec[col] is None and col != "route_pct_source":
                continue
            out.at[idx, col] = rec[col]
        flags.append(True)
    out["volume_pre_kick"] = flags
    return out
