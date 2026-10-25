"""Frozen as-of proj_edge rankings.

History dates are write-once. Live slates merge per game: a row locks at
that game's tip. Later T−30 refreshes update only games that have not
started. Each row keeps its own ``frozen_at``.

  data/cache/nba_proj_edge/YYYY-MM-DD.jsonl
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = _REPO / "data" / "cache" / "nba_proj_edge"

FROZEN_COLS = (
    "proj_edge",
    "proj_ev_edge",
    "proj_mu",
    "proj_mu_raw",
    "proj_minutes",
    "proj_hold",
    "proj_p_play",
    "proj_p_over",
    "proj_p_under",
    "proj_n",
    "proj_var",
    "proj_k",
    "base_rate",
    "_gate",
)


def snapshot_path(slate: str) -> Path:
    return SNAPSHOT_DIR / f"{str(slate)[:10]}.jsonl"


def frozen_exists(slate: str) -> bool:
    p = snapshot_path(slate)
    return p.is_file() and p.stat().st_size > 0


def leg_key(row: pd.Series | dict[str, Any]) -> str:
    get = row.get if hasattr(row, "get") else lambda k, default=None: row[k] if k in row else default
    player = str(get("player") or get("player_name") or "").strip().lower()
    prop = str(get("prop") or get("prop_type") or get("stat_type") or "").strip().lower()
    line = get("line")
    try:
        line_s = f"{float(line):g}"
    except (TypeError, ValueError):
        line_s = str(line or "")
    pt = str(get("pick_type") or "").strip().lower()
    direction = str(get("direction") or "").strip().upper()
    date = str(get("file_date") or "")[:10]
    return f"{date}|{player}|{prop}|{line_s}|{pt}|{direction}"


def write_frozen_day(
    df: pd.DataFrame,
    slate: str,
    *,
    replace: bool = False,
    frozen_at: str | None = None,
    pre_tip: bool | None = None,
    source: str = "graded",
) -> int:
    dest = snapshot_path(slate)
    if dest.is_file() and not replace:
        return 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    stamp = frozen_at or datetime.now(timezone.utc).isoformat()
    n = 0
    with dest.open("w", encoding="utf-8") as fh:
        for _, row in df.iterrows():
            rec = {
                "leg_key": leg_key(row),
                "file_date": str(slate)[:10],
                "frozen_at": stamp,
                "source": source,
            }
            if pre_tip is not None:
                rec["pre_tip"] = bool(pre_tip)
            rec["player"] = str(row.get("player") or row.get("player_name") or "")
            rec["prop"] = str(row.get("prop") or row.get("prop_type") or "")
            rec["line"] = row.get("line")
            rec["direction"] = str(row.get("direction") or "")
            rec["pick_type"] = str(row.get("pick_type") or "")
            for col in FROZEN_COLS:
                if col in df.columns:
                    val = row.get(col)
                    if pd.isna(val):
                        rec[col] = None
                    elif col == "_gate":
                        rec[col] = bool(val)
                    else:
                        try:
                            rec[col] = float(val)
                        except (TypeError, ValueError):
                            rec[col] = None
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
            n += 1
    return n


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


def game_key(row: pd.Series | dict[str, Any]) -> str:
    get = row.get if hasattr(row, "get") else lambda k, default=None: row[k] if k in row else default
    tip = str(get("tip_utc") or "")[:25]
    a = str(get("team") or "").strip().upper()
    b = str(get("opp_team") or get("opp") or "").strip().upper()
    parts = sorted(x for x in (a, b) if x and "/" not in x)
    if not parts:
        return tip or "unknown"
    return "|".join(parts + ([tip] if tip else []))


def row_still_pre_tip(
    row: pd.Series | dict[str, Any],
    now: datetime | None = None,
    *,
    fallback: bool | None = None,
) -> bool:
    """True while this game can still change a slip. Locks at that game's tip."""
    get = row.get if hasattr(row, "get") else lambda k, default=None: row[k] if k in row else default
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    tip = _parse_tip(get("tip_utc"))
    if tip is not None:
        return now < tip
    window = str(get("game_window") or "").strip().lower()
    if window == "live":
        return False
    if window in {"early", "tminus30"}:
        return True
    flag = get("slate_pre_tip")
    if flag is not None and str(flag).strip() != "" and not (isinstance(flag, float) and pd.isna(flag)):
        return bool(flag) and str(flag).strip().lower() not in {"0", "false", "nan"}
    if fallback is None:
        return False
    return bool(fallback)


def _row_rec(
    row: pd.Series,
    slate: str,
    *,
    stamp: str,
    source: str,
    pre_tip: bool,
) -> dict[str, Any]:
    rec = {
        "leg_key": leg_key(row),
        "file_date": str(slate)[:10],
        "frozen_at": stamp,
        "source": source,
        "pre_tip": bool(pre_tip),
        "player": str(row.get("player") or row.get("player_name") or ""),
        "prop": str(row.get("prop") or row.get("prop_type") or ""),
        "line": row.get("line"),
        "direction": str(row.get("direction") or ""),
        "pick_type": str(row.get("pick_type") or ""),
        "team": str(row.get("team") or ""),
        "tip_utc": str(row.get("tip_utc") or ""),
        "game_key": game_key(row),
    }
    for col in FROZEN_COLS:
        if col not in row.index:
            continue
        val = row.get(col)
        if pd.isna(val):
            rec[col] = None
        elif col == "_gate":
            rec[col] = bool(val)
        else:
            try:
                rec[col] = float(val)
            except (TypeError, ValueError):
                rec[col] = None
    return rec


def _write_recs(slate: str, recs: Iterable[dict[str, Any]]) -> int:
    dest = snapshot_path(slate)
    dest.parent.mkdir(parents=True, exist_ok=True)
    rows = list(recs)
    with dest.open("w", encoding="utf-8") as fh:
        for rec in rows:
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
    return len(rows)


def write_pre_tip_frozen_day(
    df: pd.DataFrame,
    slate: str,
    *,
    pre_tip: bool | None = None,
    frozen_at: str | None = None,
    now: datetime | None = None,
) -> int:
    """Merge live rankings. Rows lock at their own tip; later games still update.

    Returns how many rows were written or refreshed. Tipped games keep the
    ``frozen_at`` they already had.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    stamp = frozen_at or now.isoformat()
    existing = load_frozen_day(slate)
    updated = 0
    for _, row in df.iterrows():
        open_row = row_still_pre_tip(row, now, fallback=pre_tip)
        key = leg_key(row)
        if not open_row:
            continue
        existing[key] = _row_rec(row, slate, stamp=stamp, source="live_board", pre_tip=True)
        updated += 1
    if updated:
        _write_recs(slate, existing.values())
    return updated


def load_frozen_day(slate: str) -> dict[str, dict[str, Any]]:
    src = snapshot_path(slate)
    out: dict[str, dict[str, Any]] = {}
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
            key = str(rec.get("leg_key") or "")
            if key:
                out[key] = rec
    return out


def apply_frozen_proj(df: pd.DataFrame, frozen: dict[str, dict[str, Any]] | None = None) -> pd.DataFrame:
    """Overwrite projection columns from a frozen as-of file. No match → leave computed."""
    out = df.copy()
    if frozen is None:
        dates = sorted({str(x)[:10] for x in out.get("file_date", pd.Series(dtype=str)).astype(str) if str(x)[:10]})
        frozen = {}
        for d in dates:
            frozen.update(load_frozen_day(d))
    if not frozen:
        out["proj_edge_frozen"] = 0
        return out
    used = []
    for idx, row in out.iterrows():
        rec = frozen.get(leg_key(row))
        if rec is None:
            used.append(0)
            continue
        for col in FROZEN_COLS:
            if col not in rec:
                continue
            val = rec[col]
            if val is None:
                continue
            out.at[idx, col] = val
        used.append(1)
    out["proj_edge_frozen"] = used
    return out
