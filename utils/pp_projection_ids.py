"""Attach PrizePicks projection IDs to board rows from the dated step1 fetch.

NFL / NHL step8 clean exports drop projection_id, and CBB / CFB step1 store it as
``proj_id``. The Props Board deep link needs the ID of the exact line shown, so a
row only gets an ID when player + prop + pick type + line all match step1.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]

_ID_COLS = ("projection_id", "pp_projection_id", "proj_id")
_PROP_COLS = ("prop_type", "prop", "stat_type", "Prop")
_PICK_COLS = ("pick_type", "odds_type", "Pick Type")
_PLAYER_COLS = ("player", "player_name", "Player")
_LINE_COLS = ("line", "line_score", "Line")


def _first_col(df: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    return next((c for c in names if c in df.columns), None)


def _norm(v: object) -> str:
    return re.sub(r"\s+", " ", str(v or "").strip().lower())


def _prop(v: object) -> str:
    """'Shots On Goal' and 'shots_on_goal' compare equal."""
    return re.sub(r"[^a-z0-9+]", "", str(v or "").lower())


def _pick(v: object) -> str:
    t = _norm(v)
    if "gob" in t:
        return "goblin"
    if "dem" in t:
        return "demon"
    return "standard"


def _clean_id(v: object) -> str:
    s = str(v or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s if s.isdigit() else ""


def _line_key(v: object) -> str:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    return "" if pd.isna(x) else f"{float(x):.2f}"


def _step1_files(date: str, sport: str) -> list[Path]:
    day = REPO / "outputs" / date / sport.lower()
    if not day.is_dir():
        return []
    return sorted(p for p in day.glob("step1*.csv") if p.is_file())


def _step1_id_map(date: str, sport: str) -> tuple[dict, dict]:
    """Exact (player, prop, pick, line) -> id, plus (player, prop, pick) -> [(line, id)]."""
    out: dict[tuple[str, str, str, str], str] = {}
    near: dict[tuple[str, str, str], list[tuple[float, str]]] = {}
    for path in _step1_files(date, sport):
        try:
            s1 = pd.read_csv(path, dtype=str, low_memory=False).fillna("")
        except Exception:
            continue
        id_c = _first_col(s1, _ID_COLS)
        pl_c = _first_col(s1, _PLAYER_COLS)
        pr_c = _first_col(s1, _PROP_COLS)
        ln_c = _first_col(s1, _LINE_COLS)
        if not (id_c and pl_c and pr_c and ln_c):
            continue
        pk_c = _first_col(s1, _PICK_COLS)
        for _, r in s1.iterrows():
            pid = _clean_id(r[id_c])
            if not pid:
                continue
            key = (
                _norm(r[pl_c]),
                _prop(r[pr_c]),
                _pick(r[pk_c]) if pk_c else "standard",
                _line_key(r[ln_c]),
            )
            out.setdefault(key, pid)
            if key[3]:
                near.setdefault(key[:3], []).append((float(key[3]), pid))
    return out, near


def attach_projection_ids(df: pd.DataFrame, slate_date: str) -> pd.DataFrame:
    """Fill blank projection_id / pp_projection_id from outputs/<date>/<sport>/step1*.csv."""
    if df is None or df.empty or "sport" not in df.columns or not slate_date:
        return df
    pl_c = _first_col(df, _PLAYER_COLS)
    pr_c = _first_col(df, _PROP_COLS)
    ln_c = _first_col(df, _LINE_COLS)
    if not (pl_c and pr_c and ln_c):
        return df
    pk_c = _first_col(df, _PICK_COLS)
    out = df.copy()
    for c in ("projection_id", "pp_projection_id"):
        if c not in out.columns:
            out[c] = ""
    have = out["projection_id"].map(_clean_id).ne("") | out["pp_projection_id"].map(_clean_id).ne("")
    date = str(slate_date)[:10]
    for sport in out.loc[~have, "sport"].dropna().astype(str).unique():
        id_map, near = _step1_id_map(date, sport)
        if not id_map:
            continue
        rows = out.index[~have & out["sport"].astype(str).eq(sport)]
        filled = 0
        for idx in rows:
            key = (
                _norm(out.at[idx, pl_c]),
                _prop(out.at[idx, pr_c]),
                _pick(out.at[idx, pk_c]) if pk_c else "standard",
                _line_key(out.at[idx, ln_c]),
            )
            pid = id_map.get(key)
            if not pid and key[3]:
                # Board rounds some lines (NHL TOI 23.25 shows as 23.2).
                cands = {i for ln, i in near.get(key[:3], []) if abs(ln - float(key[3])) <= 0.06}
                if len(cands) == 1:
                    pid = cands.pop()
            if pid:
                out.at[idx, "projection_id"] = pid
                out.at[idx, "pp_projection_id"] = pid
                filled += 1
        print(f"  [pp_ids] {sport}: projection_id filled {filled}/{len(rows)} from step1")
    return out
