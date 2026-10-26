#!/usr/bin/env python3
"""All-category floor-persistence table (gift-scan Step 2).

  py -3.14 -m ml.floor_persistence

Line-free, as-of only. For every sport × prop × role:

    P(next game >= L5 min − k)  for k in {0, 1, 2}

Binary / threshold props use P(actual >= threshold) instead of a floor.
Unsupported football mappings stay out. Does not import or edit the
frozen NFL tracks.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO)]

from ml.volume_floor import (  # noqa: E402
    MIN_N,
    bh_keep,
    binom_p_greater,
    cell_stats,
)
from ml.volume_floor_backtest import load_stats_week  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.stat_attach_invalid import is_analysis_invalid_prop  # noqa: E402

PREREG = _REPO / "ml" / "prereg" / "floor_persistence_2026-10-25.json"
DB = _REPO / "data" / "cache" / "proporacle_ref.db"
NFL_CACHE = _REPO / "Sports" / "NFL" / "data" / "cache"
OUT_DIR = _REPO / "data" / "reports" / "floor_persistence"
K_VALUES = (0, 1, 2)
ROLES = ("lead", "top2", "all")
BH_NULL = 0.70
BH_ALPHA = 0.10
SEASONS = (2021, 2022, 2023, 2024, 2025, 2026)

SKIP_BOX = frozenset(
    {
        "game_date",
        "event_id",
        "league",
        "home_team",
        "away_team",
        "player",
        "team",
        "position",
        "espn_athlete_id",
        "espn_player_id",
        "starter",
        "season_window",
        "preseason_year",
        "season",
        "player_id",
        "player_name",
        "player_display_name",
        "position_group",
    }
)

NBA_PROPS = (
    "pts",
    "reb",
    "ast",
    "stl",
    "blk",
    "tov",
    "fgm",
    "fga",
    "fg3m",
    "fg3a",
    "fg2m",
    "fg2a",
    "ftm",
    "fta",
    "oreb",
    "dreb",
    "pra",
    "pr",
    "pa",
    "ra",
    "bs",
    "minutes",
)
NHL_PROPS = (
    "goals",
    "assists",
    "points",
    "shots_on_goal",
    "hits",
    "blocked_shots",
    "pim",
    "plus_minus",
    "pp_points",
    "faceoffs_won",
    "toi",
    "saves",
    "goals_allowed",
)
SOCCER_PROPS = (
    "sh",
    "sog",
    "g",
    "a",
    "sv",
    "pa",
    "kp",
    "tk",
    "fc",
    "yc",
    "minutes_played",
    "clearances",
    "dribble_attempts",
    "crosses",
    "g_a",
)
NFL_PROPS = (
    "rush_attempts",
    "rushing_yards",
    "receptions",
    "targets",
    "receiving_yards",
    "passing_yards",
    "pass_attempts",
    "rush_rec_yards",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_sacks",
    "fg_att",
    "fg_made",
    "pat_att",
    "pat_made",
    "kicking_points",
    "pt_att",
    "pt_yards",
    "punt_returns",
    "kickoff_returns",
)
NFL_BINARY = (
    ("player_tds", 1.0),
    ("passing_tds", 1.0),
)
CFB_MAP = {
    "TACK_TOT": "tackles",
    "SACK": "sacks",
    "TFL": "tfl",
    "PASS_YDS": "pass_yards",
    "RUSH_YDS": "rush_yards",
    "REC_YDS": "rec_yards",
    "REC": "receptions",
    "PASS_TD": "pass_tds",
    "RUSH_TD": "rush_tds",
    "REC_TD": "rec_tds",
    "PASS_INT": "interceptions",
    "KICK_PTS": "kicking_points",
    "KICK_FG": "fg_made",
    "KICK_XP": "xp_made",
    "DEF_INT": "def_int",
}
# Sibling-mapped or period faces stay out.
CFB_SKIP = frozenset({"RUSH_LONG", "PASS_CMP", "PASS_LONG", "REC_LONG"})


def load_prereg(*, path: Path | None = None) -> dict[str, Any]:
    dest = path or PREREG
    return json.loads(dest.read_text(encoding="utf-8"))


def assert_prereg(*, path: Path | None = None) -> dict[str, Any]:
    spec = load_prereg(path=path)
    assert spec["track"] == "floor_persistence"
    assert spec["prereg_date"] == "2026-10-25"
    assert spec["k"] == list(K_VALUES)
    assert spec["min_n"] == MIN_N
    assert spec["bh_null"] == BH_NULL
    return spec


def _asof_l5_min(series: pd.Series, group: pd.Series) -> pd.Series:
    prior = series.groupby(group, sort=False).shift(1)
    return prior.groupby(group, sort=False).transform(
        lambda s: s.rolling(5, min_periods=5).min()
    )


def _asof_l3_mean(series: pd.Series, group: pd.Series) -> pd.Series:
    prior = series.groupby(group, sort=False).shift(1)
    return prior.groupby(group, sort=False).transform(
        lambda s: s.rolling(3, min_periods=1).mean()
    )


def _team_rank(
    df: pd.DataFrame,
    l3: pd.Series,
    date_col: str,
    team_col: str,
) -> pd.Series:
    if team_col not in df.columns:
        return pd.Series(pd.NA, index=df.index)
    teams = df[team_col].astype(str).str.strip()
    blank = teams.isin(("", "None", "nan", "NaN"))
    if float(blank.mean()) >= 0.5:
        return pd.Series(pd.NA, index=df.index)
    return l3.groupby([df[date_col], teams]).rank(method="min", ascending=False)


def _role_masks(rank: pd.Series) -> dict[str, pd.Series]:
    r = pd.to_numeric(rank, errors="coerce")
    return {
        "lead": r.eq(1),
        "top2": r.le(2),
        "all": pd.Series(True, index=rank.index),
    }


def _score_floor(
    actual: pd.Series, floor: pd.Series, mask: pd.Series
) -> list[dict[str, Any]]:
    a = pd.to_numeric(actual, errors="coerce")
    f = pd.to_numeric(floor, errors="coerce")
    usable = mask & a.notna() & f.notna() & (f > 0)
    out = []
    for k in K_VALUES:
        keep = usable
        hits = int((a[keep] >= (f[keep] - k)).sum())
        n = int(keep.sum())
        out.append({"k": k, "hits": hits, "n": n})
    return out


def _score_threshold(
    actual: pd.Series, thresh: float, mask: pd.Series
) -> dict[str, Any]:
    a = pd.to_numeric(actual, errors="coerce")
    keep = mask & a.notna()
    hits = int((a[keep] >= thresh).sum())
    n = int(keep.sum())
    return {"k": None, "hits": hits, "n": n, "threshold": thresh}


def _box_frame(
    con: sqlite3.Connection,
    table: str,
    cols: tuple[str, ...],
    *,
    extra: str = "",
) -> pd.DataFrame:
    have = {r[1] for r in con.execute(f"PRAGMA table_info({table})")}
    use = [c for c in cols if c in have]
    keys = ["game_date", "player", "team"]
    for opt in ("minutes", "toi", "minutes_played", "season_window", "position"):
        if opt in have and opt not in keys and opt not in use:
            keys.append(opt)
    select = ", ".join(dict.fromkeys(keys + use))
    sql = f"SELECT {select} FROM {table}"
    if extra:
        sql += f" {extra}"
    df = pd.read_sql_query(sql, con)
    if df.empty:
        return df
    df["game_date"] = df["game_date"].astype(str).str.slice(0, 10)
    df["player"] = df["player"].astype(str)
    df["team"] = df.get("team", pd.Series("", index=df.index)).astype(str)
    if "season_window" in df.columns:
        sw = df["season_window"].astype(str).str.lower()
        df = df.loc[~sw.str.contains("pre", na=False)].copy()
    return df


def persist_numeric(
    df: pd.DataFrame,
    *,
    sport: str,
    props: tuple[str, ...],
    player_col: str = "player",
    date_col: str = "game_date",
    team_col: str = "team",
    role_fallback: str | None = None,
    binaries: tuple[tuple[str, float], ...] = (),
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    df = df.sort_values([player_col, date_col]).copy()
    gid = df[player_col].astype(str)
    cells: list[dict[str, Any]] = []
    fallback = None
    if role_fallback and role_fallback in df.columns:
        fallback = _asof_l3_mean(pd.to_numeric(df[role_fallback], errors="coerce"), gid)

    for prop in props:
        if prop not in df.columns:
            continue
        if is_analysis_invalid_prop(sport, prop):
            continue
        val = pd.to_numeric(df[prop], errors="coerce")
        floor = _asof_l5_min(val, gid)
        l3 = _asof_l3_mean(val, gid)
        if fallback is not None:
            l3 = l3.where(l3.notna(), fallback)
        rank = _team_rank(df, l3, date_col, team_col)
        masks = _role_masks(rank)
        canon = canon_prop(sport, prop) or prop
        for role, mask in masks.items():
            for rec in _score_floor(val, floor, mask):
                cells.append(
                    {
                        "sport": sport,
                        "prop": prop,
                        "prop_canon": canon,
                        "role": role,
                        "kind": "floor",
                        **rec,
                    }
                )

    for prop, thresh in binaries:
        if prop not in df.columns:
            continue
        val = pd.to_numeric(df[prop], errors="coerce")
        l3 = _asof_l3_mean(val, gid)
        if fallback is not None:
            l3 = l3.where(l3.notna(), fallback)
        rank = _team_rank(df, l3, date_col, team_col)
        masks = _role_masks(rank)
        canon = canon_prop(sport, prop) or prop
        for role, mask in masks.items():
            rec = _score_threshold(val, thresh, mask)
            cells.append(
                {
                    "sport": sport,
                    "prop": prop,
                    "prop_canon": canon,
                    "role": role,
                    "kind": "threshold",
                    **rec,
                }
            )
    return cells


def load_nba_like(con: sqlite3.Connection, table: str, sport: str) -> list[dict[str, Any]]:
    df = _box_frame(con, table, NBA_PROPS)
    if df.empty:
        return []
    if {"pts", "reb", "ast"}.issubset(df.columns):
        dd = (
            (pd.to_numeric(df["pts"], errors="coerce") >= 10).astype(int)
            + (pd.to_numeric(df["reb"], errors="coerce") >= 10).astype(int)
            + (pd.to_numeric(df["ast"], errors="coerce") >= 10).astype(int)
        )
        df = df.copy()
        df["double_double"] = (dd >= 2).astype(float)
    return persist_numeric(
        df,
        sport=sport,
        props=NBA_PROPS,
        role_fallback="minutes" if "minutes" in df.columns else None,
        binaries=(("double_double", 1.0),) if "double_double" in df.columns else (),
    )


def load_nhl(con: sqlite3.Connection) -> list[dict[str, Any]]:
    df = _box_frame(con, "nhl", NHL_PROPS)
    if df.empty:
        return []
    bins: tuple[tuple[str, float], ...] = ()
    if "goals" in df.columns:
        bins = (("goals", 1.0),)
    return persist_numeric(
        df,
        sport="NHL",
        props=tuple(p for p in NHL_PROPS if p != "goals"),
        role_fallback="toi" if "toi" in df.columns else None,
        binaries=bins,
    )


def load_soccer(con: sqlite3.Connection) -> list[dict[str, Any]]:
    df = _box_frame(con, "soccer", SOCCER_PROPS)
    if df.empty:
        return []
    df = df.copy()
    if {"g", "a"}.issubset(df.columns):
        df["g_a"] = pd.to_numeric(df["g"], errors="coerce").fillna(0) + pd.to_numeric(
            df["a"], errors="coerce"
        ).fillna(0)
    bins: list[tuple[str, float]] = []
    if "g" in df.columns:
        bins.append(("g", 1.0))
    if "g_a" in df.columns:
        bins.append(("g_a", 1.0))
    floor_props = tuple(p for p in SOCCER_PROPS if p not in {"g", "g_a"})
    return persist_numeric(
        df,
        sport="Soccer",
        props=floor_props,
        role_fallback="minutes_played" if "minutes_played" in df.columns else None,
        binaries=tuple(bins),
    )


def load_mlb(con: sqlite3.Connection) -> list[dict[str, Any]]:
    have = {r[1] for r in con.execute("PRAGMA table_info(mlb_gamelog)")}
    if "prop_norm" not in have:
        return []
    df = pd.read_sql_query(
        "SELECT mlb_player_id, game_date, player_type, prop_norm, stat_value, team_id "
        "FROM mlb_gamelog WHERE stat_value IS NOT NULL",
        con,
    )
    if df.empty:
        return []
    df["game_date"] = df["game_date"].astype(str).str.slice(0, 10)
    df["player"] = df["mlb_player_id"].astype(str)
    df["team"] = df.get("team_id", pd.Series("", index=df.index)).astype(str)
    cells: list[dict[str, Any]] = []
    for prop, sub in df.groupby("prop_norm", sort=False):
        prop_s = str(prop)
        work = sub.rename(columns={"stat_value": prop_s}).copy()
        kind = "threshold" if prop_s in {"home_runs", "stolen_bases", "triples"} else "floor"
        extra = persist_numeric(
            work,
            sport="MLB",
            props=(prop_s,),
            role_fallback=None,
            binaries=((prop_s, 1.0),) if kind == "threshold" else (),
        )
        if kind == "threshold":
            extra = [c for c in extra if c.get("kind") == "threshold"]
        cells.extend(extra)
        # Split pitcher / hitter as extra roles when labeled.
        for ptype, chunk in work.groupby(work["player_type"].astype(str).str.lower()):
            if ptype not in {"pitcher", "hitter"}:
                continue
            more = persist_numeric(
                chunk,
                sport="MLB",
                props=(prop_s,),
                role_fallback=None,
                binaries=((prop_s, 1.0),) if kind == "threshold" else (),
            )
            for rec in more:
                if kind == "threshold" and rec.get("kind") != "threshold":
                    continue
                rec = dict(rec)
                rec["role"] = f"{rec['role']}_{ptype}"
                cells.append(rec)
    return cells


def load_nfl(*, cache_dir: Path | None = None) -> list[dict[str, Any]]:
    frames = []
    for season in SEASONS:
        try:
            frames.append(load_stats_week(season, cache_dir=cache_dir or NFL_CACHE))
        except Exception:
            continue
    if not frames:
        return []
    df = pd.concat(frames, ignore_index=True)
    df["player"] = df["player_id"].astype(str)
    df["game_date"] = (
        df["season"].astype(str)
        + "-"
        + df["week"].astype(int).astype(str).str.zfill(2)
    )
    df["rush_attempts"] = pd.to_numeric(df.get("carries"), errors="coerce")
    df["pass_attempts"] = pd.to_numeric(df.get("attempts"), errors="coerce")
    rush = pd.to_numeric(df.get("rushing_yards"), errors="coerce")
    rec = pd.to_numeric(df.get("receiving_yards"), errors="coerce")
    df["rush_rec_yards"] = rush.fillna(0) + rec.fillna(0)
    fg = pd.to_numeric(df.get("fg_made"), errors="coerce").fillna(0)
    pat = pd.to_numeric(df.get("pat_made"), errors="coerce").fillna(0)
    df["kicking_points"] = fg * 3 + pat
    rush_td = pd.to_numeric(df.get("rushing_tds"), errors="coerce").fillna(0)
    rec_td = pd.to_numeric(df.get("receiving_tds"), errors="coerce").fillna(0)
    st_td = pd.to_numeric(df.get("special_teams_tds"), errors="coerce").fillna(0)
    df["player_tds"] = rush_td + rec_td + st_td
    df["passing_tds"] = pd.to_numeric(df.get("passing_tds"), errors="coerce")
    snap_proxy = (
        df["rush_attempts"].fillna(0)
        + df["pass_attempts"].fillna(0)
        + pd.to_numeric(df.get("targets"), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("def_tackles_solo"), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("fg_att"), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("pt_att"), errors="coerce").fillna(0)
    )
    df = df.loc[snap_proxy > 0].copy()
    return persist_numeric(
        df,
        sport="NFL",
        props=NFL_PROPS,
        role_fallback="rush_attempts",
        binaries=NFL_BINARY,
    )


def load_cfb(con: sqlite3.Connection) -> list[dict[str, Any]]:
    have = {r[1] for r in con.execute("PRAGMA table_info(espn_boxscore_rows)")}
    if "payload" not in have:
        return []
    raw = pd.read_sql_query(
        "SELECT event_id, player_key, game_date, payload FROM espn_boxscore_rows "
        "WHERE lower(sport) = 'cfb'",
        con,
    )
    if raw.empty:
        return []
    rows = []
    for rec in raw.itertuples(index=False):
        try:
            payload = json.loads(rec.payload) if rec.payload else {}
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        item = {
            "player": str(payload.get("player_norm") or rec.player_key),
            "team": str(payload.get("team_id") or ""),
            "game_date": str(rec.game_date or "")[:10],
        }
        gd = str(rec.game_date or "")
        if len(gd) == 8 and gd.isdigit():
            item["game_date"] = f"{gd[0:4]}-{gd[4:6]}-{gd[6:8]}"
        for src, dest in CFB_MAP.items():
            if src in CFB_SKIP:
                continue
            try:
                item[dest] = float(payload[src]) if payload.get(src) is not None else None
            except (TypeError, ValueError):
                item[dest] = None
        item["player_tds"] = (item.get("rush_tds") or 0) + (item.get("rec_tds") or 0)
        item["rush_rec_yards"] = (item.get("rush_yards") or 0) + (item.get("rec_yards") or 0)
        rows.append(item)
    if not rows:
        return []
    df = pd.DataFrame(rows)
    props = tuple(CFB_MAP.values()) + ("rush_rec_yards",)
    return persist_numeric(
        df,
        sport="CFB",
        props=props,
        role_fallback="minutes" if "minutes" in df.columns else None,
        binaries=(("player_tds", 1.0),),
    )


def finalize(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = []
    pvals: list[float | None] = []
    for rec in cells:
        n = int(rec.get("n") or 0)
        hits = int(rec.get("hits") or 0)
        stats = cell_stats(hits, n, breakeven=None)
        row = {**rec, **stats}
        testable = (
            rec.get("kind") == "floor"
            and rec.get("k") == 0
            and n >= MIN_N
        )
        row["p_gt_70"] = binom_p_greater(hits, n, BH_NULL) if testable else None
        scored.append(row)
        pvals.append(row["p_gt_70"])
    keep = bh_keep(pvals, alpha=BH_ALPHA)
    for i, row in enumerate(scored):
        row["bh_keep"] = bool(i in keep and row.get("p_gt_70") is not None)
        row["thin"] = int(row.get("n") or 0) < MIN_N
    scored.sort(
        key=lambda r: (
            str(r.get("sport")),
            str(r.get("prop")),
            str(r.get("role")),
            99 if r.get("k") is None else int(r["k"]),
        )
    )
    return scored


def write_reports(cells: list[dict[str, Any]], *, out_dir: Path | None = None) -> Path:
    dest = out_dir or OUT_DIR
    dest.mkdir(parents=True, exist_ok=True)
    usable = [
        c
        for c in cells
        if not c.get("thin")
        and not (c.get("sport") == "CFB" and c.get("prop") == "minutes")
    ]
    payload = {
        "prereg": "ml/prereg/floor_persistence_2026-10-25.json",
        "min_n": MIN_N,
        "k": list(K_VALUES),
        "bh_alpha": BH_ALPHA,
        "bh_null": BH_NULL,
        "n_cells": len(cells),
        "n_cells_n50": len(usable),
        "n_bh_keep": sum(1 for c in cells if c.get("bh_keep")),
        "sports": sorted({str(c.get("sport")) for c in cells}),
        "cells": cells,
    }
    latest_json = dest / "floor_persistence_latest.json"
    dated_json = dest / "floor_persistence_2026-10-25.json"
    latest_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    dated_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lookup_rows = []
    for c in usable:
        lookup_rows.append(
            {
                "sport": c.get("sport"),
                "prop": c.get("prop"),
                "prop_canon": c.get("prop_canon"),
                "role": c.get("role"),
                "kind": c.get("kind"),
                "k": c.get("k"),
                "n": c.get("n"),
                "hits": c.get("hits"),
                "hr": c.get("hr"),
                "wilson_low": c.get("wilson_low"),
                "bh_keep": c.get("bh_keep"),
            }
        )
    lookup = dest / "floor_persistence_lookup.csv"
    pd.DataFrame(lookup_rows).to_csv(lookup, index=False)

    lines = [
        "# Floor persistence (2026-10-25)",
        "",
        "Line-free, as-of L5 min. Gift-scan Step 2. Frozen NFL tracks untouched.",
        "",
        f"Cells n≥{MIN_N}: **{len(usable)}**. BH-keep P(k=0)>0.70: **{payload['n_bh_keep']}**.",
        "",
        "`n` is not box-row count. A floor cell only counts a game when the",
        "player already has five prior games **and** that L5 minimum is > 0.",
        "A floor of 0 is vacuous (next game is almost always ≥ 0), so sparse",
        "counting props (assists, key passes, yellows, TDs as a floor) shrink.",
        "",
    ]
    catalog = [
        c
        for c in usable
        if (c.get("kind") == "floor" and c.get("k") == 0)
        or (c.get("kind") == "threshold" and c.get("role") in {"all", "lead", "top2"})
    ]
    sports = sorted({str(c.get("sport")) for c in catalog})
    for sport in sports:
        rows = [c for c in catalog if c.get("sport") == sport]
        props = sorted({str(c.get("prop")) for c in rows})
        lines.extend(
            [
                f"## {sport}",
                "",
                "| Prop | Role | Kind | n | HR | Wilson | BH |",
                "|---|---|---|---:|---:|---:|---|",
            ]
        )
        for prop in props:
            chunk = [c for c in rows if c.get("prop") == prop]
            chunk.sort(
                key=lambda r: (
                    0 if r.get("kind") == "floor" else 1,
                    {"all": 0, "top2": 1, "lead": 2}.get(str(r.get("role")), 9),
                )
            )
            for c in chunk:
                lines.append(
                    f"| {c.get('prop')} | {c.get('role')} | {c.get('kind')} | "
                    f"{c.get('n')} | {c.get('hr')} | {c.get('wilson_low')} | "
                    f"{'Y' if c.get('bh_keep') else ''} |"
                )
        lines.append("")
    thin = [
        c
        for c in cells
        if c.get("thin")
        and c.get("kind") == "floor"
        and c.get("k") == 0
        and c.get("role") == "all"
        and int(c.get("n") or 0) > 0
        and not (c.get("sport") == "CFB" and c.get("prop") == "minutes")
    ]
    thin.sort(key=lambda r: (str(r.get("sport")), str(r.get("prop"))))
    if thin:
        lines.extend(
            [
                "## Measured but n<50 (positive L5 floor too rare)",
                "",
                "| Sport | Prop | n | HR |",
                "|---|---|---:|---:|",
            ]
        )
        for c in thin:
            lines.append(
                f"| {c.get('sport')} | {c.get('prop')} | {c.get('n')} | {c.get('hr')} |"
            )
        lines.append("")
    unsupported = [
        "Tennis / Golf — no box table in this run",
        "CBB boxes empty in proporacle_ref.db — out until attach exists",
        "Football YPC / longest reception / completions / period props — mapping-fail",
        "Quarters-with-X / NBA1Q — period boxes not used as a floor",
        "CFB minutes — ESPN payload MIN is a dummy (often 1.0), not a floor",
    ]
    lines.extend(["## Out until attach is fixed", ""])
    lines.extend(f"- {x}" for x in unsupported)
    md = dest / "floor_persistence_latest.md"
    md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    (dest / "floor_persistence_2026-10-25.md").write_text(md.read_text(encoding="utf-8"), encoding="utf-8")
    return latest_json


def run(*, db_path: Path | None = None, cache_dir: Path | None = None) -> dict[str, Any]:
    assert_prereg()
    db = db_path or DB
    cells: list[dict[str, Any]] = []
    missing: list[str] = []
    if not db.is_file():
        missing.append(f"missing db {db}")
        return {"error": "no db", "missing": missing, "cells": []}
    con = sqlite3.connect(str(db))
    try:
        have = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "nba" in have:
            print("floor_persistence: NBA", flush=True)
            cells.extend(load_nba_like(con, "nba", "NBA"))
        else:
            missing.append("nba")
        if "wnba" in have:
            print("floor_persistence: WNBA", flush=True)
            cells.extend(load_nba_like(con, "wnba", "WNBA"))
        else:
            missing.append("wnba")
        if "cbb" in have:
            print("floor_persistence: CBB", flush=True)
            cbb = load_nba_like(con, "cbb", "CBB")
            if cbb:
                cells.extend(cbb)
            else:
                missing.append("cbb_empty")
        if "nhl" in have:
            print("floor_persistence: NHL", flush=True)
            cells.extend(load_nhl(con))
        else:
            missing.append("nhl")
        if "soccer" in have:
            print("floor_persistence: Soccer", flush=True)
            cells.extend(load_soccer(con))
        else:
            missing.append("soccer")
        if "mlb_gamelog" in have:
            print("floor_persistence: MLB", flush=True)
            cells.extend(load_mlb(con))
        else:
            missing.append("mlb_gamelog")
        if "espn_boxscore_rows" in have:
            print("floor_persistence: CFB", flush=True)
            cells.extend(load_cfb(con))
        else:
            missing.append("espn_boxscore_rows")
    finally:
        con.close()
    print("floor_persistence: NFL", flush=True)
    cells.extend(load_nfl(cache_dir=cache_dir))
    scored = finalize(cells)
    path = write_reports(scored)
    return {
        "out": str(path),
        "n_cells": len(scored),
        "n50": sum(1 for c in scored if not c.get("thin")),
        "bh_keep": sum(1 for c in scored if c.get("bh_keep")),
        "missing": missing,
    }


def lookup_p_clear(
    sport: str,
    prop: str,
    role: str,
    k: int,
    *,
    path: Path | None = None,
) -> dict[str, Any] | None:
    dest = path or (OUT_DIR / "floor_persistence_lookup.csv")
    if not dest.is_file():
        return None
    df = pd.read_csv(dest)
    canon = canon_prop(sport, prop) or str(prop).strip().lower()
    hit = df[
        (df["sport"].astype(str).str.upper() == str(sport).upper())
        & (
            (df["prop"].astype(str) == str(prop))
            | (df["prop_canon"].astype(str) == canon)
        )
        & (df["role"].astype(str) == str(role))
        & (df["k"] == k)
    ]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()


def main() -> int:
    ap = argparse.ArgumentParser(description="Build the all-category floor-persistence table")
    ap.add_argument("--db", default="", help="Override proporacle_ref.db")
    args = ap.parse_args()
    res = run(db_path=Path(args.db) if args.db else None)
    print(json.dumps({k: v for k, v in res.items() if k != "cells"}, indent=2))
    return 0 if "error" not in res else 1


if __name__ == "__main__":
    raise SystemExit(main())
