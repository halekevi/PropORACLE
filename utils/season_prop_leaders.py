"""Season league-leaders + vs-line consistency stamps for board rows.

Observe-only. Does not change ticket gates.

Per (sport family × prop_canon):
  - rank players by season average of the box stat (min games)
  - consistency = prior games this season clearing *today's* line

Flags stamped on each row:
  season_prop_rank, season_prop_n, season_prop_avg,
  season_line_hits, season_line_n, season_line_hit_rate,
  season_leader_flag  (LEADER | TOP5 | TOP10 | \"\"),
  season_consistent   (bool),
  season_leader_tag   (print string, e.g. \"LEAD#2 CONSIST 11/13\")
"""
from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from utils.pp_board_prop_map import box_cols_for
from utils.prop_norm import canon_prop
from utils.proporacle_ref_db import resolve_ref_db

_REPO = Path(__file__).resolve().parents[1]
_MAIN = Path(r"H:\PropORACLE_main_cp")

MIN_GAMES_RANK = 5
MIN_GAMES_CONSIST = 8
CONSIST_RATE = 0.70
LEADER_TOP = 3
TOP5 = 5
TOP10 = 10

_FOLD_RE = re.compile(r"[^a-z0-9]+")


def _fold(s: object) -> str:
    return _FOLD_RE.sub("", str(s or "").lower())


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _sport_family(sport: str) -> str:
    s = str(sport or "").strip().upper()
    if s in {"NFLP"}:
        return "NFL"
    if s in {"CFB1H"}:
        return "CFB"
    if s in {"NBA1Q", "NBA1H", "NBA2H"}:
        return "NBA"
    if s in {"WNBA1Q", "WNBA1H"}:
        return "WNBA"
    if s in {"WCBB"}:
        return "CBB"
    return s


def _season_start(family: str, as_of: str) -> str:
    try:
        y = int(str(as_of)[:4])
        m = int(str(as_of)[5:7])
    except (TypeError, ValueError):
        return f"{datetime.now().year}-01-01"
    if family in {"NFL", "CFB"}:
        # Football season spans calendar years; Aug 1 start.
        return f"{y}-08-01" if m >= 8 else f"{y - 1}-08-01"
    if family in {"NBA", "WNBA", "CBB", "NHL"}:
        return f"{y}-10-01" if m >= 10 else f"{y - 1}-10-01"
    if family == "MLB":
        return f"{y}-03-01"
    if family == "SOCCER":
        # Rolling calendar year window.
        return f"{y}-01-01"
    return f"{y}-01-01"


# Football CSV caches use UPPER box names; prop map uses lower keys.
_FOOTBALL_BOX: dict[str, tuple[str, ...]] = {
    "pass_yds": ("PASS_YDS",),
    "passing_yards": ("PASS_YDS",),
    "rush_yds": ("RUSH_YDS",),
    "rushing_yards": ("RUSH_YDS",),
    "rec_yds": ("REC_YDS",),
    "receiving_yards": ("REC_YDS",),
    "receptions": ("REC",),
    "rec": ("REC",),
    "pass_td": ("PASS_TD",),
    "pass_tds": ("PASS_TD",),
    "passing_tds": ("PASS_TD",),
    "rush_td": ("RUSH_TD",),
    "rush_tds": ("RUSH_TD",),
    "rushing_tds": ("RUSH_TD",),
    "rec_td": ("REC_TD",),
    "rec_tds": ("REC_TD",),
    "receiving_tds": ("REC_TD",),
    "pass_att": ("PASS_ATT",),
    "pass_attempts": ("PASS_ATT",),
    "rush_att": ("RUSH_ATT",),
    "rush_attempts": ("RUSH_ATT",),
    "pass_comp": ("PASS_CMP",),
    "pass_completions": ("PASS_CMP",),
    "pass_int": ("PASS_INT",),
    "pass_ints": ("PASS_INT",),
    "pass_rush_yds": ("PASS_YDS", "RUSH_YDS"),
    "rush_rec_yds": ("RUSH_YDS", "REC_YDS"),
    "kick_pts": ("KICK_PTS",),
    "kicking_points": ("KICK_PTS",),
    "tackles": ("TACK_TOT",),
    "sacks": ("SACK",),
}

_SOCCER_BOX: dict[str, tuple[str, ...]] = {
    "shots": ("sh",),
    "shots_on_target": ("sog",),
    "sot": ("sog",),
    "goals": ("g",),
    "assists": ("a",),
    "saves": ("sv",),
    "goal_assists": ("g", "a"),
    "g_a": ("g", "a"),
}

_NHL_BOX: dict[str, tuple[str, ...]] = {
    "goals": ("goals",),
    "assists": ("assists",),
    "points": ("points",),
    "shots": ("shots_on_goal",),
    "sog": ("shots_on_goal",),
    "shots_on_goal": ("shots_on_goal",),
    "hits": ("hits",),
    "blocked_shots": ("blocked_shots",),
    "blocks": ("blocked_shots",),
    "saves": ("saves",),
}


def _box_cols(family: str, prop_canon: str) -> tuple[str, ...]:
    if family in {"NFL", "CFB"}:
        if prop_canon in _FOOTBALL_BOX:
            return _FOOTBALL_BOX[prop_canon]
        # Fall back: uppercase map keys from pp_board_prop_map.
        raw = box_cols_for(family if family != "CFB" else "NFL", prop_canon)
        if raw:
            return tuple(c.upper() for c in raw)
        return (prop_canon.upper(),)
    if family == "SOCCER":
        return _SOCCER_BOX.get(prop_canon) or box_cols_for("Soccer", prop_canon) or ()
    if family == "NHL":
        return _NHL_BOX.get(prop_canon) or box_cols_for("NHL", prop_canon) or ()
    # Hoops / default — lower cols from map.
    sport = {"WNBA": "WNBA", "CBB": "CBB", "NBA": "NBA"}.get(family, family)
    return box_cols_for(sport, prop_canon) or (prop_canon,)


def _football_cache(family: str) -> Path | None:
    name = "nfl_boxscore_cache.csv" if family == "NFL" else "cfb_boxscore_cache.csv"
    sport = "NFL" if family == "NFL" else "CFB"
    for root in (_MAIN, _REPO):
        p = root / "Sports" / sport / "data" / "cache" / name
        if p.is_file():
            return p
    return None


def _row_stat(row: dict[str, Any], cols: tuple[str, ...]) -> float | None:
    if not cols:
        return None
    total = 0.0
    any_ok = False
    for c in cols:
        v = _f(row.get(c))
        if v is None:
            continue
        total += v
        any_ok = True
    return total if any_ok else None


@dataclass(frozen=True)
class PlayerSeasonStat:
    player_fold: str
    player: str
    n: int
    avg: float
    values: tuple[float, ...]  # oldest→newest, prior games only
    dates: tuple[str, ...]


def _load_football_logs(
    family: str, *, start: str, as_of: str
) -> pd.DataFrame:
    path = _football_cache(family)
    if path is None:
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    if "game_date" not in df.columns:
        return pd.DataFrame()
    g = df["game_date"].astype(str).str.replace("-", "", regex=False)
    # caches sometimes store YYYYMMDD
    if g.str.len().median() == 8:
        df = df.copy()
        df["game_date"] = (
            g.str.slice(0, 4) + "-" + g.str.slice(4, 6) + "-" + g.str.slice(6, 8)
        )
    df = df[
        (df["game_date"].astype(str) >= start)
        & (df["game_date"].astype(str) < as_of)
    ]
    return df


def _load_ref_logs(table: str, *, start: str, as_of: str) -> pd.DataFrame:
    db = resolve_ref_db()
    if db is None or not Path(db).is_file():
        return pd.DataFrame()
    con = sqlite3.connect(str(db))
    try:
        tables = {
            r[0] for r in con.execute("select name from sqlite_master where type='table'")
        }
        if table not in tables:
            return pd.DataFrame()
        df = pd.read_sql_query(
            f"select * from {table} where game_date >= ? and game_date < ?",
            con,
            params=(start, as_of),
        )
    finally:
        con.close()
    return df


def _volume_mask(df: pd.DataFrame, family: str, prop_canon: str) -> pd.Series:
    """Keep games where the player actually played that role (no zero-volume pads)."""
    p = prop_canon
    if family in {"NFL", "CFB"}:
        if p in {
            "pass_yds",
            "passing_yards",
            "pass_td",
            "pass_tds",
            "passing_tds",
            "pass_att",
            "pass_attempts",
            "pass_comp",
            "pass_completions",
            "pass_int",
            "pass_ints",
            "pass_rush_yds",
        }:
            # PASS_ATT is often blank on older cache rows — accept yards/comps too.
            att = (
                pd.to_numeric(df["PASS_ATT"], errors="coerce")
                if "PASS_ATT" in df.columns
                else None
            )
            yds = (
                pd.to_numeric(df["PASS_YDS"], errors="coerce")
                if "PASS_YDS" in df.columns
                else None
            )
            cmp_ = (
                pd.to_numeric(df["PASS_CMP"], errors="coerce")
                if "PASS_CMP" in df.columns
                else None
            )
            m = pd.Series(False, index=df.index)
            if att is not None:
                m = m | att.fillna(0).gt(0)
            if yds is not None:
                m = m | yds.fillna(0).gt(0)
            if cmp_ is not None:
                m = m | cmp_.fillna(0).gt(0)
            return m
        if p in {
            "rush_yds",
            "rushing_yards",
            "rush_td",
            "rush_tds",
            "rushing_tds",
            "rush_att",
            "rush_attempts",
            "rush_rec_yds",
        }:
            att = (
                pd.to_numeric(df["RUSH_ATT"], errors="coerce")
                if "RUSH_ATT" in df.columns
                else None
            )
            yds = (
                pd.to_numeric(df["RUSH_YDS"], errors="coerce")
                if "RUSH_YDS" in df.columns
                else None
            )
            m = pd.Series(False, index=df.index)
            if att is not None:
                m = m | att.fillna(0).gt(0)
            if yds is not None:
                m = m | yds.fillna(0).abs().gt(0)
            return m
        if p in {
            "rec_yds",
            "receiving_yards",
            "receptions",
            "rec",
            "rec_td",
            "rec_tds",
            "receiving_tds",
        }:
            tgt = (
                pd.to_numeric(df["REC_TGT"], errors="coerce")
                if "REC_TGT" in df.columns
                else None
            )
            rec = (
                pd.to_numeric(df["REC"], errors="coerce")
                if "REC" in df.columns
                else None
            )
            yds = (
                pd.to_numeric(df["REC_YDS"], errors="coerce")
                if "REC_YDS" in df.columns
                else None
            )
            m = pd.Series(False, index=df.index)
            if tgt is not None:
                m = m | tgt.fillna(0).gt(0)
            if rec is not None:
                m = m | rec.fillna(0).gt(0)
            if yds is not None:
                m = m | yds.fillna(0).gt(0)
            return m
    if family in {"NBA", "WNBA", "CBB"}:
        if "minutes" in df.columns:
            return pd.to_numeric(df["minutes"], errors="coerce").fillna(0) > 0
    if family == "NHL" and "toi" in df.columns:
        # toi often "12:34"
        return df["toi"].astype(str).str.len() > 0
    if family == "SOCCER" and "minutes_played" in df.columns:
        return pd.to_numeric(df["minutes_played"], errors="coerce").fillna(0) > 0
    return pd.Series(True, index=df.index)


def _player_series(
    df: pd.DataFrame,
    *,
    family: str,
    prop_canon: str,
    player_col: str,
) -> dict[str, PlayerSeasonStat]:
    cols = _box_cols(family, prop_canon)
    if not cols or df.empty:
        return {}
    missing = [c for c in cols if c not in df.columns]
    if missing:
        return {}
    out: dict[str, PlayerSeasonStat] = {}
    work = df.copy()
    work = work[_volume_mask(work, family, prop_canon)]
    if work.empty:
        return {}
    work["_stat"] = 0.0
    for c in cols:
        work["_stat"] = work["_stat"] + pd.to_numeric(work[c], errors="coerce").fillna(0.0)
    # Drop rows where every component was NaN originally
    present = pd.Series(False, index=work.index)
    for c in cols:
        present = present | pd.to_numeric(work[c], errors="coerce").notna()
    work = work[present]
    work = work.sort_values("game_date")
    for player, g in work.groupby(player_col, sort=False):
        vals = [float(v) for v in g["_stat"].tolist() if v == v]
        if not vals:
            continue
        dates = [str(d)[:10] for d in g["game_date"].tolist()][: len(vals)]
        key = _fold(player)
        out[key] = PlayerSeasonStat(
            player_fold=key,
            player=str(player),
            n=len(vals),
            avg=sum(vals) / len(vals),
            values=tuple(vals),
            dates=tuple(dates[: len(vals)]),
        )
    return out


@lru_cache(maxsize=32)
def _season_index(family: str, as_of: str) -> dict[str, dict[str, PlayerSeasonStat]]:
    """prop_canon → player_fold → PlayerSeasonStat."""
    start = _season_start(family, as_of)
    by_prop: dict[str, dict[str, PlayerSeasonStat]] = {}

    if family in {"NFL", "CFB"}:
        df = _load_football_logs(family, start=start, as_of=as_of)
        if df.empty:
            return {}
        player_col = "player_norm" if "player_norm" in df.columns else "player"
        props = set(_FOOTBALL_BOX.keys())
        for prop in props:
            by_prop[prop] = _player_series(
                df, family=family, prop_canon=prop, player_col=player_col
            )
        return by_prop

    table = {
        "NBA": "nba",
        "WNBA": "wnba",
        "CBB": "cbb",
        "NHL": "nhl",
        "SOCCER": "soccer",
    }.get(family)
    if not table:
        return {}
    df = _load_ref_logs(table, start=start, as_of=as_of)
    if df.empty or "player" not in df.columns:
        return {}
    # Discover props from map + soccer/nhl helpers
    from utils.pp_board_prop_map import categories_for_sport

    sport_label = {
        "NBA": "NBA",
        "WNBA": "WNBA",
        "CBB": "CBB",
        "NHL": "NHL",
        "SOCCER": "Soccer",
    }[family]
    props = [c.key for c in categories_for_sport(sport_label)]
    if family == "SOCCER":
        props = list(dict.fromkeys([*props, *_SOCCER_BOX.keys()]))
    if family == "NHL":
        props = list(dict.fromkeys([*props, *_NHL_BOX.keys()]))
    for prop in props:
        series = _player_series(df, family=family, prop_canon=prop, player_col="player")
        if series:
            by_prop[prop] = series
    return by_prop


_EXCLUDE_NAMES = frozenset({"team", "total", "offense", "defense", "totals", ""})


def _ranks_for_prop(
    series: dict[str, PlayerSeasonStat],
) -> dict[str, tuple[int, int]]:
    """player_fold → (rank, pool_n) among players with n >= MIN_GAMES_RANK."""
    eligible = [
        (k, s)
        for k, s in series.items()
        if s.n >= MIN_GAMES_RANK
        and s.avg > 0
        and k not in _EXCLUDE_NAMES
        and _fold(s.player) not in _EXCLUDE_NAMES
    ]
    eligible.sort(key=lambda kv: (-kv[1].avg, -kv[1].n, kv[0]))
    pool = len(eligible)
    out: dict[str, tuple[int, int]] = {}
    for i, (k, _s) in enumerate(eligible, start=1):
        out[k] = (i, pool)
    return out


def _consistency(
    stat: PlayerSeasonStat, *, line: float, side: str
) -> tuple[int, int, float | None]:
    if not stat.values:
        return 0, 0, None
    hits = 0
    n = 0
    side_u = str(side or "OVER").upper()
    for v in stat.values:
        n += 1
        if side_u == "UNDER":
            if v < line:
                hits += 1
        else:
            if v > line:
                hits += 1
    rate = hits / n if n else None
    return hits, n, rate


def leader_flag(rank: int | None) -> str:
    if rank is None:
        return ""
    if rank <= LEADER_TOP:
        return "LEADER"
    if rank <= TOP5:
        return "TOP5"
    if rank <= TOP10:
        return "TOP10"
    return ""


def annotate_rows(rows: Iterable[dict[str, Any]], *, as_of: str) -> int:
    """Stamp season leader / consistency fields onto board rows. Returns # tagged."""
    tagged = 0
    as_of = str(as_of)[:10]
    cache_ranks: dict[tuple[str, str], dict[str, tuple[int, int]]] = {}
    for r in rows:
        sport = str(r.get("sport") or "")
        family = _sport_family(sport)
        if family in {"GOLF", "TENNIS", "MLB"}:
            # No reliable season box path yet (MLB table empty; tennis/golf separate).
            continue
        prop = canon_prop(family if family != "SOCCER" else "Soccer", r.get("prop") or r.get("prop_type"))
        if not prop:
            continue
        idx = _season_index(family, as_of)
        series = idx.get(prop) or {}
        if not series:
            continue
        key = (family, prop)
        if key not in cache_ranks:
            cache_ranks[key] = _ranks_for_prop(series)
        ranks = cache_ranks[key]
        pf = _fold(r.get("player"))
        stat = series.get(pf)
        if stat is None:
            continue
        rank_t = ranks.get(pf)
        rank = rank_t[0] if rank_t else None
        pool = rank_t[1] if rank_t else 0
        line = _f(r.get("line"))
        side = str(r.get("side") or "OVER")
        hits, n_c, rate = (0, 0, None)
        if line is not None:
            hits, n_c, rate = _consistency(stat, line=line, side=side)
        flag = leader_flag(rank)
        consistent = bool(
            rate is not None and n_c >= MIN_GAMES_CONSIST and rate >= CONSIST_RATE
        )
        r["season_prop_rank"] = rank
        r["season_prop_pool"] = pool or None
        r["season_prop_n"] = stat.n
        r["season_prop_avg"] = round(stat.avg, 2)
        r["season_line_hits"] = hits
        r["season_line_n"] = n_c
        r["season_line_hit_rate"] = None if rate is None else round(rate, 4)
        r["season_leader_flag"] = flag or None
        r["season_consistent"] = consistent
        bits: list[str] = []
        if flag and rank is not None:
            bits.append(f"{flag}#{rank}/{pool}" if pool else f"{flag}#{rank}")
        if consistent and rate is not None:
            bits.append(f"CONSIST {hits}/{n_c}")
        elif n_c >= MIN_GAMES_CONSIST and rate is not None:
            bits.append(f"sznLine {hits}/{n_c}")
        r["season_leader_tag"] = " ".join(bits) if bits else None
        if bits:
            tagged += 1
    return tagged


def clear_caches() -> None:
    _season_index.cache_clear()
