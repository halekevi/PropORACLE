"""Last-10 stat windows from game logs, including the previous season.

Graded boards in ui_runner/templates start mid-2026. The sport archives in
data/cache/*_props_history.db start earlier in 2026, and the game logs go
back through the previous season. A slate in March can still have ten games.
"""
from __future__ import annotations

import csv
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

import prop_hit_tiers as T  # noqa: E402

CACHE = _REPO / "data" / "cache"
HISTORY = {
    "MLB": CACHE / "mlb_props_history.db",
    "WNBA": CACHE / "wnba_props_history.db",
    "NBA": CACHE / "nba_props_history.db",
    "NHL": CACHE / "nhl_props_history.db",
    "Soccer": CACHE / "soccer_props_history.db",
    "Tennis": CACHE / "tennis_props_history.db",
    "CBB": CACHE / "cbb_props_history.db",
    "WCBB": CACHE / "wcbb_props_history.db",
}
# (sport, player, prop) -> sorted (date, order, value), oldest first.
Series = dict[tuple[str, str, str], list[tuple[str, int, float]]]


def norm_player(s: object) -> str:
    import re

    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def _num(v) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def _iso(raw: object) -> str:
    text = str(raw or "").strip()
    if text.endswith(".0"):
        text = text[:-2]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10]


def _add(series: Series, sport: str, player: str, prop: str, date: str, order: int, value: float | None) -> None:
    if not player or not prop or not date or value is None:
        return
    series[(sport, player, prop)].append((date, order, value))


def _sort(series: Series) -> None:
    for rows in series.values():
        rows.sort()


def last10(series: Series, sport: str, player: str, prop: str, before: str) -> tuple:
    """Up to 10 games strictly before the slate, newest first. Empty when fewer than 8."""
    window = _window(series, sport, player, prop, before)
    if window:
        return window
    parts = player.split()
    if len(parts) < 2:
        return ()
    flipped = parts[-1] + " " + " ".join(parts[:-1])
    return _window(series, sport, flipped, prop, before)


def _window(series: Series, sport: str, player: str, prop: str, before: str) -> tuple:
    rows = series.get((sport, player, prop))
    if not rows:
        return ()
    # Same-day games belong to the slate, not the prior window.
    lo = 0
    hi = len(rows)
    while lo < hi:
        mid = (lo + hi) // 2
        if rows[mid][0] < before:
            lo = mid + 1
        else:
            hi = mid
    chunk = rows[max(0, lo - 10) : lo]
    if len(chunk) < 8:
        return ()
    return tuple(value for _, _, value in reversed(chunk))


def _minutes(raw) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.lower() in {"none", "nan"}:
        return None
    if ":" in text:
        mm, ss = text.split(":", 1)
        base = _num(mm)
        extra = _num(ss)
        if base is None:
            return None
        return base + (0.0 if extra is None else extra / 60.0)
    return _num(text)


def _sum3(a, b, c) -> float | None:
    vals = [_num(a), _num(b), _num(c)]
    if any(v is None for v in vals):
        return None
    return float(vals[0] + vals[1] + vals[2])


def _played(minutes) -> bool:
    """A known zero-minute row is a DNP and stays out of the window."""
    mins = _minutes(minutes)
    return mins is None or mins > 0


def load_series() -> Series:
    series: Series = defaultdict(list)
    _load_mlb(series)
    _load_wnba(series)
    _load_sqlite_logs(series)
    _load_tennis(series)
    _sort(series)
    return series


def _load_mlb(series: Series) -> None:
    cache = _REPO / "Sports" / "MLB" / "mlb_stats_cache.csv"
    ids = _REPO / "Sports" / "MLB" / "mlb_id_cache.csv"
    if not cache.is_file() or not ids.is_file():
        return
    name_of: dict[str, str] = {}
    with ids.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            pid = str(row.get("mlb_player_id") or "").strip()
            name = norm_player(row.get("player_norm"))
            if pid and name:
                name_of[pid] = name
    order = 0
    with cache.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            name = name_of.get(str(row.get("MLB_PLAYER_ID") or "").strip())
            prop = T.canon_prop("MLB", str(row.get("PROP_NORM") or ""))
            _add(series, "MLB", name or "", prop, _iso(row.get("GAME_DATE")), order, _num(row.get("STAT_VALUE")))
            order += 1


def _load_wnba(series: Series) -> None:
    path = _REPO / "Sports" / "WNBA" / "wnba_espn_cache.csv"
    if not path.is_file():
        return
    order = 0
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            if not _played(row.get("MIN")):
                order += 1
                continue
            name = norm_player(row.get("PLAYER_NORM") or row.get("PLAYER_NAME"))
            date = _iso(row.get("game_date"))
            pts, reb, ast = _num(row.get("PTS")), _num(row.get("REB")), _num(row.get("AST"))
            stl, blk = _num(row.get("STL")), _num(row.get("BLK"))
            mapped = {
                "points": pts,
                "rebounds": reb,
                "assists": ast,
                "threes": _num(row.get("FG3M")),
                "fga": _num(row.get("FGA")),
                "steals": stl,
                "blocks": blk,
                "pra": _sum3(pts, reb, ast),
                "pts+reb": None if pts is None or reb is None else pts + reb,
                "pts+ast": None if pts is None or ast is None else pts + ast,
                "reb+ast": None if reb is None or ast is None else reb + ast,
                "stocks": None if stl is None or blk is None else stl + blk,
            }
            for prop, value in mapped.items():
                _add(series, "WNBA", name, prop, date, order, value)
            order += 1


def _load_sqlite_logs(series: Series) -> None:
    path = CACHE / "historical_actuals.db"
    if not path.is_file():
        return
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    sql = """
        select sport, player_name, game_date, minutes, points, rebounds, assists,
               threes_made, fg_attempted, steals, blocks, pra, pts_rebs, pts_asts,
               rebs_asts, blks_stls, shots, shots_on_target, saves, goals
        from player_game_logs
    """
    order = 0
    for row in con.execute(sql):
        sport_raw = str(row["sport"] or "")
        sport = T.norm_sport(sport_raw)
        if sport not in {"NBA", "CBB", "WCBB", "NHL", "Soccer"}:
            continue
        if not _played(row["minutes"]):
            order += 1
            continue
        name = norm_player(row["player_name"])
        date = _iso(row["game_date"])
        for prop, value in _log_props(sport, row).items():
            _add(series, sport, name, prop, date, order, value)
        order += 1
    con.close()


def _log_props(sport: str, row) -> dict[str, float | None]:
    pts, reb, ast = _num(row["points"]), _num(row["rebounds"]), _num(row["assists"])
    stl, blk = _num(row["steals"]), _num(row["blocks"])
    if sport in {"NBA", "CBB", "WCBB"}:
        return {
            "points": pts,
            "rebounds": reb,
            "assists": ast,
            "threes": _num(row["threes_made"]),
            "fga": _num(row["fg_attempted"]),
            "steals": stl,
            "blocks": blk,
            "pra": _num(row["pra"]) if _num(row["pra"]) is not None else _sum3(pts, reb, ast),
            "pts+reb": _num(row["pts_rebs"]) if _num(row["pts_rebs"]) is not None else (None if pts is None or reb is None else pts + reb),
            "pts+ast": _num(row["pts_asts"]) if _num(row["pts_asts"]) is not None else (None if pts is None or ast is None else pts + ast),
            "reb+ast": _num(row["rebs_asts"]) if _num(row["rebs_asts"]) is not None else (None if reb is None or ast is None else reb + ast),
            "stocks": _num(row["blks_stls"]) if _num(row["blks_stls"]) is not None else (None if stl is None or blk is None else stl + blk),
        }
    if sport == "Soccer":
        return {
            "shots": _num(row["shots"]),
            "sog": _num(row["shots_on_target"]),
            "saves": _num(row["saves"]),
            "goals": _num(row["goals"]),
        }
    return {
        "sog": _num(row["shots"]),
        "points": pts,
        "assists": ast,
        "goals": _num(row["goals"]),
        "saves": _num(row["saves"]),
    }


def _load_tennis(series: Series) -> None:
    root = _REPO / "Sports" / "Tennis" / "data" / "sackmann"
    paths = [root / "atp_matches_combined.csv", root / "wta_matches_combined.csv"]
    if not any(p.is_file() for p in paths):
        return
    import pandas as pd

    sys.path.insert(0, str(_REPO / "Sports" / "Tennis" / "scripts"))
    from tennis_shared import build_sackmann_player_index

    frames = [pd.read_csv(p, low_memory=False) for p in paths if p.is_file()]
    index = build_sackmann_player_index(pd.concat(frames, ignore_index=True))
    for player, matches in index.items():
        name = norm_player(player)
        for rec in matches:
            date = _iso(rec.get("date"))
            order = int(rec.get("match_num") or 0)
            for prop in ("games_won", "match_total_games"):
                _add(series, "Tennis", name, prop, date, order, _num(rec.get(prop)))


def iter_history(skip_dates: set[str]):
    """Archive legs on dates the templates board does not already cover."""
    for sport, path in HISTORY.items():
        if not path.is_file():
            continue
        con = sqlite3.connect(path)
        sql = (
            "select grade_date, player_name, prop_type, line, direction, result, pick_type, "
            "team, opp_team, actual_value "
            "from props_history where result in ('HIT', 'MISS')"
        )
        for date, player, prop, line, direction, result, pick, team, opp, actual in con.execute(sql):
            day = str(date or "")[:10]
            if day in skip_dates:
                continue
            yield {
                "date": day,
                "sport": sport,
                "player": player,
                "prop": prop,
                "line": line,
                "direction": direction,
                "result": result,
                "pick_type": pick,
                "team": team,
                "opp_team": opp,
                "actual": actual,
            }
        con.close()
