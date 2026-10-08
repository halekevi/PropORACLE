"""PrizePicks vs Vegas player-prop cover, signed to the pick.

OVER: vegas − PP (positive = PP is the cheaper over).
UNDER: PP − vegas (positive = PP is the cheaper under).
Period slates (1Q/1H) must not join full-game Vegas lines.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Any

from utils.line_movement import (
    _pipeline_prop_to_odds_market as odds_market_for_prop,
)
from utils.player_name_utils import normalize_player_name

PERIOD_SPORTS = frozenset({"NBA1Q", "NBA1H", "WNBA1Q", "WNBA1H"})

SPORT_TO_ODDS_KEY: dict[str, str] = {
    "NBA": "basketball_nba",
    "NHL": "icehockey_nhl",
    "MLB": "baseball_mlb",
    "WNBA": "basketball_wnba",
    "Soccer": "soccer_epl",
    "NFL": "americanfootball_nfl",
    "CFB": "americanfootball_ncaaf",
}

GAP_BUCKETS: tuple[tuple[str, float, float], ...] = (
    ("against 2+", float("-inf"), -2.0),
    ("against 0-2", -2.0, 0.0),
    ("on 0-1", 0.0, 1.0),
    ("on 1-2", 1.0, 2.0),
    ("on 2+", 2.0, float("inf")),
)


def allows_vegas_join(sport: str) -> bool:
    return str(sport or "").strip().upper() not in PERIOD_SPORTS


def signed_cover(pp_line: float, vegas_line: float, side: str) -> float:
    """Positive = Vegas agrees the pick side is easier than the PP number."""
    s = str(side or "").strip().upper()
    if s in ("O", "OVER"):
        return float(vegas_line) - float(pp_line)
    if s in ("U", "UNDER"):
        return float(pp_line) - float(vegas_line)
    raise ValueError(f"side must be OVER or UNDER, got {side!r}")


def gap_bucket(gap: float) -> str:
    for name, lo, hi in GAP_BUCKETS:
        if lo <= gap < hi:
            return name
    return "on 2+"


def player_key(name: str) -> str:
    return normalize_player_name(name).strip().lower()


def odds_market(sport: str, prop: str) -> str:
    if not allows_vegas_join(sport):
        return ""
    sk = SPORT_TO_ODDS_KEY.get(str(sport or "").strip())
    if not sk:
        return ""
    m = str(odds_market_for_prop(str(prop or ""), sk) or "").strip().lower()
    if m.startswith("player_") or m.startswith("batter_") or m.startswith("pitcher_"):
        return m
    return ""


def nearest_vegas_line(
    index: dict[tuple[str, str, str], list[float]],
    *,
    sport: str,
    player: str,
    prop: str,
    pp_line: float,
    max_dist: float = 6.0,
) -> float | None:
    """Closest Vegas number for player+market. Rejects period sports."""
    mkt = odds_market(sport, prop)
    if not mkt:
        return None
    key = (str(sport).strip(), player_key(player), mkt)
    lines = index.get(key) or []
    if not lines:
        return None
    best = min(lines, key=lambda v: abs(v - pp_line))
    if abs(best - pp_line) > max_dist:
        return None
    return best


def build_vegas_index(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], list[float]]:
    """Index vegas_props.csv-like dicts: board_sport, player, odds_market, line."""
    out: dict[tuple[str, str, str], list[float]] = {}
    for r in rows:
        sport = str(r.get("board_sport") or r.get("sport") or "").strip()
        if not allows_vegas_join(sport):
            continue
        try:
            line = float(r.get("line"))
        except (TypeError, ValueError):
            continue
        mkt = str(r.get("odds_market") or "").strip().lower()
        if not mkt:
            mkt = odds_market(sport, str(r.get("prop_type") or r.get("prop") or ""))
        if not mkt:
            continue
        pk = player_key(str(r.get("player") or ""))
        if not pk:
            continue
        out.setdefault((sport, pk, mkt), []).append(line)
    return out


GAME_SPORT_PARENT: dict[str, str] = {
    "NBA1Q": "NBA",
    "NBA1H": "NBA",
    "WNBA1Q": "WNBA",
    "WNBA1H": "WNBA",
}


def parent_game_sport(sport: str) -> str:
    s = str(sport or "").strip()
    return GAME_SPORT_PARENT.get(s.upper(), s)


def _blank(v: Any) -> bool:
    if v is None:
        return True
    s = str(v).strip()
    return s == "" or s in ("—", "-", "nan", "None")


def _as_float(v: Any) -> float | None:
    if _blank(v):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def team_perspective_spread(home_spread: Any, *, player_is_home: bool) -> float | None:
    """Negative means the prop player's team is favored."""
    if home_spread is None or str(home_spread).strip() in ("", "—", "-", "nan", "None"):
        return None
    try:
        hs = float(home_spread)
    except (TypeError, ValueError):
        return None
    return hs if player_is_home else -hs


def build_game_index(
    rows: list[dict[str, Any]],
    *,
    abbrs_for_id: dict[tuple[str, str], list[str]] | None = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """(sport, team abbr or 'id:<espn id>') → game line.

    `spread` on the record stays home-signed (negative = home favored).
    Extra abbreviations from the team-id crosswalk are indexed too, so SAS and SA
    land on the same game.
    """
    out: dict[tuple[str, str], dict[str, Any]] = {}
    aliases = abbrs_for_id or {}
    for r in rows:
        sport = str(r.get("sport") or "").strip()
        if not sport:
            continue
        rec = {
            "spread": r.get("spread"),
            "total": r.get("total"),
            "home_team": r.get("home_team"),
            "away_team": r.get("away_team"),
            "home_team_id": str(r.get("home_team_id") or "").strip(),
            "away_team_id": str(r.get("away_team_id") or "").strip(),
            "home_win_prob": r.get("home_win_prob"),
            "away_win_prob": r.get("away_win_prob"),
            "provider": r.get("provider"),
            "is_closing": r.get("is_closing"),
            "captured_at": r.get("captured_at"),
        }
        keys: list[str] = []
        for team in (r.get("home_team"), r.get("away_team")):
            ab = str(team or "").strip().upper()
            if ab:
                keys.append(ab)
        for tid, side_keys in (
            (rec["home_team_id"], aliases.get((sport, rec["home_team_id"]), [])),
            (rec["away_team_id"], aliases.get((sport, rec["away_team_id"]), [])),
        ):
            if tid:
                keys.append(f"id:{tid}")
            for ab in side_keys:
                cleaned = str(ab or "").strip().upper()
                if cleaned:
                    keys.append(cleaned)
        for key in keys:
            out[(sport, key)] = rec
    return out


def apply_vegas_to_prop(
    prop: dict[str, Any],
    vegas_index: dict[tuple[str, str, str], list[float]],
) -> bool:
    """Fill line_vegas + vegas_gap. Returns True if a Vegas number was attached."""
    sport = str(prop.get("sport") or "").strip()
    if not allows_vegas_join(sport):
        return False
    pp = _as_float(prop.get("line"))
    if pp is None:
        return False
    existing = _as_float(prop.get("line_vegas"))
    vg = existing
    attached = False
    if vg is None:
        found = nearest_vegas_line(
            vegas_index,
            sport=sport,
            player=str(prop.get("player") or ""),
            prop=str(prop.get("prop") or prop.get("prop_type") or ""),
            pp_line=pp,
        )
        if found is None:
            return False
        vg = found
        prop["line_vegas"] = str(round(vg, 2))
        attached = True
    side = str(prop.get("direction") or prop.get("over_under") or "")
    try:
        prop["vegas_gap"] = str(round(signed_cover(pp, vg, side), 3))
    except (TypeError, ValueError):
        pass
    return attached


def apply_game_lines_to_prop(
    prop: dict[str, Any],
    game_index: dict[tuple[str, str], dict[str, Any]],
    *,
    abbr_to_ids: dict[tuple[str, str], set[str]] | None = None,
) -> bool:
    """Fill the player's-team spread, total, and win probability.

    Join key is the ESPN team id. An abbreviation is only a lookup into that id.
    `game_spread` is signed for the prop player's team (negative = that team favored).
    Period slates use the full-game environment.
    """
    sport = parent_game_sport(str(prop.get("sport") or ""))
    abbr = str(prop.get("team") or "").strip().upper()
    if not abbr or abbr in ("—", "-") or "/" in abbr:
        return False
    team_id = ""
    mapped = (abbr_to_ids or {}).get((sport, abbr)) or set()
    if len(mapped) == 1:
        team_id = next(iter(mapped))
    rec = game_index.get((sport, f"id:{team_id}")) if team_id else None
    if rec is None:
        rec = game_index.get((sport, abbr))
    if not rec:
        return False

    home_id = str(rec.get("home_team_id") or "").strip()
    away_id = str(rec.get("away_team_id") or "").strip()
    home_abbr = str(rec.get("home_team") or "").strip().upper()
    away_abbr = str(rec.get("away_team") or "").strip().upper()
    if team_id and team_id == home_id:
        player_is_home = True
    elif team_id and team_id == away_id:
        player_is_home = False
    elif abbr == home_abbr:
        player_is_home = True
    elif abbr == away_abbr:
        player_is_home = False
    else:
        return False

    attached = False
    team_spread = team_perspective_spread(rec.get("spread"), player_is_home=player_is_home)
    if _blank(prop.get("game_spread")) and team_spread is not None:
        prop["game_spread"] = str(team_spread)
        attached = True
    if _blank(prop.get("game_total")) and rec.get("total") is not None:
        prop["game_total"] = str(rec["total"])
        attached = True
    win_p = rec.get("home_win_prob") if player_is_home else rec.get("away_win_prob")
    if _blank(prop.get("team_win_prob")) and win_p is not None:
        try:
            prop["team_win_prob"] = str(round(float(win_p), 4))
            attached = True
        except (TypeError, ValueError):
            pass
    if _blank(prop.get("line_provider")) and rec.get("provider"):
        prop["line_provider"] = str(rec["provider"])
        attached = True
    if _blank(prop.get("line_is_closing")) and rec.get("is_closing") is not None:
        prop["line_is_closing"] = str(int(rec["is_closing"]))
        attached = True
    if _blank(prop.get("line_captured_at")) and rec.get("captured_at"):
        prop["line_captured_at"] = str(rec["captured_at"])
        attached = True
    return attached


def enrich_props(
    props: list[dict[str, Any]],
    *,
    vegas_index: dict[tuple[str, str, str], list[float]] | None = None,
    game_index: dict[tuple[str, str], dict[str, Any]] | None = None,
    abbr_to_ids: dict[tuple[str, str], set[str]] | None = None,
) -> int:
    """Mutate graded/API prop dicts. Returns count of props that gained a Vegas line."""
    n = 0
    vg = vegas_index or {}
    gm = game_index or {}
    for p in props:
        if not isinstance(p, dict):
            continue
        if apply_vegas_to_prop(p, vg):
            n += 1
        apply_game_lines_to_prop(p, gm, abbr_to_ids=abbr_to_ids)
    return n


def is_grading_suspect(row: dict[str, Any]) -> bool:
    v = row.get("grading_suspect")
    if v is True or v == 1:
        return True
    if isinstance(v, str) and v.strip().lower() in ("1", "true", "yes"):
        return True
    return False


def vegas_csv_candidates(date_str: str, *repos: Path) -> list[Path]:
    """outputs/<date>/vegas_props.csv then the durable cache copy."""
    out: list[Path] = []
    for root in repos:
        if root is None:
            continue
        out.append(Path(root) / "outputs" / date_str / "vegas_props.csv")
        out.append(Path(root) / "data" / "cache" / "vegas_props" / f"{date_str}.csv")
    return out


def find_vegas_csv(
    date_str: str,
    *,
    explicit: str = "",
    repos: list[Path] | tuple[Path, ...] | None = None,
) -> Path | None:
    if explicit:
        p = Path(explicit)
        if p.is_file():
            return p
    for p in vegas_csv_candidates(date_str, *(repos or ())):
        if p.is_file():
            return p
    return None


def archive_vegas_csv(src: Path, date_str: str, repo: Path) -> Path | None:
    """Keep a copy that survives outputs/ wipes so next-day grades can join."""
    if not src.is_file() or not str(date_str or "").strip():
        return None
    dest = Path(repo) / "data" / "cache" / "vegas_props" / f"{str(date_str).strip()[:10]}.csv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return dest


STEP8_FILE_SPORT: dict[str, str] = {
    "mlb": "MLB",
    "wnba": "WNBA",
    "nba": "NBA",
    "nhl": "NHL",
    "soccer": "Soccer",
    "nfl": "NFL",
    "cfb": "CFB",
    "cbb": "CBB",
}


def sport_from_step8_name(name: str) -> str | None:
    """Map step8_mlb_direction_clean.xlsx → MLB. Period slates return None."""
    n = str(name or "").lower()
    if "1q" in n or "1h" in n:
        return None
    m = re.search(r"step8_([a-z0-9]+)_", n)
    if not m:
        return None
    return STEP8_FILE_SPORT.get(m.group(1))


def real_vegas_line(
    *,
    pp_line: float | None,
    open_line: float | None,
    line_vegas: float | None,
    implied_prob: float | None,
    line_movement: float | None,
) -> float | None:
    """Odds API / book number, not the PP fallback that copies the PrizePicks line.

    open_line is filled with the PP line when the snapshot misses, so same-number
    rows only count when implied_prob or line_movement says a book actually matched.
    """
    if line_vegas is not None:
        return line_vegas
    book_hit = implied_prob is not None
    try:
        if line_movement is not None and abs(float(line_movement)) > 1e-9:
            book_hit = True
    except (TypeError, ValueError):
        pass
    if pp_line is not None and open_line is not None and abs(open_line - pp_line) > 0.04:
        return open_line
    if book_hit and open_line is not None:
        return open_line
    return None


def slate_rows_to_vegas(
    rows: list[dict[str, Any]],
    *,
    board_sport: str,
) -> list[dict[str, Any]]:
    """Turn step8 / Full Slate dicts into vegas_props.csv-shaped rows."""
    if not allows_vegas_join(board_sport):
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        player = str(r.get("player") or r.get("Player") or "").strip()
        prop = str(
            r.get("prop")
            or r.get("Prop")
            or r.get("prop_type")
            or r.get("Prop Type")
            or ""
        ).strip()
        if not player or not prop:
            continue
        pp = _as_float(r.get("line") or r.get("Line"))
        vg = real_vegas_line(
            pp_line=pp,
            open_line=_as_float(r.get("open_line") or r.get("Open Line")),
            line_vegas=_as_float(r.get("line_vegas") or r.get("Line (LV)")),
            implied_prob=_as_float(r.get("implied_prob")),
            line_movement=_as_float(r.get("line_movement")),
        )
        if vg is None:
            continue
        mkt = odds_market(board_sport, prop)
        out.append(
            {
                "player": player,
                "prop_type": prop,
                "line": vg,
                "pick_type": "Standard",
                "source_book": "vegas",
                "board_sport": board_sport,
                "odds_market": mkt,
                "vegas_bookmaker": str(r.get("implied_prob_book") or "").strip(),
            }
        )
    return out
