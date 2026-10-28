"""NBA L5 card window (RS + playoffs) with preseason as an early-season bridge.

PrizePicks cards show the 5 newest regular-season and playoff games — no
preseason. That is the L5=5 gate window. Never pad the card window with
preseason (one-time ESPN preseason DB backfill is unrelated ingest).

Preseason is a separate rank flag only while the player has fewer than
``PRE_BRIDGE_MIN_RS`` current-season regular-season games. After that the
``pre_`` bucket is retired (not shown, not ranked).
"""
from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any

import pandas as pd

from utils.cushion_z import l5_clears
from utils.nba_board_team import first_regular_season_date_public as first_regular_season_date
from utils.nba_minutes_role import _norm_name

PROP_COL = {
    "points": "pts",
    "pts": "pts",
    "rebounds": "reb",
    "reb": "reb",
    "assists": "ast",
    "ast": "ast",
    "steals": "stl",
    "stl": "stl",
    "blocked shots": "blk",
    "blocks": "blk",
    "blk": "blk",
    "turnovers": "tov",
    "tov": "tov",
    "3-pt made": "fg3m",
    "3ptm": "fg3m",
    "3-ptm": "fg3m",
    "3pm": "fg3m",
    "fg3m": "fg3m",
    # Attempt / volume props (role-sticky; Demons included on edge scan)
    "fg attempted": "fga",
    "field goals attempted": "fga",
    "fga": "fga",
    "fg made": "fgm",
    "field goals made": "fgm",
    "fgm": "fgm",
    "two pointers attempted": "fg2a",
    "2pt attempted": "fg2a",
    "2pa": "fg2a",
    "fg2a": "fg2a",
    "two pointers made": "fg2m",
    "2pt made": "fg2m",
    "2pm": "fg2m",
    "fg2m": "fg2m",
    "3-pt attempted": "fg3a",
    "3pt attempted": "fg3a",
    "three pointers attempted": "fg3a",
    "3pa": "fg3a",
    "fg3a": "fg3a",
    "free throws attempted": "fta",
    "ft attempted": "fta",
    "fta": "fta",
    "free throws made": "ftm",
    "ft made": "ftm",
    "ftm": "ftm",
    "pts+rebs+asts": "pra",
    "pts+reb+ast": "pra",
    "pra": "pra",
    "pts+rebs": "pr",
    "pts+reb": "pr",
    "pr": "pr",
    "pts+asts": "pa",
    "pts+ast": "pa",
    "pa": "pa",
    "rebs+asts": "ra",
    "reb+ast": "ra",
    "ra": "ra",
}

EARLY_SEASON_DAYS = 14  # retained for opener / minutes helpers only
CARD_WINDOWS = frozenset({"regular", "playoffs"})
PRE_WINDOWS = frozenset({"preseason"})
PRE_THIN_MIN = 2  # need ≥2 preseason games to judge confirms/conflicts
# Retire pre_ once the player has this many current-season RS boxes (minutes>0).
# ~Oct 27–30 for a typical 2026 opener slate when teams hit 5 RS games.
PRE_BRIDGE_MIN_RS = 5
PRE_RETIRED = "pre_retired"
DRTG_MIN_TEAM_GAMES = 10

_WINDOW_TAG = {
    "preseason": "P",
    "regular": "R",
    "playoffs": "O",
}


def prop_col(prop: str) -> str | None:
    key = str(prop or "").strip().lower().replace("(combo)", "").strip()
    return PROP_COL.get(key)


def _as_date(d: date | datetime | str | None) -> date | None:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    s = str(d).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def nba_rs_opener(slate: date | datetime | str | None = None) -> date | None:
    """First regular-season game date from the box cache (not a hardcoded Oct 22)."""
    d = _as_date(slate) or date.today()
    return first_regular_season_date(as_of=d)


def is_early_season(slate: date | datetime | str | None) -> bool:
    """True from opener through the first 14 RS calendar days (context only)."""
    d = _as_date(slate)
    if d is None:
        return False
    opener = nba_rs_opener(d)
    if opener is None:
        return False
    if d < opener:
        return (opener - d).days <= 1
    return (d - opener).days < EARLY_SEASON_DAYS


def team_current_season_rs_n(
    boxes: pd.DataFrame,
    team: str,
    *,
    as_of: date | datetime | str | None = None,
) -> int:
    """Distinct current-season regular-season games for this team before ``as_of``."""
    if boxes is None or getattr(boxes, "empty", True):
        return 0
    from utils.nba_minutes_role import _norm_team

    tm = _norm_team(team)
    if not tm:
        return 0
    opener = nba_rs_opener(as_of)
    cutoff = _as_date(as_of)
    team_col = "team" if "team" in boxes.columns else None
    date_col = "game_date" if "game_date" in boxes.columns else None
    win_col = "window" if "window" in boxes.columns else None
    if team_col is None or date_col is None:
        return 0
    g = boxes[boxes[team_col].map(_norm_team) == tm]
    if win_col:
        g = g[g[win_col].astype(str).str.lower().eq("regular")]
    if g.empty:
        return 0
    gd = pd.to_datetime(g[date_col], errors="coerce")
    mask = gd.notna()
    if opener is not None:
        mask = mask & (gd.dt.date >= opener)
    if cutoff is not None:
        mask = mask & (gd.dt.date < cutoff)
    return int(gd[mask].dt.date.nunique())


def drtg_display_ok(team_gp: object) -> bool:
    """Hide DRtg until the opponent has about 10 current RS games."""
    try:
        return int(team_gp) >= DRTG_MIN_TEAM_GAMES
    except (TypeError, ValueError):
        return False


def exclude_preseason_from_l5(slate: date | datetime | str | None = None) -> bool:
    """Card window never mixes preseason into L5. Always True (slate unused)."""
    _ = slate
    return True


def current_season_rs_n(
    boxes: pd.DataFrame,
    player: str,
    *,
    as_of: date | datetime | str | None = None,
) -> int:
    """Count this player's current-season regular-season boxes with minutes > 0.

    Opener = first RS date for the campaign containing ``as_of``. Playoffs and
    preseason do not count toward the pre_ bridge.
    """
    d = _as_date(as_of) or date.today()
    opener = nba_rs_opener(d)
    if opener is None or boxes is None or boxes.empty:
        return 0
    if "_pn" not in boxes.columns:
        return 0
    pn = _norm_name(player)
    g = boxes[boxes["_pn"] == pn]
    if g.empty:
        return 0
    n = 0
    for _, row in g.iterrows():
        if str(row.get("window") or "") != "regular":
            continue
        gd = _as_date(row.get("game_date"))
        if gd is None or gd < opener or gd > d:
            continue
        try:
            mins = float(row.get("minutes") or 0)
        except (TypeError, ValueError):
            continue
        if mins > 0 and not math.isnan(mins):
            n += 1
    return n


def preseason_bridge_active(
    boxes: pd.DataFrame,
    player: str,
    *,
    as_of: date | datetime | str | None = None,
) -> bool:
    """True while current-season RS games < PRE_BRIDGE_MIN_RS (show pre_ flags)."""
    return current_season_rs_n(boxes, player, as_of=as_of) < PRE_BRIDGE_MIN_RS


def l5_current_season_n(
    games: list[dict[str, Any]] | None,
    *,
    as_of: date | datetime | str | None = None,
) -> int:
    """How many of the card L5 games are current-season regular season (0–5).

    Last-season RS and playoffs stay in the card window; they do not count.
    """
    opener = nba_rs_opener(as_of)
    if not games or opener is None:
        return 0
    n = 0
    for g in games:
        w = str(g.get("window") or "")
        tag = str(g.get("tag") or "")
        if w != "regular" and tag != "R":
            continue
        gd = _as_date(g.get("date"))
        if gd is not None and gd >= opener:
            n += 1
    return n


def card_window_meta(
    boxes: pd.DataFrame,
    player: str,
    *,
    n: int = 5,
    as_of: date | datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Newest n RS+playoff boxes with minutes > 0 (no prop). May be shorter than n."""
    rows = _game_rows(
        boxes,
        player,
        "points",
        windows=CARD_WINDOWS,
        n=None,
        as_of=as_of,
    )
    if not rows:
        return []
    return rows[:n]


def attach_nba_l5_season_counts(
    df: pd.DataFrame,
    *,
    as_of: date | datetime | str | None = None,
    boxes: pd.DataFrame | None = None,
    require_fresh: bool = True,
) -> pd.DataFrame:
    """Stamp current-season RS n, 2026-in-L5 (0–5), and is_early_season on each row.

    Load failures raise so step8 can log a skip and leave the columns blank;
    ``_META`` freshness then FAILs instead of silently writing an empty n.
    """
    out = df.copy()
    for col, default in (
        ("current_season_rs_n", pd.NA),
        ("l5_current_season_n", pd.NA),
        ("is_early_season", False),
    ):
        if col not in out.columns:
            out[col] = default
    early = bool(is_early_season(as_of))
    out["is_early_season"] = early
    player_col = "player" if "player" in out.columns else (
        "Player" if "Player" in out.columns else None
    )
    if player_col is None:
        return out
    frame = boxes
    if frame is None:
        from utils.nba_minutes_role import load_nba_box_frame

        frame = load_nba_box_frame(require_fresh=require_fresh)
    if frame is None or getattr(frame, "empty", True):
        return out
    cache_rs: dict[str, int] = {}
    cache_l5: dict[str, int] = {}
    rs_vals: list[Any] = []
    l5_vals: list[Any] = []
    for _, row in out.iterrows():
        player = str(row.get(player_col) or "").strip()
        key = _norm_name(player)
        if not key:
            rs_vals.append(pd.NA)
            l5_vals.append(pd.NA)
            continue
        if key not in cache_rs:
            cache_rs[key] = current_season_rs_n(frame, player, as_of=as_of)
            meta = card_window_meta(frame, player, n=5, as_of=as_of)
            cache_l5[key] = l5_current_season_n(meta, as_of=as_of)
        rs_vals.append(int(cache_rs[key]))
        l5_vals.append(int(cache_l5[key]))
    out["current_season_rs_n"] = rs_vals
    out["l5_current_season_n"] = l5_vals
    return out


def _tag(window: str) -> str:
    return _WINDOW_TAG.get(str(window), "?")


def window_mix(games: list[dict[str, Any]] | None) -> str:
    if not games:
        return ""
    return "".join(_tag(g.get("window", "")) for g in games)


def _game_rows(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    *,
    windows: set[str] | frozenset[str],
    n: int | None = 5,
    as_of: date | datetime | str | None = None,
) -> list[dict[str, Any]] | None:
    col = prop_col(prop)
    if col is None or col not in boxes.columns:
        return None
    pn = _norm_name(player)
    g = boxes[boxes["_pn"] == pn].copy()
    if g.empty:
        return None
    # Backtests must pass as_of=tip so post-tip boxes cannot leak into L5.
    # Live boards before tip are naturally as-of (tonight is not in the DB yet).
    cutoff = _as_date(as_of)
    if cutoff is not None:
        gd = pd.to_datetime(g["game_date"], errors="coerce")
        g = g[gd.dt.date < cutoff]
        if g.empty:
            return None
    usable = g[g["window"].isin(windows)].sort_values("game_date", ascending=False)
    out: list[dict[str, Any]] = []
    for _, row in usable.iterrows():
        try:
            mins = float(row.get("minutes") or 0)
            val = float(row.get(col))
        except (TypeError, ValueError):
            continue
        if math.isnan(val) or mins <= 0:
            continue
        w = str(row["window"])
        out.append(
            {
                "date": str(row["game_date"])[:10],
                "window": w,
                "minutes": mins,
                "val": val,
                "tag": _tag(w),
            }
        )
        if n is not None and len(out) >= n:
            break
    if n is not None and len(out) < n:
        return None
    return out if out else None


def l5_card_window(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    *,
    n: int = 5,
    as_of: date | datetime | str | None = None,
) -> list[dict[str, Any]] | None:
    """5 newest regular-season + playoff boxes (no preseason). Matches PP card.

    Pass ``as_of`` (tip / slate date) for graded backtests so L5 only uses
    boxes strictly before tip. Omitting ``as_of`` keeps the live-card behavior
    (newest five in the DB — fine pre-tip when tonight is not boxed yet).
    """
    return _game_rows(
        boxes, player, prop, windows=CARD_WINDOWS, n=n, as_of=as_of
    )


def l5_preseason(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    *,
    n: int | None = None,
    as_of: date | datetime | str | None = None,
    board_team: str | None = None,
) -> list[dict[str, Any]]:
    """Preseason boxes, newest first. Never mixed into card L5.

    Prefer the current campaign's October preseason (year of Oct tip). Only
    fall back to prior-year preseason when the player has no current-year
    preseason games. When ``board_team`` is set, tag ``team_changed`` if
    those preseason boxes were with a different club.
    """
    rows = _game_rows(boxes, player, prop, windows=PRE_WINDOWS, n=None) or []
    if not rows:
        return []
    d = _as_date(as_of) or date.today()
    # Campaign year = year of the October tip for this season.
    campaign = d.year if d.month >= 10 else d.year - 1
    cur = [r for r in rows if str(r["date"])[:4] == str(campaign)]
    chosen = cur if cur else [
        r for r in rows if str(r["date"])[:4] == str(campaign - 1)
    ]
    if not chosen:
        chosen = rows
    if n is not None:
        chosen = chosen[:n]

    if board_team:
        pn = _norm_name(player)
        g = boxes[boxes["_pn"] == pn]
        pre_g = g[g["window"].isin(PRE_WINDOWS)]
        if not pre_g.empty and "team" in pre_g.columns:
            from utils.nba_minutes_role import _norm_team

            bt = _norm_team(board_team)
            teams = {
                _norm_team(t)
                for t in pre_g["team"].dropna().astype(str).tolist()
                if _norm_team(t)
            }
            if bt and teams and bt not in teams:
                for r in chosen:
                    r["team_changed"] = True
    return chosen


def l5_rs_only(
    boxes: pd.DataFrame, player: str, prop: str, *, n: int = 5
) -> list[dict[str, Any]] | None:
    """Legacy: regular-season only. Prefer l5_card_window (includes playoffs)."""
    return _game_rows(boxes, player, prop, windows={"regular"}, n=n)


def l5_blend_pre_rs(
    boxes: pd.DataFrame, player: str, prop: str, *, n: int = 5
) -> list[dict[str, Any]] | None:
    """Legacy chronological blend. Not used for the L5=5 gate."""
    return _game_rows(boxes, player, prop, windows={"preseason", "regular"}, n=n)


def _preseason_clears(
    pre: list[dict[str, Any]], line: float, side: str
) -> bool | None:
    """True if every preseason game clears; False if any miss; None if thin."""
    if len(pre) < PRE_THIN_MIN:
        return None
    vals = [r["val"] for r in pre]
    return bool(l5_clears(vals, line, side) if len(vals) >= 5 else _all_clear(vals, line, side))


def _all_clear(vals: list[float], line: float, side: str) -> bool:
    side_u = str(side or "").strip().upper()
    if side_u == "OVER":
        return all(v > line for v in vals)
    if side_u == "UNDER":
        return all(v < line for v in vals)
    return False


def preseason_flag(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    line: float,
    side: str,
    *,
    as_of: date | datetime | str | None = None,
    board_team: str | None = None,
) -> str:
    """pre_confirms | pre_conflicts | pre_thin | pre_retired — never a list drop.

    ``pre_retired`` when the player already has ``PRE_BRIDGE_MIN_RS`` current-season
    RS games — callers must not show or rank the pre_ bucket.
    """
    if not preseason_bridge_active(boxes, player, as_of=as_of):
        return PRE_RETIRED
    pre = l5_preseason(
        boxes, player, prop, as_of=as_of, board_team=board_team
    )
    cleared = _preseason_clears(pre, line, side)
    if cleared is None:
        return "pre_thin"
    return "pre_confirms" if cleared else "pre_conflicts"


def ticket_l5_window(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    *,
    slate: date | datetime | str | None = None,
    n: int = 5,
) -> tuple[list[dict[str, Any]] | None, str]:
    """Return (card games, mode). Always card window (RS+playoffs)."""
    _ = slate
    return l5_card_window(boxes, player, prop, n=n), "card_rs_playoffs"


def card_l5_ok(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    line: float,
    side: str,
    *,
    slate: date | datetime | str | None = None,
) -> dict[str, Any] | None:
    """Directional L5=5 on the card window. Preseason is early-season flag-only."""
    card = l5_card_window(boxes, player, prop, n=5)
    if card is None:
        return None
    vals = [r["val"] for r in card]
    if not l5_clears(vals, line, side):
        return None
    rs_n = current_season_rs_n(boxes, player, as_of=slate)
    bridge = rs_n < PRE_BRIDGE_MIN_RS
    if bridge:
        pre = l5_preseason(boxes, player, prop, as_of=slate)
        flag = preseason_flag(boxes, player, prop, line, side, as_of=slate)
    else:
        pre = []
        flag = PRE_RETIRED
    return {
        "card": card,
        "preseason": pre,
        "card_vals": vals,
        "pre_vals": [r["val"] for r in pre],
        "pre_dates": [r["date"] for r in pre],
        "card_dates": [r["date"] for r in card],
        "window_mix": window_mix(card),
        "pre_mix": window_mix(pre) if pre else "",
        "pre_flag": flag,
        "n_pre": len(pre),
        "n_card_rs": sum(1 for r in card if r["tag"] == "R"),
        "n_card_po": sum(1 for r in card if r["tag"] == "O"),
        "n_current_season_rs": rs_n,
        "l5_current_season_n": l5_current_season_n(card, as_of=slate),
        "pre_bridge_active": bridge,
        "mode": "card_rs_playoffs",
        "opener": str(nba_rs_opener(slate)) if slate else None,
        "ticket_vals": vals,
        "ticket_avg": sum(vals) / 5.0,
        "pre_team_changed": any(r.get("team_changed") for r in pre),
    }


def early_season_l5_ok(
    boxes: pd.DataFrame,
    player: str,
    prop: str,
    line: float,
    side: str,
    *,
    slate: date | datetime | str | None = None,
) -> dict[str, Any] | None:
    """Alias for card_l5_ok (RS-only / dual-blend gates retired)."""
    return card_l5_ok(boxes, player, prop, line, side, slate=slate)


def format_side_by_side(detail: dict[str, Any]) -> str:
    """One-line card | preseason print for reports. Omits pre when retired."""
    card = detail.get("card") or []
    card_s = " ".join(f"{g['date']}:{g['val']:g}({g['tag']})" for g in card)
    card_part = f"card[{detail.get('window_mix','')}] {card_s}"
    flag = str(detail.get("pre_flag") or "")
    if flag == PRE_RETIRED or detail.get("pre_bridge_active") is False:
        return card_part
    pre = detail.get("preseason") or []
    pre_s = (
        " ".join(f"{g['date']}:{g['val']:g}({g['tag']})" for g in pre)
        if pre
        else "(none)"
    )
    return f"{card_part}  |  pre[{flag}/{detail.get('pre_mix','')}] {pre_s}"
