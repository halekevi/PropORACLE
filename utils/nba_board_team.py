"""Player → team from the PrizePicks / step8 board, not last-season boxes.

Trades and FA signings leave box-score history on the old club. Opponent D,
usage, and ESPN game matching must use tonight's board team (e.g. Garland LAC).

Also resolves the regular-season opener from ESPN (season_type=2), cached on
disk, so early-L5 and classify_game do not hardcode ~Oct 22.
"""
from __future__ import annotations

import json
import urllib.request
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from utils.nba_minutes_role import _norm_name, load_nba_box_frame
from utils.roster_change import norm_nba_team

_REPO = Path(__file__).resolve().parents[1]
_OPENER_CACHE_PATH = _REPO / "data" / "cache" / "nba_rs_opener.json"


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


def season_start_year(as_of: date) -> int:
    """Autumn year of the NBA season containing ``as_of`` (2025-26 → 2025)."""
    return as_of.year if as_of.month >= 8 else as_of.year - 1


def step8_path(slate: str, *, root: Path | None = None) -> Path:
    base = root or _REPO
    return base / "outputs" / slate / "nba" / "step8_all_direction_clean.xlsx"


def load_board_player_teams(
    slate: str,
    *,
    root: Path | None = None,
) -> dict[str, dict[str, str]]:
    """Map normalized player → {team, opp, source} from the slate step8 board."""
    path = step8_path(slate, root=root)
    if not path.exists():
        return {}
    df = pd.read_excel(path, sheet_name="ALL", header=1)
    if "Player" not in df.columns or "Team" not in df.columns:
        return {}
    out: dict[str, dict[str, str]] = {}
    for _, r in df.iterrows():
        player = str(r.get("Player") or "").strip()
        team = norm_nba_team(r.get("Team"))
        if not player or not team:
            continue
        pn = _norm_name(player)
        opp = norm_nba_team(r.get("Opp") or r.get("Opp Team") or "")
        if pn not in out:
            out[pn] = {
                "player": player,
                "team": team,
                "opp": opp,
                "source": "step8_board",
            }
    return out


def last_box_team(
    player: str,
    *,
    as_of: date | str | None = None,
    windows: set[str] | None = None,
) -> str:
    """Most recent box-score team on/before as_of, PP-normalized."""
    boxes = load_nba_box_frame()
    if boxes.empty:
        return ""
    pn = _norm_name(player)
    g = boxes[boxes["_pn"] == pn].copy()
    if g.empty:
        return ""
    cutoff = _as_date(as_of)
    if cutoff is not None:
        g = g[g["game_date"].dt.date <= cutoff]
    if windows:
        g = g[g["window"].isin(windows)]
    if g.empty:
        return ""
    g = g.sort_values("game_date", ascending=False)
    return norm_nba_team(g.iloc[0].get("team"))


def board_vs_last_box_mismatches(
    slate: str,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Board team vs latest box, and vs last prior-season (non-preseason) box.

    Latest-box often already shows the new club after preseason. The prior-RS
    comparison surfaces offseason trades/signings that still poison L5 context
    if opponent D is built from old-team boxes without a roster flag.
    """
    board = load_board_player_teams(slate, root=root)
    d = _as_date(slate) or date.today()
    opener = first_regular_season_date_public(as_of=d)
    prior_cutoff = (opener - timedelta(days=1)) if opener else d

    def _collect(windows: set[str] | None, *, as_of_cut: date) -> dict[str, Any]:
        mismatches: list[dict[str, str]] = []
        matched = 0
        no_box = 0
        for _pn, info in board.items():
            box_team = last_box_team(
                info["player"], as_of=as_of_cut, windows=windows
            )
            if not box_team:
                no_box += 1
                continue
            if box_team == info["team"]:
                matched += 1
                continue
            mismatches.append(
                {
                    "player": info["player"],
                    "board_team": info["team"],
                    "board_opp": info.get("opp") or "",
                    "last_box_team": box_team,
                }
            )
        mismatches.sort(key=lambda r: (r["board_team"], r["player"]))
        return {
            "matched": matched,
            "no_box_history": no_box,
            "mismatch_n": len(mismatches),
            "mismatch_rate": round(len(mismatches) / max(len(board), 1), 4),
            "mismatches": mismatches,
        }

    latest = _collect(None, as_of_cut=d)
    prior = _collect({"regular", "playoffs"}, as_of_cut=prior_cutoff)
    return {
        "slate": slate,
        "board_players": len(board),
        "opener": str(opener) if opener else None,
        "prior_cutoff": str(prior_cutoff),
        "vs_latest_box": latest,
        "vs_prior_season_box": prior,
        # Back-compat top-level fields = prior-season trade view (the useful one).
        "matched": prior["matched"],
        "no_box_history": prior["no_box_history"],
        "mismatch_n": prior["mismatch_n"],
        "mismatch_rate": prior["mismatch_rate"],
        "mismatches": prior["mismatches"],
    }


def _read_opener_cache() -> dict[str, str]:
    if not _OPENER_CACHE_PATH.exists():
        return {}
    try:
        raw = json.loads(_OPENER_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _write_opener_cache(season_year: int, opener: date) -> None:
    data = _read_opener_cache()
    data[str(season_year)] = opener.isoformat()
    _OPENER_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _OPENER_CACHE_PATH.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _espn_first_regular_date(season_year: int) -> date | None:
    """Scan ESPN scoreboards Oct 1–Nov 15 for the first season_type=2 tip."""
    # ESPN season.year for 2026-27 boards is 2027; accept season_year or +1.
    start = date(season_year, 10, 1)
    for i in range(0, 46):  # through mid-November
        d = start + timedelta(days=i)
        ymd = d.strftime("%Y%m%d")
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
            f"scoreboard?dates={ymd}"
        )
        try:
            with urllib.request.urlopen(url, timeout=20) as resp:
                board = json.loads(resp.read())
        except Exception:
            continue
        for ev in board.get("events") or []:
            season = ev.get("season") or {}
            try:
                stype = int(season.get("type") or 0)
            except (TypeError, ValueError):
                stype = 0
            slug = str(season.get("slug") or "").lower()
            if stype == 2 or slug == "regular-season":
                return d
    return None


@lru_cache(maxsize=8)
def first_regular_season_date(
    *,
    season_start_year_arg: int | None = None,
    as_of: str | None = None,
) -> date | None:
    """Earliest regular-season tip for the season containing ``as_of``.

    Order: disk cache → ESPN season_type=2 probe → box dates after Oct 1 that
    classify as regular once opener is known (bootstrap via ESPN only).
    """
    d = _as_date(as_of) or date.today()
    y = season_start_year_arg if season_start_year_arg is not None else season_start_year(d)

    cached = _read_opener_cache().get(str(y))
    if cached:
        try:
            return date.fromisoformat(cached)
        except ValueError:
            pass

    opener = _espn_first_regular_date(y)
    if opener is not None:
        _write_opener_cache(y, opener)
        return opener
    return None


def first_regular_season_date_public(
    *,
    as_of: date | datetime | str | None = None,
    season_start_year_arg: int | None = None,
) -> date | None:
    """Non-cached-arg wrapper for callers that pass date objects."""
    d = _as_date(as_of)
    return first_regular_season_date(
        season_start_year_arg=season_start_year_arg,
        as_of=d.isoformat() if d else None,
    )


def games_with_team(
    player: str,
    team: str,
    *,
    as_of: date | str | None = None,
    windows: set[str] | None = None,
) -> int:
    """Count box rows for player on ``team`` on/before as_of."""
    boxes = load_nba_box_frame()
    if boxes.empty:
        return 0
    pn = _norm_name(player)
    tm = norm_nba_team(team)
    g = boxes[boxes["_pn"] == pn].copy()
    if g.empty or not tm:
        return 0
    cutoff = _as_date(as_of)
    if cutoff is not None:
        g = g[g["game_date"].dt.date <= cutoff]
    if windows:
        g = g[g["window"].isin(windows)]
    g = g[g["team"].map(norm_nba_team) == tm]
    # Positive minutes only
    try:
        g = g[g["minutes"].fillna(0) > 0]
    except Exception:
        pass
    return int(len(g))


def team_changed_status(
    player: str,
    board_team: str,
    *,
    slate: date | str | None = None,
    min_new_team_games: int = 5,
) -> dict[str, Any]:
    """Tag offseason moves; block L5=5 power plays until 5 games on the new club.

    Early-window RS-only L5 still pulls prior-season boxes — those are the old
    team/role. ``team_changed=1`` until ``games_with_new_team >= min_new_team_games``
    (regular + preseason count toward the new-team sample).
    """
    d = _as_date(slate) or date.today()
    opener = first_regular_season_date_public(as_of=d)
    prior_cutoff = (opener - timedelta(days=1)) if opener else d
    board_tm = norm_nba_team(board_team)
    prior_tm = last_box_team(
        player, as_of=prior_cutoff, windows={"regular", "playoffs"}
    )
    changed = bool(board_tm and prior_tm and board_tm != prior_tm)
    n_new = games_with_team(player, board_tm, as_of=d) if board_tm else 0
    blocked = changed and n_new < int(min_new_team_games)
    return {
        "player": player,
        "board_team": board_tm,
        "prior_team": prior_tm,
        "team_changed": int(changed),
        "games_with_new_team": n_new,
        "min_new_team_games": int(min_new_team_games),
        "power_l5_blocked": blocked,
        "block_reason": (
            f"team_changed:{prior_tm}->{board_tm}; new_team_games={n_new}<{min_new_team_games}"
            if blocked
            else ""
        ),
    }


def annotate_board_team_changed(
    slate: str,
    *,
    root: Path | None = None,
    min_new_team_games: int = 5,
) -> dict[str, Any]:
    """Board-wide team_changed tags for the slate."""
    board = load_board_player_teams(slate, root=root)
    rows = []
    blocked = []
    for _pn, info in board.items():
        st = team_changed_status(
            info["player"],
            info["team"],
            slate=slate,
            min_new_team_games=min_new_team_games,
        )
        rows.append(st)
        if st["power_l5_blocked"]:
            blocked.append(st)
    return {
        "slate": slate,
        "board_players": len(board),
        "team_changed_n": sum(1 for r in rows if r["team_changed"]),
        "power_l5_blocked_n": len(blocked),
        "min_new_team_games": min_new_team_games,
        "rows": rows,
        "power_l5_blocked": blocked,
    }
