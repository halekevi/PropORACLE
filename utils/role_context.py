"""Minutes + team-stat role columns for list exports (flags only, no gating).

Pulls team top/bottom ranks from Sports/*/data/*_top3_vs_defense.csv when
present. Derives starter_flag / minutes_flag / l5_minutes_gap from step8
minute columns that already exist on the row.
"""
from __future__ import annotations

import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]

# step8 prop_norm / board labels → leader-file category
_PROP_TO_CAT: dict[str, str] = {
    "pts": "pts",
    "points": "pts",
    "ast": "ast",
    "assists": "ast",
    "reb": "reb",
    "rebounds": "reb",
    "pra": "pra",
    "pts+rebs+asts": "pra",
    "pr": "pts",  # no dedicated PR board in top3 — fall back empty
    "pa": "ast",
    "ra": "reb",
    "stl": "stl",
    "steals": "stl",
    "blk": "blk",
    "blocks": "blk",
    "stocks": "stocks",
    "blks+stls": "stocks",
    "fg3m": "fg3m",
    "3ptmade": "fg3m",
    "3pm": "fg3m",
    "threes": "fg3m",
}

_ASSIST_PROPS = {"ast", "assists", "pa", "pts+asts", "pts+ast"}
_BIG_POS = {"c", "f-c", "c-f", "center"}


def _norm_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or "").strip().lower())
    return "".join(c for c in t if not unicodedata.combining(c))


def _num(v: object) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:  # NaN
        return None
    return x


def prop_to_leader_cat(prop: object) -> str:
    p = str(prop or "").strip().lower().replace(" ", "")
    if p in _PROP_TO_CAT:
        return _PROP_TO_CAT[p]
    p2 = p.replace("+", "").replace("_", "")
    return _PROP_TO_CAT.get(p2, "")


def _top3_path(sport: str) -> Path | None:
    sp = str(sport or "").upper()
    if sp in {"NBA", "NBA1H", "NBA1Q"}:
        p = _REPO / "Sports/NBA/data/nba_top3_vs_defense.csv"
        return p if p.exists() else None
    if sp in {"WNBA", "WNBA1H", "WNBA1Q"}:
        p = _REPO / "Sports/WNBA/data/wnba_top3_vs_defense.csv"
        return p if p.exists() else None
    return None


@lru_cache(maxsize=8)
def _load_leader_table(path_str: str) -> pd.DataFrame:
    df = pd.read_csv(path_str, encoding="utf-8-sig")
    if "leader_side" not in df.columns:
        df = df.copy()
        df["leader_side"] = "top"
    df["_pn"] = df.get("PLAYER_NORM", pd.Series([""] * len(df))).map(_norm_name)
    if "PLAYER_NORM" not in df.columns and "PLAYER_NAME" in df.columns:
        df["_pn"] = df["PLAYER_NAME"].map(_norm_name)
    df["_cat"] = df["category"].astype(str).str.lower().str.strip()
    df["_side"] = df["leader_side"].astype(str).str.lower().str.strip()
    df["_team"] = df.get("TEAM", pd.Series([""] * len(df))).astype(str).str.upper().str.strip()
    return df


_TEAM_ALIASES = {
    "SAS": "SA",
    "SA": "SA",
    "NYK": "NY",
    "NY": "NY",
    "GSW": "GS",
    "GS": "GS",
    "NOP": "NO",
    "NO": "NO",
    "UTA": "UTAH",
    "UTAH": "UTAH",
    "WAS": "WSH",
    "WSH": "WSH",
    "PHX": "PHO",
    "PHO": "PHO",
    "BKN": "BK",
    "BK": "BK",
}


def _norm_team(v: object) -> str:
    t = str(v or "").strip().upper()
    if not t or t in {"NAN", "NONE"}:
        return ""
    return _TEAM_ALIASES.get(t, t)


def _team_from_row(row: dict[str, Any]) -> str:
    for k in ("team", "Team", "TEAM", "player_team", "team_abbr"):
        v = _norm_team(row.get(k))
        if v:
            return v
    return ""


def lookup_team_rank(
    *,
    sport: str,
    player: str,
    team: str,
    prop: str,
) -> dict[str, Any]:
    """Return team_rank / team_n / top3 / bottom3 for this player's prop family."""
    out = {
        "team_rank": "",
        "team_n": "",
        "top3": False,
        "bottom3": False,
        "leader_cat": "",
        "ast_team_rank": "",
    }
    cat = prop_to_leader_cat(prop)
    out["leader_cat"] = cat
    path = _top3_path(sport)
    if path is None:
        return out
    df = _load_leader_table(str(path))
    pn = _norm_name(player)
    tm = _norm_team(team)

    def _rank_for(category: str) -> tuple[float | None, float | None, bool, bool]:
        if not category:
            return None, None, False, False
        sub = df[df["_cat"] == category]
        if tm:
            sub_tm = sub[sub["_team"].map(_norm_team) == tm]
        else:
            sub_tm = sub
        # team_n ≈ players with a rank row for this team+cat (top+bottom boards)
        team_n = int(sub_tm["_pn"].nunique()) if len(sub_tm) else None
        hit = sub_tm[sub_tm["_pn"] == pn]
        if hit.empty:
            # player may have moved teams — try name-only
            hit = sub[sub["_pn"] == pn]
        if hit.empty:
            return None, team_n, False, False
        # Prefer current-team row
        if tm and (hit["_team"].map(_norm_team) == tm).any():
            hit = hit[hit["_team"].map(_norm_team) == tm]
        # Prefer top-board row when both exist
        top_hit = hit[hit["_side"] == "top"]
        row = (top_hit if len(top_hit) else hit).iloc[0]
        rank = _num(row.get("rank_on_team"))
        side = str(row.get("_side") or "").lower()
        is_top = side == "top" and rank is not None and rank <= 3
        is_bot = side == "bottom" and rank is not None and rank <= 3
        return rank, team_n, bool(is_top), bool(is_bot)

    rank, team_n, is_top, is_bot = _rank_for(cat)
    if rank is not None:
        out["team_rank"] = int(rank)
    if team_n is not None:
        out["team_n"] = int(team_n)
    out["top3"] = is_top
    out["bottom3"] = is_bot

    ast_rank, _, _, _ = _rank_for("ast")
    if ast_rank is not None:
        out["ast_team_rank"] = int(ast_rank)
    return out


def minutes_fields(row: dict[str, Any]) -> dict[str, Any]:
    """starter_flag / l5_minutes_gap / minutes_flag from existing step8 cols.

    Prefer role_mpg / Min Tier (preseason+season) over L5-alone for starter_flag.
    Keep L5 minutes as min_last5_avg; gap = L5 − season RS mpg.
    """
    min_l5 = _num(row.get("min_last5_avg") or row.get("min_player_avg"))
    min_season = _num(
        row.get("min_season_avg") or row.get("min_per_game") or row.get("mpg")
    )
    role_mpg = _num(row.get("role_mpg"))
    starter = str(row.get("starter_flag") or "").strip().lower()
    tier = str(
        row.get("minutes_tier_label")
        or row.get("Min Tier")
        or row.get("minutes_tier")
        or ""
    ).strip().upper()
    # numeric minutes_tier 0–3 still shows up
    if not starter:
        if tier in {"3", "3.0", "HIGH"}:
            starter = "starter"
        elif tier in {"0", "0.0", "1", "1.0", "LOW"}:
            starter = "bench"
        elif tier in {"2", "2.0", "MEDIUM", "MED"}:
            starter = "rotation"
        elif role_mpg is not None:
            if role_mpg >= 28:
                starter = "starter"
            elif role_mpg >= 18:
                starter = "rotation"
            else:
                starter = "bench"
        elif min_l5 is not None:
            if min_l5 >= 28:
                starter = "starter"
            elif min_l5 >= 18:
                starter = "rotation"
            else:
                starter = "bench"

    gap = ""
    if min_l5 is not None and min_season is not None:
        gap = round(min_l5 - min_season, 2)

    flags: list[str] = []
    if starter == "bench" or (min_l5 is not None and min_l5 < 20):
        flags.append("bench_minutes_L5")
    if isinstance(gap, (int, float)):
        if gap >= 5:
            flags.append("minutes_up")
        elif gap <= -5:
            flags.append("minutes_down")
    if str(row.get("roster_changed") or "") in {"1", "True", "true"} or str(
        row.get("roster_change_reason") or ""
    ).strip():
        flags.append("role_changed")

    return {
        "min_last5_avg": min_l5 if min_l5 is not None else "",
        "min_season_avg": min_season if min_season is not None else "",
        "starter_flag": starter,
        "l5_minutes_gap": gap,
        "minutes_flag": "|".join(flags) if flags else "",
    }


def is_big_man(row: dict[str, Any]) -> bool:
    pos = str(row.get("pos") or row.get("position") or row.get("Pos") or "").strip().lower()
    if pos in _BIG_POS or pos.startswith("c"):
        return True
    return "center" in pos


def is_listed_point_guard(row: dict[str, Any]) -> bool:
    """Board rarely stores PG vs SG; treat explicit PG tags only."""
    for k in ("pos", "position", "Pos", "primary_pos", "listed_pos"):
        v = str(row.get(k) or "").strip().lower()
        if v in {"pg", "point guard", "g-pg"} or v.startswith("pg"):
            return True
    return False


def not_primary_playmaker(row: dict[str, Any], *, ast_team_rank: object) -> bool:
    """Assist-Under role flag from current roster + preseason APG.

    Prefer preseason team assist rank (rotation, 10+ mpg). Fall back to the
    passed ast_team_rank only when preseason has no row. Last-season APG alone
    does not clear a player (Castle led SA last year, 1 assist opener night).
    Does not gate tickets.
    """
    if is_listed_point_guard(row):
        return False
    try:
        from utils.nba_minutes_role import preseason_assist_ranks_by_team

        ranks = preseason_assist_ranks_by_team()
        team = _team_from_row(row)
        pn = _norm_name(row.get("player") or row.get("player_name"))
        pre_rank = ranks.get((team, pn))
        if pre_rank is not None:
            # Only the clear #1 preseason playmaker avoids the flag.
            # Top-2 still flags (Castle #2 behind Fox, 1 AST on opener).
            return int(pre_rank) > 1
        # On roster but below rotation mpg / no preseason — treat as non-primary
        if team:
            team_has = any(t == team for (t, _) in ranks.keys())
            if team_has:
                return True
    except Exception:
        pass
    rank = _num(ast_team_rank)
    if rank is not None:
        return int(rank) > 1
    return True


def enrich_leg_row(row: dict[str, Any], *, sport: str = "", prop: str = "") -> dict[str, Any]:
    """Attach export columns onto a leg dict (non-destructive copy fields)."""
    sp = sport or str(row.get("sport") or "")
    pr = prop or str(row.get("prop") or row.get("prop_norm") or row.get("prop_type") or "")
    player = str(row.get("player") or row.get("player_name") or "").strip()
    team = _team_from_row(row)
    ranks = lookup_team_rank(sport=sp, player=player, team=team, prop=pr)
    mins = minutes_fields(row)
    return {
        **mins,
        "team_rank": ranks["team_rank"],
        "team_n": ranks["team_n"],
        "top3": ranks["top3"],
        "bottom3": ranks["bottom3"],
        "leader_cat": ranks["leader_cat"],
        "ast_team_rank": ranks["ast_team_rank"],
        "big_man": is_big_man(row),
    }
