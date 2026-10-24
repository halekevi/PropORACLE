"""Map display prop labels to top-3 / strat category ids (pts, ast, sog, …)."""

from __future__ import annotations

import re
import unicodedata

from utils.prop_norm import canon_prop as _canon_prop

NBA_PROP_CAT: dict[str, str] = {
    "points": "pts",
    "pts": "pts",
    "rebounds": "reb",
    "reb": "reb",
    "offensiverebounds": "reb",
    "defensiverebounds": "reb",
    "assists": "ast",
    "ast": "ast",
    "steals": "stl",
    "stl": "stl",
    "blocks": "blk",
    "blk": "blk",
    "3ptmade": "fg3m",
    "3ptmmade": "fg3m",
    "3pt": "fg3m",
    "fg3m": "fg3m",
    "threes": "fg3m",
    "pts+rebs+asts": "pra",
    "pts+rebs": "pra",
    "pts+asts": "pra",
    "rebs+asts": "pra",
    "fantasyscore": "pra",
}

WNBA_PROP_CAT = dict(NBA_PROP_CAT)

NHL_PROP_CAT: dict[str, str] = {
    "goals": "goals",
    "goal": "goals",
    "assists": "assists",
    "assist": "assists",
    "points": "points",
    "pts": "points",
    "powerplaypoints": "points",
    "shots": "shots",
    "shotsongoal": "shots",
    "sog": "shots",
    "shots_on_goal": "shots",
}

MLB_PROP_CAT: dict[str, str] = {
    "hits": "hits",
    "hit": "hits",
    "runs": "hits",
    "run": "hits",
    "totalbases": "total_bases",
    "total_bases": "total_bases",
    "homeruns": "home_runs",
    "home_runs": "home_runs",
    "hr": "home_runs",
    "rbi": "rbi",
    "rbis": "rbi",
}

_SPORT_MAPS: dict[str, dict[str, str]] = {
    "NBA": NBA_PROP_CAT,
    "NBA1H": NBA_PROP_CAT,
    "NBA1Q": NBA_PROP_CAT,
    "WNBA": WNBA_PROP_CAT,
    "WNBA1H": WNBA_PROP_CAT,
    "WNBA1Q": WNBA_PROP_CAT,
    "NHL": NHL_PROP_CAT,
    "MLB": MLB_PROP_CAT,
}


def _norm_sport(sport: object) -> str:
    return str(sport or "").strip().upper()


def _norm_prop_key(prop: object) -> str:
    s = str(prop or "").strip().lower()
    s = re.sub(r"[^a-z0-9+]+", " ", s).strip()
    s = s.replace(" ", "")
    s = s.replace("threepoint", "3pt").replace("3ptmade", "3pt")
    return s


def prop_to_category(sport: object, prop: object, *, prop_raw: object = "") -> str:
    sp = _norm_sport(sport)
    canon = _canon_prop(sp, prop_raw or prop)
    hoop = {
        "points": "pts",
        "rebounds": "reb",
        "assists": "ast",
        "steals": "stl",
        "blocks": "blk",
        "threes": "fg3m",
        "threes_att": "fg3a",
        "fga": "fga",
        "fgm": "fgm",
        "pra": "pra",
        "pts+reb": "pra",
        "pts+ast": "pra",
        "reb+ast": "pra",
        "stocks": "pra",
    }
    if sp in {"NBA", "NBA1H", "NBA1Q", "WNBA", "WNBA1H", "WNBA1Q", "CBB", "WCBB"}:
        if canon in hoop:
            return hoop[canon]
    nhl = {
        "goals": "goals",
        "assists": "assists",
        "points": "points",
        "sog": "shots",
        "blocked_shots": "shots",
    }
    if sp == "NHL":
        return nhl.get(canon, "")
    mlb = {
        "hits": "hits",
        "runs": "hits",
        "total_bases": "total_bases",
        "home_runs": "home_runs",
        "rbis": "rbi",
    }
    if sp == "MLB":
        return mlb.get(canon, "")
    soccer = {
        "goals": "goals",
        "assists": "assists",
        "shots": "shots",
        "sog": "shots_on_target",
        "shots_on_target": "shots_on_target",
        "saves": "saves",
        "goalie_saves": "saves",
        "passes": "passes",
        "tackles": "tackles",
    }
    if sp == "SOCCER":
        return soccer.get(canon, "")
    key = _norm_prop_key(prop_raw or prop)
    if "+" in key:
        for part in key.split("+"):
            if part in ("pts", "rebs", "asts", "reb", "ast"):
                return "pra"
    m = _SPORT_MAPS.get(sp, {})
    if key in m:
        return m[key]
    for pat, cat in m.items():
        if pat in key or key in pat:
            return cat
    return m.get(key, "")
