"""WNBA Goblin-70 line cuts: Rebounds 3.5+ skip D, Assists 2.5 / high-poss fade,
Fantasy off (all sports), Points/Rebounds/Pts+Reb/Pts+Ast season-avg cover,
3PM vol observe."""
from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(_ROOT / "scripts"))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import prop_hit_tiers as T  # noqa: E402
from utils.player_name_utils import normalize_player_name  # noqa: E402

CACHE = _ROOT / "Sports" / "WNBA" / "wnba_espn_cache.csv"
ASSISTS_HIGH_POSS = 18.0
# Season avg >= line+cover_unit. A-only drag is on these four; Assists/3PM/RA
# invert or go flat, so they stay on plain G70 (same scoping as PRA Off).
AVG_COVER_PROPS = frozenset({"points", "rebounds", "pts+reb", "pts+ast"})
# box: (date, fga, fta, tov, pts, reb, ast)
_AVG_COVER_STAT = {
    "points": lambda b: b[4],
    "rebounds": lambda b: b[5],
    "pts+reb": lambda b: b[4] + b[5],
    "pts+ast": lambda b: b[4] + b[6],
}


def _sport(r: dict[str, Any]) -> str:
    return str(r.get("sport") or "").strip().upper()


def _pick(r: dict[str, Any]) -> str:
    raw = str(r.get("pick_type") or r.get("pick") or "").strip().lower()
    if "goblin" in raw:
        return "Goblin"
    if "demon" in raw:
        return "Demon"
    if "standard" in raw or raw:
        return "Standard"
    return ""


def _side(r: dict[str, Any]) -> str:
    s = str(r.get("side") or r.get("direction") or r.get("over_under") or "").strip().upper()
    if s in {"O", "OVER", "HIGHER"}:
        return "OVER"
    if s in {"U", "UNDER", "LOWER"}:
        return "UNDER"
    return s


def _prop(r: dict[str, Any]) -> str:
    return T.canon_prop(_sport(r) or "WNBA", str(r.get("prop") or r.get("prop_type") or ""))


def _line_eq(r: dict[str, Any], target: float) -> bool:
    try:
        return abs(float(r.get("line")) - float(target)) < 1e-9
    except (TypeError, ValueError):
        return False


def _wnba_goblin_over(r: dict[str, Any]) -> bool:
    return _sport(r) == "WNBA" and _pick(r) == "Goblin" and _side(r) == "OVER"


def _line_ge(r: dict[str, Any], target: float) -> bool:
    try:
        return float(r.get("line")) + 1e-9 >= float(target)
    except (TypeError, ValueError):
        return False


def wnba_rebounds_skip_d(r: dict[str, Any]) -> bool:
    """Goblin OVER Rebounds 3.5+ skips D. Keep L5=5+L10>=8+cover (+ avg-cover).

    Market inversion, not a flipped reb-D label. CSV Elite allows 31.7 rpg,
    Weak 40.1. L5=5+L10 D-pass vs fail: 3.5 61.9% n=21 vs 73.6% n=53;
    4.5+ 67.9% n=53 vs 77.3% n=141. Ticket-path skip D (cover+avg) on 4.5+
    is 85.9% n=99 vs D-required 78.8% n=33. <=2.5 does not invert — D stays.
    Do not copy this skip onto Reb+Ast / Pts+Reb.
    """
    if not _wnba_goblin_over(r):
        return False
    if _prop(r) != "rebounds":
        return False
    return _line_ge(r, 3.5)


wnba_rebounds_35_skip_d = wnba_rebounds_skip_d


def wnba_assists_25_ticket_fade(r: dict[str, Any]) -> bool:
    """Goblin OVER Assists 2.5 is off tickets. 1.5 stays.

    L5=5 is already 64.3% (45/70) vs 1.5 at 81.1% (129/159). D and cover
    both invert at 2.5. Not a missing stat gate.
    """
    if not _wnba_goblin_over(r):
        return False
    if _prop(r) != "assists":
        return False
    return _line_eq(r, 2.5)


def wnba_fantasy_ticket_fade(r: dict[str, Any]) -> bool:
    """Fantasy is off tickets for every sport, not just WNBA.

    List SKIP_PROPS was sport-agnostic but label-incomplete (missed
    fantasy_score). The ticket fade was WNBA-only, so WNBA1Q/1H/NBA/NHL
    still cleared the generic L5+L10+D+cover stack. Same leak shape as
    the WNBA Goblin 80% n=25 sleeve. Off until a keep-list pass.
    """
    if _prop(r) == "fantasy":
        return True
    from utils.fantasy_prop_filter import is_fantasy_prop_label

    return is_fantasy_prop_label(r.get("prop") or r.get("prop_type") or "")


def _norm_name(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


def _row_date(r: dict[str, Any]) -> str:
    for key in ("game_date", "date", "slate_date", "start_time", "commence_time"):
        raw = str(r.get(key) or "")[:10]
        if len(raw) == 10 and raw[4:5] == "-":
            return raw
    return ""


def _row_poss_g(r: dict[str, Any]) -> float | None:
    for key in ("poss_g", "possessions_per_game"):
        raw = r.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


@lru_cache(maxsize=1)
def _wnba_boxes() -> dict[str, list[tuple[str, float, float, float, float, float, float]]]:
    """player_norm -> [(date, FGA, FTA, TO, PTS, REB, AST), ...] same-game excluded by caller."""
    import pandas as pd

    if not CACHE.is_file():
        return {}
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    pn = df.get("PLAYER_NORM")
    if pn is None:
        df["PLAYER_NORM"] = df["PLAYER_NAME"].map(_norm_name)
    else:
        df["PLAYER_NORM"] = pn.fillna("").astype(str)
        empty = df["PLAYER_NORM"] == ""
        if empty.any():
            df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm_name)
    for col in ("MIN", "FGA", "FTA", "TO", "PTS", "REB", "AST"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df = df.sort_values(["PLAYER_NORM", "game_date"])
    out: dict[str, list[tuple[str, float, float, float, float, float, float]]] = {}
    for rec in df.itertuples(index=False):
        if float(rec.MIN) < 1:
            continue
        out.setdefault(rec.PLAYER_NORM, []).append(
            (
                rec.game_date,
                float(rec.FGA),
                float(rec.FTA),
                float(rec.TO),
                float(rec.PTS),
                float(rec.REB),
                float(rec.AST),
            )
        )
    return out


def _prior_boxes(player: str, slate_date: str):
    d = str(slate_date or "")[:10]
    if len(d) != 10:
        return []
    return [b for b in _wnba_boxes().get(_norm_name(player), []) if b[0] < d]


def poss_g_entering(player: str, slate_date: str) -> float | None:
    """FGA+0.44*FTA+TO per game from boxes strictly before slate_date."""
    prior = _prior_boxes(player, slate_date)
    if not prior:
        return None
    n = len(prior)
    fga = sum(b[1] for b in prior)
    fta = sum(b[2] for b in prior)
    tov = sum(b[3] for b in prior)
    return (fga + 0.44 * fta + tov) / n


def season_avg_entering(player: str, slate_date: str, prop: str) -> float | None:
    """Entering-game season mean for Points / Rebounds / Pts+Reb / Pts+Ast."""
    getter = _AVG_COVER_STAT.get(prop)
    if getter is None:
        return None
    prior = _prior_boxes(player, slate_date)
    if not prior:
        return None
    return sum(getter(b) for b in prior) / len(prior)


def attach_wnba_poss(r: dict[str, Any], date: str = "") -> dict[str, Any]:
    """Fill poss_g on WNBA Goblin Assists from ESPN cache. Other rows unchanged."""
    if not _wnba_goblin_over(r) or _prop(r) != "assists":
        return r
    if date and not r.get("game_date"):
        r["game_date"] = str(date)[:10]
    if _row_poss_g(r) is not None:
        return r
    d = _row_date(r) or str(date or "")[:10]
    poss = poss_g_entering(str(r.get("player") or ""), d)
    if poss is not None:
        r["poss_g"] = poss
    return r


def _row_season_avg(r: dict[str, Any]) -> float | None:
    """Prefer stamped season mean. Do not use season_avg — recs may fall back to L5."""
    for key in ("avg_season", "stat_season_avg"):
        raw = r.get(key)
        if raw in (None, ""):
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


def attach_wnba_season_avg(r: dict[str, Any], date: str = "") -> dict[str, Any]:
    """Fill avg_season on Points/Rebounds/Pts+Reb/Pts+Ast from ESPN cache."""
    if not _wnba_goblin_over(r) or _prop(r) not in AVG_COVER_PROPS:
        return r
    if date and not r.get("game_date"):
        r["game_date"] = str(date)[:10]
    if _row_season_avg(r) is not None:
        return r
    d = _row_date(r) or str(date or "")[:10]
    avg = season_avg_entering(str(r.get("player") or ""), d, _prop(r))
    if avg is not None:
        r["avg_season"] = avg
    return r


def attach_wnba_ticket_priors(r: dict[str, Any], date: str = "") -> dict[str, Any]:
    attach_wnba_poss(r, date)
    attach_wnba_season_avg(r, date)
    return r


def wnba_avg_cover_ok(r: dict[str, Any]) -> bool:
    """Season avg >= line+cover_unit on Points/Rebounds/Pts+Reb/Pts+Ast only.

    A-only on those four is 56–66%. Assists A-only 82.4% beats A∩B 76.6%;
    3PM and Reb+Ast are flat. Missing avg fails open. Do not copy onto
    Assists / 3PM / Reb+Ast / PRA / FGA.
    """
    if not _wnba_goblin_over(r):
        return True
    prop = _prop(r)
    if prop not in AVG_COVER_PROPS:
        return True
    avg = _row_season_avg(r)
    if avg is None:
        d = _row_date(r)
        if d:
            avg = season_avg_entering(str(r.get("player") or ""), d, prop)
    if avg is None:
        return True
    try:
        line = float(r.get("line"))
    except (TypeError, ValueError):
        return True
    return avg >= line + float(T.cover_need("WNBA", prop))


def wnba_assists_high_poss_fade(r: dict[str, Any]) -> bool:
    """Goblin OVER Assists with entering poss/g >= 18 is off tickets.

    L5=5 is 64.2% (34/53) vs poss<18 at 75.6% (226/299). High-usage
    ball-handlers vs a tiny Goblin assist line. Missing poss does not fade.
    Do not copy onto Pts+Ast / PRA.
    """
    if not _wnba_goblin_over(r):
        return False
    if _prop(r) != "assists":
        return False
    poss = _row_poss_g(r)
    if poss is None:
        d = _row_date(r)
        if d:
            poss = poss_g_entering(str(r.get("player") or ""), d)
    if poss is None:
        return False
    return poss >= ASSISTS_HIGH_POSS


def threes_vol_ok(r: dict[str, Any]) -> bool:
    """5+ L10 3PA/g + season 3P%>=33 (min 40 3PA). Independent of L10/D."""
    try:
        l10_3pa_g = float(r.get("l10_3pa_g"))
        season_3pp = float(r.get("season_3pp"))
        season_3pa = float(r.get("season_3pa") or 0)
    except (TypeError, ValueError):
        return False
    if l10_3pa_g < 5:
        return False
    if season_3pa < 40:
        return False
    return season_3pp >= 0.33
