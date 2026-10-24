"""NFL team offensive / defensive ranks (league + division) and game tightness.

Scoring O/D come from ESPN completed regular-season scores.
Pass/rush offense comes from our boxscore cache (team sum per game).
Pass/rush defense comes from data/reference/nfl_team_defense.csv.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd

from utils.nfl_divisions import (
    DIVISION_ORDER,
    nfl_conference,
    nfl_division,
    same_conference,
    same_division,
)
from utils.nfl_espn_context import NFL_SCOREBOARD, _get_json, canon_nfl_abbr
from utils.nfl_prop_defense import last_completed_nfl_season
from utils.nfl_stat_leaders import load_nfl_boxscores

_REPO = Path(__file__).resolve().parents[1]
GAMES_PATH = _REPO / "Sports/NFL/data/nfl_game_results.csv"
RANKS_PATH = _REPO / "Sports/NFL/data/nfl_team_unit_ranks.csv"
TIGHT_PATH = _REPO / "Sports/NFL/data/nfl_divisional_tightness.json"
DEFENSE_PATH = _REPO / "data/reference/nfl_team_defense.csv"

ONE_SCORE_MAX = 8  # FG + TD, or a missed 2-pt after a TD
FIELD_GOAL_MAX = 3


def fetch_regular_season_games(
    season: int,
    *,
    max_week: int = 18,
    sleep_s: float = 0.08,
) -> pd.DataFrame:
    """Pull completed regular-season games week-by-week.

    ESPN scoreboard date ranges (YYYYMMDD-YYYYMMDD) 400; week + seasontype +
    dates=<year> is the same call the O/D markdown pairings already use.
    """
    from utils.nfl_stat_leaders import season_from_game_date

    seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    empty_streak = 0
    for week in range(1, int(max_week) + 1):
        try:
            payload = _get_json(
                NFL_SCOREBOARD,
                params={
                    "seasontype": 2,
                    "week": int(week),
                    "dates": int(season),
                    "limit": "100",
                },
                timeout=45.0,
            )
        except Exception as exc:
            print(f"[NFL games] week {week} {season} failed ({type(exc).__name__}: {exc})")
            time.sleep(max(0.0, sleep_s))
            continue
        n_before = len(rows)
        for ev in payload.get("events") or []:
            rec = _game_row(ev, season=int(season), week=week)
            if not rec:
                continue
            if rec["event_id"] in seen:
                continue
            if season_from_game_date(rec["date"]) != int(season):
                continue
            seen.add(rec["event_id"])
            rows.append(rec)
        if len(rows) == n_before:
            empty_streak += 1
            if empty_streak >= 2 and week >= 3:
                break
        else:
            empty_streak = 0
        time.sleep(max(0.0, sleep_s))
    return pd.DataFrame(rows)


def _game_row(ev: dict[str, Any], *, season: int, week: int) -> dict[str, Any] | None:
    sea = ev.get("season") or {}
    try:
        sea_type = int(sea.get("type") or 0)
    except (TypeError, ValueError):
        sea_type = 0
    slug = str(sea.get("slug") or "").lower()
    if sea_type and sea_type != 2:
        return None
    if slug and "regular" not in slug:
        return None
    comps = ev.get("competitions") or []
    if not comps or not isinstance(comps[0], dict):
        return None
    comp = comps[0]
    st = (comp.get("status") or {}).get("type") or {}
    if not st.get("completed"):
        return None
    home = away = ""
    hs = as_ = None
    for c in comp.get("competitors") or []:
        if not isinstance(c, dict):
            continue
        ab = canon_nfl_abbr((c.get("team") or {}).get("abbreviation"))
        if not ab:
            continue
        try:
            sc = int(float(c.get("score")))
        except (TypeError, ValueError):
            return None
        if str(c.get("homeAway") or "").lower() == "home":
            home, hs = ab, sc
        else:
            away, as_ = ab, sc
    if not home or not away or hs is None or as_ is None:
        return None
    wk = week
    try:
        wk = int((ev.get("week") or {}).get("number") or week or 0)
    except (TypeError, ValueError):
        wk = week
    margin = abs(int(hs) - int(as_))
    return {
        "season": int(season),
        "week": int(wk),
        "event_id": str(ev.get("id") or ""),
        "date": str(comp.get("date") or ev.get("date") or "")[:10],
        "home": home,
        "away": away,
        "home_score": int(hs),
        "away_score": int(as_),
        "margin": int(margin),
        "total": int(hs) + int(as_),
        "divisional": bool(same_division(home, away)),
        "conference": bool(same_conference(home, away)),
        "home_division": nfl_division(home),
        "away_division": nfl_division(away),
    }


def load_or_fetch_games(
    repo: Path | None = None,
    *,
    seasons: list[int] | None = None,
    refresh: bool = False,
) -> pd.DataFrame:
    root = repo or _REPO
    path = root / "Sports/NFL/data/nfl_game_results.csv"
    want = seasons or [last_completed_nfl_season()]
    cached = pd.DataFrame()
    if path.is_file() and not refresh:
        cached = pd.read_csv(path, encoding="utf-8-sig")
    frames: list[pd.DataFrame] = []
    have_seasons = set()
    if not cached.empty and "season" in cached.columns:
        have_seasons = {int(x) for x in pd.to_numeric(cached["season"], errors="coerce").dropna()}
        frames.append(cached)
    missing = [s for s in want if s not in have_seasons]
    for season in missing:
        got = fetch_regular_season_games(int(season))
        if got.empty:
            continue
        frames.append(got)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "event_id" in out.columns:
        out = out.drop_duplicates(subset=["season", "event_id"], keep="last")
    else:
        out = out.drop_duplicates(subset=["season", "date", "home", "away"], keep="last")
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8-sig")
    return out


def _offense_from_boxscores(repo: Path, season: int) -> pd.DataFrame:
    df = load_nfl_boxscores(repo, season=season)
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["PASS_YDS"] = pd.to_numeric(work.get("PASS_YDS"), errors="coerce").fillna(0.0)
    work["RUSH_YDS"] = pd.to_numeric(work.get("RUSH_YDS"), errors="coerce").fillna(0.0)
    gid = "event_id" if "event_id" in work.columns else "game_date"
    per = work.groupby(["TEAM", gid], as_index=False).agg(
        pass_yds=("PASS_YDS", "sum"),
        rush_yds=("RUSH_YDS", "sum"),
    )
    agg = per.groupby("TEAM", as_index=False).agg(
        pass_off_ypg=("pass_yds", "mean"),
        rush_off_ypg=("rush_yds", "mean"),
        off_games=("pass_yds", "count"),
    )
    agg = agg.rename(columns={"TEAM": "team"})
    return agg


def _scoring_from_games(games: pd.DataFrame, season: int) -> pd.DataFrame:
    g = games[games["season"] == season].copy()
    if g.empty:
        return pd.DataFrame()
    sides: list[dict[str, Any]] = []
    for r in g.itertuples(index=False):
        sides.append({"team": r.home, "pf": int(r.home_score), "pa": int(r.away_score)})
        sides.append({"team": r.away, "pf": int(r.away_score), "pa": int(r.home_score)})
    sdf = pd.DataFrame(sides)
    out = sdf.groupby("team", as_index=False).agg(
        pf_pg=("pf", "mean"),
        pa_pg=("pa", "mean"),
        games=("pf", "count"),
    )
    return out


def _rank_high(s: pd.Series) -> pd.Series:
    return s.rank(ascending=False, method="min").astype("Int64")


def _rank_low(s: pd.Series) -> pd.Series:
    return s.rank(ascending=True, method="min").astype("Int64")


def build_nfl_team_unit_ranks(
    repo: Path | None = None,
    *,
    season: int | None = None,
    games: pd.DataFrame | None = None,
) -> pd.DataFrame:
    root = repo or _REPO
    yr = int(season) if season else last_completed_nfl_season()
    if games is None:
        games = load_or_fetch_games(root, seasons=[yr], refresh=False)
    scoring = _scoring_from_games(games, yr)
    off_box = _offense_from_boxscores(root, yr)
    defense = pd.read_csv(root / "data/reference/nfl_team_defense.csv", encoding="utf-8-sig")
    if "team_abbr" in defense.columns:
        defense["team"] = defense["team_abbr"].map(canon_nfl_abbr)
    else:
        defense["team"] = defense["team"].map(canon_nfl_abbr)

    teams = sorted({t for t in defense["team"].tolist() if t})
    out = pd.DataFrame({"team": teams})
    out["conference"] = out["team"].map(nfl_conference)
    out["division"] = out["team"].map(nfl_division)
    out["season"] = yr
    if not scoring.empty:
        out = out.merge(scoring, on="team", how="left")
    else:
        out["pf_pg"] = pd.NA
        out["pa_pg"] = pd.NA
        out["games"] = pd.NA
    if not off_box.empty:
        out = out.merge(off_box, on="team", how="left")
    else:
        out["pass_off_ypg"] = pd.NA
        out["rush_off_ypg"] = pd.NA
        out["off_games"] = pd.NA
    dkeep = defense[
        [c for c in ("team", "opp_pass_ypg", "opp_rush_ypg", "pass_def_rank", "rush_def_rank", "pa_rank") if c in defense.columns]
    ].drop_duplicates("team")
    out = out.merge(dkeep, on="team", how="left")

    out["score_off_rank"] = _rank_high(pd.to_numeric(out["pf_pg"], errors="coerce"))
    out["score_def_rank"] = _rank_low(pd.to_numeric(out["pa_pg"], errors="coerce"))
    if out["score_def_rank"].isna().all() and "pa_rank" in out.columns:
        out["score_def_rank"] = pd.to_numeric(out["pa_rank"], errors="coerce").astype("Int64")
    out["pass_off_rank"] = _rank_high(pd.to_numeric(out["pass_off_ypg"], errors="coerce"))
    out["rush_off_rank"] = _rank_high(pd.to_numeric(out["rush_off_ypg"], errors="coerce"))
    if "pass_def_rank" not in out.columns:
        out["pass_def_rank"] = pd.NA
    if "rush_def_rank" not in out.columns:
        out["rush_def_rank"] = pd.NA

    def _div_rank(col: str, *, high: bool) -> pd.Series:
        ser = pd.to_numeric(out[col], errors="coerce")
        fn = _rank_high if high else _rank_low
        return out.groupby("division", group_keys=False)[col].transform(
            lambda s: fn(pd.to_numeric(s, errors="coerce"))
        )

    out["div_score_off_rank"] = _div_rank("pf_pg", high=True)
    out["div_score_def_rank"] = _div_rank("pa_pg", high=False)
    if out["div_score_def_rank"].isna().all() and "pa_rank" in out.columns:
        # pa_rank is already 1=stingiest league-wide; re-rank within division on pa_pg or pa_rank.
        out["div_score_def_rank"] = out.groupby("division", group_keys=False)["score_def_rank"].rank(
            method="min", ascending=True
        ).astype("Int64")
    out["div_pass_off_rank"] = _div_rank("pass_off_ypg", high=True)
    out["div_rush_off_rank"] = _div_rank("rush_off_ypg", high=True)
    out["div_pass_def_rank"] = out.groupby("division", group_keys=False)["pass_def_rank"].rank(
        method="min", ascending=True
    ).astype("Int64")
    out["div_rush_def_rank"] = out.groupby("division", group_keys=False)["rush_def_rank"].rank(
        method="min", ascending=True
    ).astype("Int64")

    out["division"] = pd.Categorical(out["division"], categories=list(DIVISION_ORDER), ordered=True)
    out = out.sort_values(["division", "div_score_off_rank", "team"]).reset_index(drop=True)
    out["division"] = out["division"].astype(str)
    return out


def _bucket_stats(g: pd.DataFrame) -> dict[str, Any]:
    if g.empty:
        return {"n": 0}
    m = pd.to_numeric(g["margin"], errors="coerce")
    tot = pd.to_numeric(g["total"], errors="coerce")
    n = int(m.notna().sum())
    return {
        "n": n,
        "mean_margin": round(float(m.mean()), 2),
        "median_margin": round(float(m.median()), 2),
        "pct_one_score": round(100.0 * float((m <= ONE_SCORE_MAX).mean()), 1),
        "pct_fg_or_less": round(100.0 * float((m <= FIELD_GOAL_MAX).mean()), 1),
        "pct_blowout_14": round(100.0 * float((m >= 14).mean()), 1),
        "mean_total": round(float(tot.mean()), 1),
    }


def analyze_divisional_tightness(games: pd.DataFrame) -> dict[str, Any]:
    g = games.copy()
    if g.empty:
        return {"n": 0}

    def _flag(s: pd.Series) -> pd.Series:
        if pd.api.types.is_bool_dtype(s):
            return s.fillna(False)
        return s.astype(str).str.lower().isin(["true", "1", "yes"])

    g["divisional"] = _flag(g["divisional"])
    g["conference"] = _flag(g["conference"])
    overall = _bucket_stats(g)
    div = _bucket_stats(g[g["divisional"]])
    nondiv = _bucket_stats(g[~g["divisional"]])
    conf_only = _bucket_stats(g[(~g["divisional"]) & g["conference"]])
    other = _bucket_stats(g[(~g["divisional"]) & ~g["conference"]])
    by_div: dict[str, Any] = {}
    for name in DIVISION_ORDER:
        sub = g[g["divisional"] & (g["home_division"] == name)]
        by_div[name] = _bucket_stats(sub)
    seasons = sorted({int(x) for x in pd.to_numeric(g["season"], errors="coerce").dropna()})
    by_season: dict[str, Any] = {}
    full_mask = pd.Series(False, index=g.index)
    for s in seasons:
        sub = g[g["season"] == s]
        by_season[str(s)] = {
            "all": _bucket_stats(sub),
            "divisional": _bucket_stats(sub[sub["divisional"]]),
            "non_divisional": _bucket_stats(sub[~sub["divisional"]]),
        }
        if len(sub) >= 200:
            full_mask = full_mask | (g["season"] == s)
    full_g = g[full_mask] if full_mask.any() else g
    div_full = _bucket_stats(full_g[full_g["divisional"]])
    nondiv_full = _bucket_stats(full_g[~full_g["divisional"]])
    delta = None
    if div_full.get("n") and nondiv_full.get("n"):
        delta = {
            "mean_margin_div_minus_nondiv": round(div_full["mean_margin"] - nondiv_full["mean_margin"], 2),
            "one_score_pp_div_minus_nondiv": round(div_full["pct_one_score"] - nondiv_full["pct_one_score"], 1),
            "tighter_if_negative_margin": div_full["mean_margin"] < nondiv_full["mean_margin"],
            "sample": "full regular seasons (>=200 games)",
        }
    return {
        "seasons": seasons,
        "one_score_max": ONE_SCORE_MAX,
        "overall": overall,
        "divisional": div,
        "non_divisional": nondiv,
        "conference_non_div": conf_only,
        "interconference": other,
        "full_seasons": {
            "divisional": div_full,
            "non_divisional": nondiv_full,
        },
        "by_season": by_season,
        "by_division": by_div,
        "delta": delta,
        "note": (
            "Regular season only. One-score = margin <= 8. "
            "Headline delta uses completed 17-game seasons. "
            "Negative mean-margin delta means division games finish closer."
        ),
    }


def attach_nfl_unit_ranks(df: pd.DataFrame, repo: Path | None = None) -> pd.DataFrame:
    root = repo or _REPO
    path = root / "Sports/NFL/data/nfl_team_unit_ranks.csv"
    out = df.copy()
    extra = [
        "team_division",
        "opp_division",
        "divisional_game",
        "team_div_score_off_rank",
        "team_div_score_def_rank",
        "opp_div_score_off_rank",
        "opp_div_score_def_rank",
        "team_score_off_rank",
        "team_score_def_rank",
        "team_pass_off_rank",
        "team_rush_off_rank",
        "opp_score_off_rank",
        "opp_score_def_rank",
    ]
    for c in extra:
        if c not in out.columns:
            out[c] = pd.NA
    team_col = next((c for c in ("team", "team_abbr") if c in out.columns), None)
    opp_col = next((c for c in ("opp_team", "opponent", "opp") if c in out.columns), None)
    if not team_col:
        return out
    t = out[team_col].map(canon_nfl_abbr)
    out["team_division"] = t.map(nfl_division)
    if opp_col:
        o = out[opp_col].map(canon_nfl_abbr)
        out["opp_division"] = o.map(nfl_division)
        out["divisional_game"] = [same_division(a, b) for a, b in zip(t, o)]
    if not path.is_file():
        return out
    ranks = pd.read_csv(path, encoding="utf-8-sig")
    ranks["team"] = ranks["team"].map(canon_nfl_abbr)
    keep = [
        "team",
        "div_score_off_rank",
        "div_score_def_rank",
        "score_off_rank",
        "score_def_rank",
        "pass_off_rank",
        "rush_off_rank",
        "div_pass_off_rank",
        "div_rush_off_rank",
        "div_pass_def_rank",
        "div_rush_def_rank",
    ]
    keep = [c for c in keep if c in ranks.columns]
    rmap = ranks[keep].drop_duplicates("team")

    def _join(series: pd.Series, col: str) -> pd.Series:
        if col not in rmap.columns:
            return pd.Series(pd.NA, index=out.index)
        m = rmap.set_index("team")[col].to_dict()
        return series.map(lambda x: m.get(x, pd.NA))

    out["team_div_score_off_rank"] = _join(t, "div_score_off_rank")
    out["team_div_score_def_rank"] = _join(t, "div_score_def_rank")
    out["team_score_off_rank"] = _join(t, "score_off_rank")
    out["team_score_def_rank"] = _join(t, "score_def_rank")
    out["team_pass_off_rank"] = _join(t, "pass_off_rank")
    out["team_rush_off_rank"] = _join(t, "rush_off_rank")
    if opp_col:
        out["opp_div_score_off_rank"] = _join(o, "div_score_off_rank")
        out["opp_div_score_def_rank"] = _join(o, "div_score_def_rank")
        out["opp_score_off_rank"] = _join(o, "score_off_rank")
        out["opp_score_def_rank"] = _join(o, "score_def_rank")
        # Opponent offense ranks (tackle props use opp rush OFF, not rush D)
        out["opp_pass_off_rank"] = _join(o, "pass_off_rank")
        out["opp_rush_off_rank"] = _join(o, "rush_off_rank")
    return out


def write_nfl_unit_rank_artifacts(
    repo: Path | None = None,
    *,
    season: int | None = None,
    extra_seasons: list[int] | None = None,
    refresh_games: bool = False,
) -> dict[str, Any]:
    root = repo or _REPO
    seasons = list(extra_seasons or [])
    hint = last_completed_nfl_season()
    for s in (hint, hint - 1, hint - 2):
        if s not in seasons:
            seasons.append(int(s))
    games = load_or_fetch_games(root, seasons=seasons, refresh=refresh_games)
    if season is not None:
        yr = int(season)
    else:
        counts = (
            games.groupby("season").size()
            if not games.empty and "season" in games.columns
            else pd.Series(dtype=int)
        )
        full = [int(s) for s, n in counts.items() if int(n) >= 200]
        yr = max(full) if full else int(hint) - 1
    ranks = build_nfl_team_unit_ranks(root, season=yr, games=games)
    ranks_path = root / "Sports/NFL/data/nfl_team_unit_ranks.csv"
    ranks_path.parent.mkdir(parents=True, exist_ok=True)
    ranks.to_csv(ranks_path, index=False, encoding="utf-8-sig")
    tightness = analyze_divisional_tightness(games)
    tightness["rank_season"] = yr
    tight_path = root / "Sports/NFL/data/nfl_divisional_tightness.json"
    tight_path.write_text(json.dumps(tightness, indent=2), encoding="utf-8")
    return {
        "season": yr,
        "ranks_path": ranks_path,
        "tight_path": tight_path,
        "n_teams": int(len(ranks)),
        "n_games": int(len(games)),
        "tightness": tightness,
        "ranks": ranks,
    }
