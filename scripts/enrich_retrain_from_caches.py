#!/usr/bin/env python3
"""Attach cache-based enrichment onto data/retrain_dataset.csv.

Historical step8 joins drop usage / per-stat D / tennis surface because dated
slates never kept those columns. Season caches still match by player / opp /
calendar surface, which is what live scoring uses.

  py -3.14 scripts/enrich_retrain_from_caches.py
  py -3.14 scripts/enrich_retrain_from_caches.py --input data/retrain_dataset.csv
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils import basketball_prop_defense as bpd  # noqa: E402
from utils.sport_season_windows import nba_stats_season, prior_nba_stats_season  # noqa: E402
from utils.basketball_minutes_tier import assign_minutes_tier  # noqa: E402
from utils.nfl_prop_defense import snap_pct_to_minutes_tier  # noqa: E402
from utils.prop_category import prop_to_category  # noqa: E402

# WNBA usage-cache team keys (LVA / NYL / PHO / GSV / WAS).
_WNBA_USAGE_TEAM = {
    "CONN": "CON",
    "CONNECTICUT": "CON",
    "SUN": "CON",
    "NY": "NYL",
    "NYL": "NYL",
    "LIBERTY": "NYL",
    "LV": "LVA",
    "LVA": "LVA",
    "ACES": "LVA",
    "LA": "LAS",
    "LAS": "LAS",
    "SPARKS": "LAS",
    "PHX": "PHO",
    "PHO": "PHO",
    "MERCURY": "PHO",
    "WASH": "WAS",
    "WSH": "WAS",
    "WAS": "WAS",
    "MYS": "WAS",
    "GS": "GSV",
    "GSV": "GSV",
    "GSW": "GSV",
    "VALKYRIES": "GSV",
}

_SURFACE_ENC = {"hard": 0.0, "clay": 1.0, "grass": 2.0}


def _norm_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or "").strip().lower())
    return "".join(c for c in t if not unicodedata.combining(c))


def _wnba_team(v: object) -> str:
    s = str(v or "").strip().upper()
    if not s or s in {"NAN", "NONE", "NULL", ""}:
        return ""
    return _WNBA_USAGE_TEAM.get(s, s)


def _scale_usage(usg: object) -> float | None:
    try:
        u = float(usg)
    except (TypeError, ValueError):
        return None
    if np.isnan(u):
        return None
    if u > 1.0:
        u /= 100.0
    return round(u, 4)


def _wnba_usage_tier(u: float | None) -> str:
    if u is None or (isinstance(u, float) and np.isnan(u)):
        return ""
    if u >= 0.25:
        return "high"
    if u >= 0.18:
        return "medium"
    return "low"


def _nba_usage_tier(u: float | None) -> str:
    if u is None or (isinstance(u, float) and np.isnan(u)):
        return ""
    if u >= 0.28:
        return "star"
    if u >= 0.22:
        return "high"
    if u >= 0.16:
        return "medium"
    return "role"


def _load_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _season_block(cache: dict, *keys: str) -> dict:
    for k in keys:
        if k in cache and isinstance(cache[k], dict):
            return cache[k]
    for v in cache.values():
        if isinstance(v, dict) and ("players" in v or "teams" in v):
            return v
    return {}


def _calendar_surface(file_date: object) -> str:
    s = str(file_date or "")[:10]
    if len(s) < 10:
        return "Hard"
    if s < "2026-06-09":
        return "Clay"
    if s < "2026-07-14":
        return "Grass"
    return "Hard"


def _ensure_cols(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan


def _as_object(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        df[col] = pd.Series(pd.NA, index=df.index, dtype=object)
    elif str(df[col].dtype) != "object":
        df[col] = df[col].astype(object)


def _stat_def_lookup_table(csv_path: Path, canon_fn) -> dict[str, dict[str, tuple[float, str]]]:
    if not csv_path.is_file():
        return {}
    raw = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "team" not in raw.columns:
        return {}
    cats = sorted(set(bpd.PROP_TO_CAT.values()))
    table: dict[str, dict[str, tuple[float, str]]] = {}
    for _, row in raw.iterrows():
        team = canon_fn(row.get("team"))
        if not team:
            continue
        inner: dict[str, tuple[float, str]] = {}
        for cat in cats:
            rk = row.get(f"{cat}_rank")
            tr = row.get(f"{cat}_tier")
            try:
                rank = float(rk) if pd.notna(rk) else np.nan
            except (TypeError, ValueError):
                rank = np.nan
            inner[cat] = (rank, "" if pd.isna(tr) else str(tr))
        table[team] = inner
    return table


def _apply_stat_def(
    df: pd.DataFrame,
    idx,
    *,
    csv_path: Path,
    canon_fn,
) -> int:
    table = _stat_def_lookup_table(csv_path, canon_fn)
    if not table or "prop" not in df.columns:
        return 0
    opp_col = "opp_team" if "opp_team" in df.columns else ("opp" if "opp" in df.columns else None)
    if not opp_col:
        return 0
    props = df.loc[idx, "prop"].map(bpd.prop_category)
    opps = df.loc[idx, opp_col].map(canon_fn)
    ranks, tiers, cats = [], [], []
    for team, cat in zip(opps, props, strict=False):
        rec = table.get(str(team) if team else "", {}).get(str(cat) if cat else "")
        if rec is None:
            ranks.append(np.nan)
            tiers.append("")
            cats.append(cat or "")
            continue
        ranks.append(rec[0])
        tiers.append(rec[1])
        cats.append(cat or "")
    df.loc[idx, "stat_def_rank"] = pd.Series(ranks, index=idx, dtype=float)
    _as_object(df, "stat_def_tier")
    _as_object(df, "stat_def_category")
    df.loc[idx, "stat_def_tier"] = pd.Series(tiers, index=idx, dtype=object)
    df.loc[idx, "stat_def_category"] = pd.Series(cats, index=idx, dtype=object)
    return int(pd.Series(ranks, dtype=float).notna().sum())


def _enrich_wnba(df: pd.DataFrame, root: Path) -> dict[str, int]:
    sport = df.get("sport", pd.Series("", index=df.index)).astype(str).str.strip().str.upper()
    mask = sport.eq("WNBA")
    n = int(mask.sum())
    stats = {"wnba_rows": n, "usage": 0, "pace": 0, "stat_def": 0, "star": 0}
    if n == 0:
        return stats

    usage_cache = _load_json(root / "Sports" / "WNBA" / "data" / "wnba_usage_cache.json")
    pace_cache = _load_json(root / "Sports" / "WNBA" / "data" / "wnba_team_pace_cache.json")
    players = (_season_block(usage_cache, "season_2025", "season_2026").get("players") or {})
    teams = (_season_block(pace_cache, "season_2025", "season_2026").get("teams") or {})

    by_nt: dict[tuple[str, str], dict] = {}
    by_n: dict[str, dict] = {}
    for rec in players.values():
        if not isinstance(rec, dict):
            continue
        nn = _norm_name(rec.get("player_name"))
        tt = _wnba_team(rec.get("team"))
        if nn:
            by_n[nn] = rec
            if tt:
                by_nt[(nn, tt)] = rec

    star_path = root / "Sports" / "WNBA" / "data" / "wnba_star_tiers.csv"
    star_map: dict[str, int] = {}
    if star_path.is_file():
        star_df = pd.read_csv(star_path, encoding="utf-8-sig")
        for _, r in star_df.iterrows():
            star_map[_norm_name(r.get("player_name"))] = int(r.get("star_tier") or 2)

    _ensure_cols(
        df,
        [
            "usage_pct",
            "usage_tier",
            "min_per_game",
            "min_last5_avg",
            "min_season_avg",
            "team_pace",
            "opp_pace",
            "pace_delta",
            "pace_context",
            "star_tier",
            "is_franchise_star",
            "stat_def_rank",
            "stat_def_tier",
            "stat_def_category",
        ],
    )
    _as_object(df, "usage_tier")
    _as_object(df, "pace_context")
    _as_object(df, "stat_def_tier")
    _as_object(df, "stat_def_category")

    idx = df.index[mask]
    names = df.loc[idx, "player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=idx)
    team = df.loc[idx, "team"].map(_wnba_team) if "team" in df.columns else pd.Series("", index=idx)
    opp = df.loc[idx, "opp_team"].map(_wnba_team) if "opp_team" in df.columns else pd.Series("", index=idx)

    recs = []
    for nn, tt in zip(names, team, strict=False):
        recs.append(by_nt.get((nn, tt)) or by_n.get(nn))

    usg, mpg, utier = [], [], []
    for rec in recs:
        if not rec:
            usg.append(np.nan)
            mpg.append(np.nan)
            utier.append("")
            continue
        u = _scale_usage(rec.get("usage_pct"))
        usg.append(u if u is not None else np.nan)
        try:
            mpg.append(float(rec.get("min_per_game")))
        except (TypeError, ValueError):
            mpg.append(np.nan)
        utier.append(_wnba_usage_tier(u))
    usg_s = pd.Series(usg, index=idx, dtype=float)
    mpg_s = pd.Series(mpg, index=idx, dtype=float)
    fill_u = df.loc[idx, "usage_pct"].isna()
    df.loc[idx, "usage_pct"] = df.loc[idx, "usage_pct"].where(~fill_u, usg_s)
    fill_m = df.loc[idx, "min_per_game"].isna()
    df.loc[idx, "min_per_game"] = df.loc[idx, "min_per_game"].where(~fill_m, mpg_s)
    df.loc[idx, "min_last5_avg"] = df.loc[idx, "min_last5_avg"].where(
        df.loc[idx, "min_last5_avg"].notna(), df.loc[idx, "min_per_game"]
    )
    df.loc[idx, "min_season_avg"] = df.loc[idx, "min_season_avg"].where(
        df.loc[idx, "min_season_avg"].notna(), df.loc[idx, "min_per_game"]
    )
    blank_tier = df.loc[idx, "usage_tier"].isna() | (df.loc[idx, "usage_tier"].astype(str).str.strip() == "")
    df.loc[idx, "usage_tier"] = df.loc[idx, "usage_tier"].where(~blank_tier, utier)
    stats["usage"] = int(df.loc[idx, "usage_pct"].notna().sum())

    pace_map = {str(k).upper(): (v or {}).get("pace") for k, v in teams.items()}
    tp = team.map(lambda t: pace_map.get(t))
    op = opp.map(lambda t: pace_map.get(t))
    tp = pd.to_numeric(tp, errors="coerce")
    op = pd.to_numeric(op, errors="coerce")
    df.loc[idx, "team_pace"] = df.loc[idx, "team_pace"].where(df.loc[idx, "team_pace"].notna(), tp)
    df.loc[idx, "opp_pace"] = df.loc[idx, "opp_pace"].where(df.loc[idx, "opp_pace"].notna(), op)
    both = df.loc[idx, "team_pace"].notna() & df.loc[idx, "opp_pace"].notna()
    delta = df.loc[idx, "team_pace"] - df.loc[idx, "opp_pace"]
    df.loc[idx, "pace_delta"] = df.loc[idx, "pace_delta"].where(df.loc[idx, "pace_delta"].notna(), delta.where(both))
    stats["pace"] = int(df.loc[idx, "team_pace"].notna().sum())

    stars = names.map(lambda n: star_map.get(n, 2))
    df.loc[idx, "star_tier"] = df.loc[idx, "star_tier"].where(df.loc[idx, "star_tier"].notna(), stars)
    df.loc[idx, "is_franchise_star"] = (pd.to_numeric(df.loc[idx, "star_tier"], errors="coerce") == 1).astype(float)
    stats["star"] = int((pd.to_numeric(df.loc[idx, "star_tier"], errors="coerce") == 1).sum())

    try:
        from utils.wnba_prop_defense import canon_team, default_csv_path

        stats["stat_def"] = _apply_stat_def(
            df, idx, csv_path=Path(default_csv_path()), canon_fn=canon_team
        )
    except Exception as exc:
        print(f"[enrich] WNBA stat_def attach failed: {exc}")
    return stats


def _enrich_nba(df: pd.DataFrame, root: Path) -> dict[str, int]:
    sport = df.get("sport", pd.Series("", index=df.index)).astype(str).str.strip().str.upper()
    mask = sport.isin({"NBA", "NBA1H", "NBA1Q"})
    n = int(mask.sum())
    stats = {"nba_rows": n, "usage": 0, "pace": 0, "stat_def": 0}
    if n == 0:
        return stats

    usage_cache = _load_json(root / "Sports" / "NBA" / "data" / "nba_usage_cache.json")
    pace_cache = _load_json(root / "Sports" / "NBA" / "data" / "nba_team_pace_cache.json")
    cur = f"season_{nba_stats_season()}"
    prior = f"season_{prior_nba_stats_season()}"
    players = (_season_block(usage_cache, cur, prior, "season_2025-26", "season_2024-25").get("players") or {})
    teams = (_season_block(pace_cache, cur, prior, "season_2025-26", "season_2024-25").get("teams") or {})
    by_n: dict[str, dict] = {}
    for rec in players.values():
        if isinstance(rec, dict):
            nn = _norm_name(rec.get("player_name"))
            if nn:
                by_n[nn] = rec
    pace_map = {str(k).upper(): (v or {}).get("pace") for k, v in teams.items()}
    def_map = {str(k).upper(): (v or {}).get("def_rating") for k, v in teams.items()}

    _ensure_cols(
        df,
        [
            "usage_pct",
            "usage_tier",
            "min_per_game",
            "min_last5_avg",
            "reb_pct",
            "ast_pct",
            "team_pace",
            "opp_pace",
            "pace_delta",
            "opp_def_rating",
            "stat_def_rank",
            "stat_def_tier",
            "stat_def_category",
        ],
    )
    _as_object(df, "usage_tier")
    _as_object(df, "stat_def_tier")
    _as_object(df, "stat_def_category")
    idx = df.index[mask]
    names = df.loc[idx, "player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=idx)
    team = (
        df.loc[idx, "team"].astype(str).str.strip().str.upper()
        if "team" in df.columns
        else pd.Series("", index=idx)
    )
    opp = (
        df.loc[idx, "opp_team"].astype(str).str.strip().str.upper()
        if "opp_team" in df.columns
        else pd.Series("", index=idx)
    )

    usg, mpg, utier, reb, ast = [], [], [], [], []
    for nn in names:
        rec = by_n.get(nn)
        if not rec:
            usg.append(np.nan)
            mpg.append(np.nan)
            utier.append("")
            reb.append(np.nan)
            ast.append(np.nan)
            continue
        u = _scale_usage(rec.get("usage_pct"))
        usg.append(u if u is not None else np.nan)
        try:
            mpg.append(float(rec.get("min_per_game")))
        except (TypeError, ValueError):
            mpg.append(np.nan)
        utier.append(_nba_usage_tier(u))
        try:
            reb.append(float(rec.get("reb_pct")))
        except (TypeError, ValueError):
            reb.append(np.nan)
        try:
            ast.append(float(rec.get("ast_pct")))
        except (TypeError, ValueError):
            ast.append(np.nan)

    usg_s = pd.Series(usg, index=idx, dtype=float)
    df.loc[idx, "usage_pct"] = df.loc[idx, "usage_pct"].where(df.loc[idx, "usage_pct"].notna(), usg_s)
    df.loc[idx, "min_per_game"] = df.loc[idx, "min_per_game"].where(
        df.loc[idx, "min_per_game"].notna(), pd.Series(mpg, index=idx, dtype=float)
    )
    df.loc[idx, "min_last5_avg"] = df.loc[idx, "min_last5_avg"].where(
        df.loc[idx, "min_last5_avg"].notna(), df.loc[idx, "min_per_game"]
    )
    blank_tier = df.loc[idx, "usage_tier"].isna() | (df.loc[idx, "usage_tier"].astype(str).str.strip() == "")
    df.loc[idx, "usage_tier"] = df.loc[idx, "usage_tier"].where(~blank_tier, utier)
    df.loc[idx, "reb_pct"] = df.loc[idx, "reb_pct"].where(
        df.loc[idx, "reb_pct"].notna(), pd.Series(reb, index=idx, dtype=float)
    )
    df.loc[idx, "ast_pct"] = df.loc[idx, "ast_pct"].where(
        df.loc[idx, "ast_pct"].notna(), pd.Series(ast, index=idx, dtype=float)
    )
    stats["usage"] = int(df.loc[idx, "usage_pct"].notna().sum())

    tp = pd.to_numeric(team.map(pace_map), errors="coerce")
    op = pd.to_numeric(opp.map(pace_map), errors="coerce")
    df.loc[idx, "team_pace"] = df.loc[idx, "team_pace"].where(df.loc[idx, "team_pace"].notna(), tp)
    df.loc[idx, "opp_pace"] = df.loc[idx, "opp_pace"].where(df.loc[idx, "opp_pace"].notna(), op)
    df.loc[idx, "opp_def_rating"] = df.loc[idx, "opp_def_rating"].where(
        df.loc[idx, "opp_def_rating"].notna(), pd.to_numeric(opp.map(def_map), errors="coerce")
    )
    both = df.loc[idx, "team_pace"].notna() & df.loc[idx, "opp_pace"].notna()
    delta = df.loc[idx, "team_pace"] - df.loc[idx, "opp_pace"]
    df.loc[idx, "pace_delta"] = df.loc[idx, "pace_delta"].where(df.loc[idx, "pace_delta"].notna(), delta.where(both))
    stats["pace"] = int(df.loc[idx, "team_pace"].notna().sum())

    try:
        from utils.nba_prop_defense import canon_team, default_csv_path

        stats["stat_def"] = _apply_stat_def(
            df, idx, csv_path=Path(default_csv_path()), canon_fn=canon_team
        )
    except Exception as exc:
        print(f"[enrich] NBA stat_def attach failed: {exc}")
    return stats


def _enrich_tennis(df: pd.DataFrame, root: Path) -> dict[str, int]:
    sport = df.get("sport", pd.Series("", index=df.index)).astype(str).str.strip().str.upper()
    mask = sport.eq("TENNIS")
    n = int(mask.sum())
    stats = {"tennis_rows": n, "surface": 0, "profile": 0}
    if n == 0:
        return stats

    profiles = _load_json(root / "Sports" / "Tennis" / "data" / "tennis_surface_profiles.json")
    tour_lookup = _load_json(root / "Sports" / "Tennis" / "data" / "tournament_surfaces.json")
    cols = [
        "surface",
        "surface_encoded",
        "aces_per_match_mean",
        "first_serve_pct",
        "win_rate_on_surface",
        "games_won_per_match",
        "n_matches_on_surface",
        "surface_specialist",
        "surface_struggle",
        "h2h_win_rate_on_surface",
    ]
    _ensure_cols(df, cols)
    _as_object(df, "surface")
    idx = df.index[mask]
    fd = df.loc[idx, "file_date"] if "file_date" in df.columns else pd.Series("", index=idx)
    tour_col = next((c for c in ("tournament", "event", "match") if c in df.columns), None)

    surf = []
    for i in idx:
        tournament = str(df.at[i, tour_col] or "") if tour_col else ""
        hit = ""
        if tournament:
            tl = tournament.lower()
            for key, s in tour_lookup.items():
                if key != "default" and str(key).lower() in tl:
                    hit = str(s)
                    break
        surf.append(hit or _calendar_surface(fd.at[i] if hasattr(fd, "at") else fd.loc[i]))

    surf_s = pd.Series(surf, index=idx)
    blank = df.loc[idx, "surface"].isna() | (df.loc[idx, "surface"].astype(str).str.strip().isin({"", "nan", "None"}))
    df.loc[idx, "surface"] = df.loc[idx, "surface"].where(~blank, surf_s)
    enc = df.loc[idx, "surface"].astype(str).str.strip().str.lower().map(_SURFACE_ENC)
    df.loc[idx, "surface_encoded"] = df.loc[idx, "surface_encoded"].where(
        df.loc[idx, "surface_encoded"].notna(), enc
    )
    stats["surface"] = int(df.loc[idx, "surface_encoded"].notna().sum())

    names = (
        df.loc[idx, "player"].map(lambda s: re.sub(r"\s+", " ", str(s or "").strip().lower()))
        if "player" in df.columns
        else pd.Series("", index=idx)
    )
    prof_n = 0
    for i, nn, s in zip(idx, names, df.loc[idx, "surface"].astype(str).str.strip().str.title(), strict=False):
        rec = profiles.get(f"{nn}_{s}") if nn else None
        if not rec or int(rec.get("n_matches_on_surface") or 0) < 5:
            continue
        prof_n += 1
        for k in (
            "aces_per_match_mean",
            "first_serve_pct",
            "win_rate_on_surface",
            "games_won_per_match",
            "n_matches_on_surface",
        ):
            if pd.isna(df.at[i, k]) or str(df.at[i, k]).strip() in {"", "nan"}:
                v = rec.get(k)
                if v is not None:
                    df.at[i, k] = v
        wr = float(rec.get("win_rate_on_surface") or 0)
        nm = int(rec.get("n_matches_on_surface") or 0)
        if pd.isna(df.at[i, "surface_specialist"]) or str(df.at[i, "surface_specialist"]).strip() in {"", "nan"}:
            df.at[i, "surface_specialist"] = 1.0 if (wr > 0.65 and nm >= 15) else 0.0
        if pd.isna(df.at[i, "surface_struggle"]) or str(df.at[i, "surface_struggle"]).strip() in {"", "nan"}:
            df.at[i, "surface_struggle"] = 1.0 if (wr < 0.35 and nm >= 10) else 0.0
    stats["profile"] = prof_n
    return stats


def _prop_key(s: object) -> str:
    t = str(s or "").strip().lower()
    return re.sub(r"[^a-z0-9+]+", "", t)


def _enrich_lagged_stat_avgs(df: pd.DataFrame) -> dict[str, object]:
    """Pre-game last-5 / last-10 / season averages from prior actuals (no same-game leakage)."""
    dt = pd.to_datetime(df["game_date"], errors="coerce") if "game_date" in df.columns else pd.Series(pd.NaT, index=df.index)
    if dt.isna().all() and "file_date" in df.columns:
        dt = pd.to_datetime(df["file_date"], errors="coerce")
    actual = pd.to_numeric(df["actual_value"], errors="coerce") if "actual_value" in df.columns else pd.Series(np.nan, index=df.index)
    player = df["player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=df.index)
    prop_src = df["prop"] if "prop" in df.columns else df.get("prop_type", df.get("stat_type"))
    prop = prop_src.map(_prop_key) if prop_src is not None else pd.Series("", index=df.index)
    sport = df["sport"].astype(str).str.strip().str.upper() if "sport" in df.columns else pd.Series("", index=df.index)

    keys = pd.DataFrame({"sport": sport, "player": player, "prop": prop, "dt": dt}, index=df.index)
    games = pd.DataFrame(
        {"sport": sport, "player": player, "prop": prop, "dt": dt, "actual": actual},
        index=df.index,
    )
    games = games.loc[games["dt"].notna() & games["actual"].notna() & games["player"].ne("") & games["prop"].ne("")]
    games = games.drop_duplicates(["sport", "player", "prop", "dt"], keep="first")
    if games.empty:
        return {"games": 0, "stat_last5_avg": 0, "stat_last10_avg": 0, "stat_season_avg": 0, "fill_by_sport": {}}
    games = games.sort_values(["sport", "player", "prop", "dt"], kind="mergesort")
    grp = games.groupby(["sport", "player", "prop"], sort=False)
    games["_prior"] = grp["actual"].shift(1)
    g2 = games.groupby(["sport", "player", "prop"], sort=False)
    l5 = g2["_prior"].rolling(5, min_periods=3).mean()
    l10 = g2["_prior"].rolling(10, min_periods=5).mean()
    szn = g2["_prior"].expanding(min_periods=5).mean()
    games["_l5"] = l5.reset_index(level=[0, 1, 2], drop=True)
    games["_l10"] = l10.reset_index(level=[0, 1, 2], drop=True)
    games["_szn"] = szn.reset_index(level=[0, 1, 2], drop=True)

    merged = keys.merge(
        games[["sport", "player", "prop", "dt", "_l5", "_l10", "_szn"]],
        on=["sport", "player", "prop", "dt"],
        how="left",
    )
    merged.index = df.index
    stats: dict[str, object] = {"games": int(len(games))}
    for col, src in (
        ("stat_last5_avg", "_l5"),
        ("stat_last10_avg", "_l10"),
        ("stat_season_avg", "_szn"),
    ):
        if col not in df.columns:
            df[col] = np.nan
        cur = pd.to_numeric(df[col], errors="coerce")
        new = pd.to_numeric(merged[src], errors="coerce")
        before = int(cur.notna().sum())
        df[col] = cur.where(cur.notna(), new)
        stats[col] = int(df[col].notna().sum()) - before
    sport_u = df["sport"].astype(str).str.strip().str.upper()
    l5 = pd.to_numeric(df["stat_last5_avg"], errors="coerce")
    stats["fill_by_sport"] = {
        sp: float(l5.loc[sport_u.eq(sp)].notna().mean()) if int(sport_u.eq(sp).sum()) else 0.0
        for sp in ("WNBA", "MLB", "SOCCER", "TENNIS", "NBA")
    }
    return stats


_TOP3_FILES = (
    ("NBA", "Sports/NBA/data/nba_top3_vs_defense.csv"),
    ("WNBA", "Sports/WNBA/data/wnba_top3_vs_defense.csv"),
    ("MLB", "Sports/MLB/data/mlb_hitter_top3_vs_defense.csv"),
    ("NHL", "Sports/NHL/data/nhl_top3_vs_defense.csv"),
    ("SOCCER", "Sports/Soccer/data/soccer_top3_vs_defense.csv"),
)

_HOOP_SPORTS = frozenset({"NBA", "NBA1H", "NBA1Q", "WNBA", "WNBA1H", "WNBA1Q", "CBB", "WCBB"})
_CFB_SHARE_HINTS = (
    (("rush", "rushing"), "rush_att_share"),
    (("pass", "passing"), "pass_att_share"),
    (("rec", "receiving", "target", "tgt", "reception"), "rec_tgt_share"),
)


def _fill_numeric(dst: pd.Series, src: pd.Series) -> pd.Series:
    a = pd.to_numeric(dst, errors="coerce")
    b = pd.to_numeric(src, errors="coerce")
    return a.where(a.notna(), b)


def _cfb_share_col(prop: object) -> str:
    t = str(prop or "").strip().lower()
    for hints, col in _CFB_SHARE_HINTS:
        if any(h in t for h in hints):
            return col
    return "rec_tgt_share"


def _load_top3_maps(root: Path) -> dict[str, dict[tuple[str, str], dict[str, float]]]:
    """sport -> (player_norm, category) -> ranks."""
    out: dict[str, dict[tuple[str, str], dict[str, float]]] = {}
    for sport, rel in _TOP3_FILES:
        path = root / rel
        if not path.is_file():
            continue
        csv = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        if csv.empty or "PLAYER_NORM" not in csv.columns or "category" not in csv.columns:
            continue
        csv["PLAYER_NORM"] = csv["PLAYER_NORM"].map(_norm_name)
        csv["category"] = csv["category"].astype(str).str.strip().str.lower()
        csv["season_avg"] = pd.to_numeric(csv.get("season_avg"), errors="coerce")
        uniq = csv.dropna(subset=["PLAYER_NORM", "category"]).drop_duplicates(
            ["PLAYER_NORM", "category"], keep="last"
        )
        uniq = uniq.copy()
        uniq["league_stat_rank"] = uniq.groupby("category")["season_avg"].rank(
            ascending=False, method="min"
        )
        side = csv.get("leader_side", pd.Series("", index=csv.index)).astype(str).str.strip().str.lower()
        rank = pd.to_numeric(csv.get("rank_on_team"), errors="coerce")
        top = csv.loc[side.eq("top"), ["PLAYER_NORM", "category"]].copy()
        top["team_top3_rank"] = rank.loc[side.eq("top")].values
        bot = csv.loc[side.eq("bottom"), ["PLAYER_NORM", "category"]].copy()
        bot["team_bottom3_rank"] = rank.loc[side.eq("bottom")].values
        top = top.drop_duplicates(["PLAYER_NORM", "category"], keep="first")
        bot = bot.drop_duplicates(["PLAYER_NORM", "category"], keep="first")
        merged = uniq[["PLAYER_NORM", "category", "league_stat_rank", "season_avg"]].merge(
            top, on=["PLAYER_NORM", "category"], how="left"
        ).merge(bot, on=["PLAYER_NORM", "category"], how="left")
        mpg_col = "avg_min" if "avg_min" in csv.columns else ("avg_toi" if "avg_toi" in csv.columns else None)
        mpg_map: dict[str, float] = {}
        if mpg_col:
            extra = csv.dropna(subset=["PLAYER_NORM"]).drop_duplicates("PLAYER_NORM", keep="last")
            mpg_map = dict(
                zip(
                    extra["PLAYER_NORM"],
                    pd.to_numeric(extra[mpg_col], errors="coerce"),
                    strict=False,
                )
            )
        sport_map: dict[tuple[str, str], dict[str, float]] = {}
        for _, r in merged.iterrows():
            rec = {
                "league_stat_rank": r.get("league_stat_rank"),
                "team_top3_rank": r.get("team_top3_rank"),
                "team_bottom3_rank": r.get("team_bottom3_rank"),
                "min_per_game": mpg_map.get(r["PLAYER_NORM"]),
            }
            sport_map[(r["PLAYER_NORM"], r["category"])] = rec
        out[sport] = sport_map
    return out


def _enrich_stat_leaders(df: pd.DataFrame, root: Path) -> dict[str, object]:
    """Join team top/bottom-3 and league rank of season_avg for the prop's stat."""
    maps = _load_top3_maps(root)
    stats: dict[str, object] = {"sports": sorted(maps)}
    if not maps or df.empty:
        return stats
    _ensure_cols(df, ["team_top3_rank", "team_bottom3_rank", "league_stat_rank", "min_per_game"])
    sport = df["sport"].astype(str).str.strip().str.upper() if "sport" in df.columns else pd.Series("", index=df.index)
    player = df["player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=df.index)
    prop = df["prop"] if "prop" in df.columns else df.get("prop_type", pd.Series("", index=df.index))
    cats = [
        prop_to_category(sp, pr).strip().lower()
        for sp, pr in zip(sport, prop, strict=False)
    ]
    t3, b3, lg, mpg = [], [], [], []
    for sp, nn, cat in zip(sport, player, cats, strict=False):
        rec = maps.get(sp, {}).get((nn, cat)) if cat else None
        if not rec:
            t3.append(np.nan)
            b3.append(np.nan)
            lg.append(np.nan)
            mpg.append(np.nan)
            continue
        t3.append(rec.get("team_top3_rank"))
        b3.append(rec.get("team_bottom3_rank"))
        lg.append(rec.get("league_stat_rank"))
        mpg.append(rec.get("min_per_game"))
    idx = df.index
    df["team_top3_rank"] = _fill_numeric(df["team_top3_rank"], pd.Series(t3, index=idx))
    df["team_bottom3_rank"] = _fill_numeric(df["team_bottom3_rank"], pd.Series(b3, index=idx))
    df["league_stat_rank"] = _fill_numeric(df["league_stat_rank"], pd.Series(lg, index=idx))
    df["min_per_game"] = _fill_numeric(df["min_per_game"], pd.Series(mpg, index=idx))
    stats["top3"] = int(pd.to_numeric(df["team_top3_rank"], errors="coerce").notna().sum())
    stats["bottom3"] = int(pd.to_numeric(df["team_bottom3_rank"], errors="coerce").notna().sum())
    stats["league"] = int(pd.to_numeric(df["league_stat_rank"], errors="coerce").notna().sum())
    return stats


def _enrich_cbb_minutes(df: pd.DataFrame, root: Path) -> dict[str, object]:
    path = root / "Sports" / "CBB" / "cbb_boxscore_cache.csv"
    stats: dict[str, object] = {"cbb_rows": 0, "mpg": 0}
    if "sport" not in df.columns or not path.is_file():
        return stats
    sport = df["sport"].astype(str).str.strip().str.upper()
    mask = sport.eq("CBB") | sport.eq("WCBB")
    stats["cbb_rows"] = int(mask.sum())
    if not mask.any():
        return stats
    box = pd.read_csv(path, usecols=["player_norm", "MIN"], encoding="utf-8-sig", low_memory=False)
    box["MIN"] = pd.to_numeric(box["MIN"], errors="coerce")
    box = box.loc[box["MIN"].gt(0) & box["player_norm"].notna()]
    mpg = box.groupby(box["player_norm"].map(_norm_name), sort=False)["MIN"].mean()
    _ensure_cols(df, ["min_per_game", "min_season_avg"])
    names = df.loc[mask, "player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=df.index[mask])
    mapped = names.map(mpg)
    df.loc[mask, "min_per_game"] = _fill_numeric(df.loc[mask, "min_per_game"], mapped)
    df.loc[mask, "min_season_avg"] = _fill_numeric(df.loc[mask, "min_season_avg"], df.loc[mask, "min_per_game"])
    stats["mpg"] = int(pd.to_numeric(df.loc[mask, "min_per_game"], errors="coerce").notna().sum())
    return stats


def _enrich_cfb_volume(df: pd.DataFrame, root: Path) -> dict[str, object]:
    path = root / "Sports" / "CFB" / "data" / "reference" / "cfb_player_volume.csv"
    stats: dict[str, object] = {"cfb_rows": 0, "share": 0, "league": 0}
    if "sport" not in df.columns or not path.is_file():
        return stats
    sport = df["sport"].astype(str).str.strip().str.upper()
    mask = sport.eq("CFB")
    stats["cfb_rows"] = int(mask.sum())
    if not mask.any():
        return stats
    vol = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    vol["player_norm"] = vol["player_norm"].map(_norm_name)
    for col in ("rush_att_share", "pass_att_share", "rec_tgt_share"):
        if col in vol.columns:
            vol[col] = pd.to_numeric(vol[col], errors="coerce")
            vol[f"{col}_team_rank"] = vol.groupby("team_abbr")[col].rank(ascending=False, method="min")
            vol[f"{col}_team_bottom"] = vol.groupby("team_abbr")[col].rank(ascending=True, method="min")
            vol[f"{col}_league_rank"] = vol[col].rank(ascending=False, method="min")
    by_name = {r["player_norm"]: r for _, r in vol.iterrows()}
    _ensure_cols(df, ["team_top3_rank", "team_bottom3_rank", "league_stat_rank", "poss_share"])
    idx = df.index[mask]
    prop = df.loc[idx, "prop"] if "prop" in df.columns else df.loc[idx].get("prop_type")
    names = df.loc[idx, "player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=idx)
    t3, b3, lg, share = [], [], [], []
    for nn, pr in zip(names, prop, strict=False):
        rec = by_name.get(nn)
        if rec is None:
            t3.append(np.nan)
            b3.append(np.nan)
            lg.append(np.nan)
            share.append(np.nan)
            continue
        col = _cfb_share_col(pr)
        sv = rec.get(col)
        tr = rec.get(f"{col}_team_rank")
        br = rec.get(f"{col}_team_bottom")
        lr = rec.get(f"{col}_league_rank")
        share.append(sv)
        lg.append(lr)
        try:
            trf = float(tr)
        except (TypeError, ValueError):
            trf = float("nan")
        try:
            brf = float(br)
        except (TypeError, ValueError):
            brf = float("nan")
        t3.append(trf if trf == trf and trf <= 3 else np.nan)
        b3.append(brf if brf == brf and brf <= 3 else np.nan)
    df.loc[idx, "poss_share"] = _fill_numeric(df.loc[idx, "poss_share"], pd.Series(share, index=idx))
    df.loc[idx, "team_top3_rank"] = _fill_numeric(df.loc[idx, "team_top3_rank"], pd.Series(t3, index=idx))
    df.loc[idx, "team_bottom3_rank"] = _fill_numeric(df.loc[idx, "team_bottom3_rank"], pd.Series(b3, index=idx))
    df.loc[idx, "league_stat_rank"] = _fill_numeric(df.loc[idx, "league_stat_rank"], pd.Series(lg, index=idx))
    stats["share"] = int(pd.to_numeric(df.loc[idx, "poss_share"], errors="coerce").notna().sum())
    stats["league"] = int(pd.to_numeric(df.loc[idx, "league_stat_rank"], errors="coerce").notna().sum())
    _ensure_cols(df, ["minutes_tier"])
    shares = pd.to_numeric(df.loc[idx, "poss_share"], errors="coerce")
    tiers = shares.map(lambda v: "HIGH" if v >= 0.25 else ("MEDIUM" if v >= 0.10 else ("LOW" if v == v else "")))
    blank = df.loc[idx, "minutes_tier"].isna() | df.loc[idx, "minutes_tier"].astype(str).str.strip().isin(("", "UNKNOWN", "nan"))
    df.loc[idx, "minutes_tier"] = df.loc[idx, "minutes_tier"].where(~blank, tiers)
    return stats


def _enrich_nfl_snaps(df: pd.DataFrame, root: Path) -> dict[str, object]:
    path = root / "Sports" / "NFL" / "data" / "nfl_snap_pct_cache.json"
    stats: dict[str, object] = {"nfl_rows": 0, "snap": 0}
    if "sport" not in df.columns or not path.is_file():
        return stats
    sport = df["sport"].astype(str).str.strip().str.upper()
    mask = sport.isin({"NFL", "NFLP"})
    stats["nfl_rows"] = int(mask.sum())
    if not mask.any():
        return stats
    payload = _load_json(path)
    players = payload.get("players") if isinstance(payload, dict) else {}
    if not isinstance(players, dict):
        return stats
    _ensure_cols(df, ["snap_pct", "minutes_tier"])
    idx = df.index[mask]
    names = df.loc[idx, "player"].map(_norm_name) if "player" in df.columns else pd.Series("", index=idx)
    snaps, tiers = [], []
    for nn in names:
        rec = players.get(nn) or {}
        raw = rec.get("snap_pct_L3")
        if raw is None:
            raw = rec.get("snap_pct_season")
        try:
            v = float(raw)
        except (TypeError, ValueError):
            v = float("nan")
        snaps.append(v)
        tiers.append(snap_pct_to_minutes_tier(v) if v == v else "")
    df.loc[idx, "snap_pct"] = _fill_numeric(df.loc[idx, "snap_pct"], pd.Series(snaps, index=idx))
    blank = df.loc[idx, "minutes_tier"].isna() | df.loc[idx, "minutes_tier"].astype(str).str.strip().isin(("", "UNKNOWN", "nan"))
    df.loc[idx, "minutes_tier"] = df.loc[idx, "minutes_tier"].where(~blank, pd.Series(tiers, index=idx))
    stats["snap"] = int(pd.to_numeric(df.loc[idx, "snap_pct"], errors="coerce").notna().sum())
    return stats


def _enrich_raw_minutes_tier(df: pd.DataFrame) -> dict[str, object]:
    """Overwrite basketball minutes_tier from raw MPG, not stored labels."""
    stats: dict[str, object] = {"hoop_rows": 0, "known": 0}
    if "sport" not in df.columns:
        return stats
    sport = df["sport"].astype(str).str.strip().str.upper()
    mask = sport.isin(_HOOP_SPORTS)
    stats["hoop_rows"] = int(mask.sum())
    if not mask.any():
        return stats
    _ensure_cols(df, ["minutes_tier"])
    sub = assign_minutes_tier(df.loc[mask].copy(), mpg_col=None)
    df.loc[mask, "minutes_tier"] = sub["minutes_tier"].values
    stats["known"] = int(df.loc[mask, "minutes_tier"].astype(str).str.upper().isin({"LOW", "MEDIUM", "HIGH"}).sum())
    return stats


def _enrich_dir_l10(df: pd.DataFrame) -> dict[str, object]:
    """Fill hit_rate_l10 (0–1 rate) from directional L10 counts so dir_l10 is usable."""
    stats: dict[str, object] = {"filled": 0}
    if "l10_over" not in df.columns:
        return stats
    _ensure_cols(df, ["hit_rate_l10"])
    over = df["direction"].astype(str).str.strip().str.upper().eq("OVER") if "direction" in df.columns else pd.Series(True, index=df.index)
    ov = pd.to_numeric(df["l10_over"], errors="coerce")
    un = pd.to_numeric(df["l10_under"], errors="coerce") if "l10_under" in df.columns else pd.Series(np.nan, index=df.index)
    hits = ov.where(over, un)
    played = ov.add(un, fill_value=0)
    rate = hits.divide(played.where(played > 0))
    before = pd.to_numeric(df["hit_rate_l10"], errors="coerce")
    df["hit_rate_l10"] = before.where(before.notna(), rate)
    stats["filled"] = int(df["hit_rate_l10"].notna().sum()) - int(before.notna().sum())
    return stats


def enrich_retrain_dataframe(df: pd.DataFrame, root: Path | None = None) -> pd.DataFrame:
    root = Path(root) if root is not None else _REPO
    wnba = _enrich_wnba(df, root)
    nba = _enrich_nba(df, root)
    tennis = _enrich_tennis(df, root)
    lagged = _enrich_lagged_stat_avgs(df)
    leaders = _enrich_stat_leaders(df, root)
    cbb_min = _enrich_cbb_minutes(df, root)
    cfb = _enrich_cfb_volume(df, root)
    nfl = _enrich_nfl_snaps(df, root)
    mpg_tier = _enrich_raw_minutes_tier(df)
    l10 = _enrich_dir_l10(df)
    print(
        "[enrich] WNBA "
        f"rows={wnba['wnba_rows']:,} usage={wnba['usage']:,} pace={wnba['pace']:,} "
        f"stat_def={wnba['stat_def']:,} star1={wnba['star']:,}"
    )
    print(
        "[enrich] NBA "
        f"rows={nba['nba_rows']:,} usage={nba['usage']:,} pace={nba['pace']:,} "
        f"stat_def={nba['stat_def']:,}"
    )
    print(
        "[enrich] Tennis "
        f"rows={tennis['tennis_rows']:,} surface={tennis['surface']:,} "
        f"profile={tennis['profile']:,}"
    )
    fill = lagged.get("fill_by_sport") or {}
    print(
        "[enrich] lagged last5 avg "
        f"new_l5={lagged.get('stat_last5_avg', 0):,} games={lagged.get('games', 0):,} "
        + " ".join(f"{sp}={fill.get(sp, 0):.1%}" for sp in ("WNBA", "MLB", "SOCCER", "TENNIS", "NBA"))
    )
    print(
        "[enrich] role leaders "
        f"top3={leaders.get('top3', 0):,} bottom3={leaders.get('bottom3', 0):,} "
        f"league={leaders.get('league', 0):,}"
    )
    print(
        f"[enrich] CBB mpg={cbb_min.get('mpg', 0):,} "
        f"CFB share={cfb.get('share', 0):,} NFL snap={nfl.get('snap', 0):,} "
        f"hoop minutes_tier={mpg_tier.get('known', 0):,} dir_l10_filled={l10.get('filled', 0):,}"
    )
    return df


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=_REPO / "data" / "retrain_dataset.csv")
    ap.add_argument("--output", type=Path, default=None)
    ap.add_argument(
        "--lagged-only",
        action="store_true",
        help="Only attach pre-game last-5/10/season avgs from prior actuals (skip usage/D/surface caches).",
    )
    args = ap.parse_args()
    inp = args.input if args.input.is_absolute() else (_REPO / args.input)
    out = args.output if args.output is not None else inp
    if not out.is_absolute():
        out = _REPO / out
    print(f"[enrich] loading {inp}")
    df = pd.read_csv(inp, low_memory=False, encoding="utf-8-sig")
    if args.lagged_only:
        lagged = _enrich_lagged_stat_avgs(df)
        fill = lagged.get("fill_by_sport") or {}
        print(
            "[enrich] lagged last5 avg "
            f"new_l5={lagged.get('stat_last5_avg', 0):,} games={lagged.get('games', 0):,} "
            + " ".join(f"{sp}={fill.get(sp, 0):.1%}" for sp in ("WNBA", "MLB", "SOCCER", "TENNIS", "NBA"))
        )
    else:
        df = enrich_retrain_dataframe(df, _REPO)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[enrich] wrote {out} rows={len(df):,}")
    sport = df["sport"].astype(str).str.strip().str.upper()
    for sp, col in (
        ("WNBA", "usage_pct"),
        ("WNBA", "stat_def_rank"),
        ("TENNIS", "surface_encoded"),
        ("NBA", "usage_pct"),
        ("WNBA", "stat_last5_avg"),
        ("MLB", "stat_last5_avg"),
        ("SOCCER", "stat_last5_avg"),
        ("TENNIS", "stat_last5_avg"),
    ):
        sub = df.loc[sport.eq(sp), col] if col in df.columns else pd.Series(dtype=float)
        fill = float(sub.notna().mean()) if len(sub) else 0.0
        print(f"  {sp} {col} fill={fill:.1%} n={len(sub):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
