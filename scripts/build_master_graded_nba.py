#!/usr/bin/env python3
"""Build NBA master graded-props parquet for walk-forward gate studies.

Output (live tree):
  data/training/master_graded_<YYYY-MM-DD>.parquet
  data/training/master_graded_<YYYY-MM-DD>_README.md

Sources (read-only; does not change pipeline scripts):
  - data/training/graded_export_20260516.csv  (2026-02-19 .. 2026-05-03)
  - outputs/<day>/nba/step1_pp_props_today.csv  (this season boards)
  - data/cache/proporacle_ref.db               (nba box scores)
  - data/line_history.db                       (first/last line + fetch count)
  - data/cache/game_lines.db                   (spread / total)
  - rolling team defense/pace from proporacle_ref.db (day-as-of, no leakage)

Features use only games strictly before the prop's game_date (no leakage).
"""
from __future__ import annotations

import argparse
import sqlite3
import unicodedata
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")

PROP_TO_COL = {
    "points": "pts",
    "rebounds": "reb",
    "assists": "ast",
    "steals": "stl",
    "blocked shots": "blk",
    "blocks": "blk",
    "turnovers": "tov",
    "3-pt made": "fg3m",
    "3-pt attempted": "fg3a",
    "fg made": "fgm",
    "fg attempted": "fga",
    "free throws made": "ftm",
    "free throws attempted": "fta",
    "two pointers made": "fg2m",
    "two pointers attempted": "fg2a",
    "pts+rebs+asts": "pra",
    "pts+rebs": "pr",
    "pts+asts": "pa",
    "rebs+asts": "ra",
    "reb+ast": "ra",
    "blks+stls": "bs",
    "fantasy score": "fantasy_score",
    "offensive rebounds": "oreb",
    "defensive rebounds": "dreb",
    "personal fouls": "pf",
}

TEAM_ALIASES = {
    "NYK": "NY",
    "NOP": "NO",
    "SAS": "SA",
    "GSW": "GS",
    "PHX": "PHO",
    "UTAH": "UTA",
    "WSH": "WAS",
    "BRK": "BKN",
}

# Strict 30 NBA clubs after aliasing (exclude All-Star / exhibition / foreign).
# Codes match norm_team() output (PHX→PHO, WSH→WAS, etc.).
NBA_TEAMS = {
    "ATL", "BKN", "BOS", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GS",
    "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NO", "NY",
    "OKC", "ORL", "PHI", "PHO", "POR", "SA", "SAC", "TOR", "UTA", "WAS",
}


def nm(s: object) -> str:
    t = unicodedata.normalize("NFD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    return (
        t.lower()
        .replace(".", "")
        .replace("'", "")
        .replace(" jr", "")
        .replace(" iii", "")
        .replace(" ii", "")
        .strip()
    )


def norm_team(t: object) -> str:
    s = str(t or "").strip().upper()
    return TEAM_ALIASES.get(s, s)


def prop_col(prop: object) -> str | None:
    key = str(prop or "").strip().lower()
    return PROP_TO_COL.get(key)


def season_start(ymd: str) -> str:
    y = int(str(ymd)[:4])
    # NBA season opens ~Oct 1 of calendar year for Oct–Jun games.
    return f"{y}-10-01" if str(ymd)[5:7] >= "10" else f"{y - 1}-10-01"


def load_boxes(db: Path) -> pd.DataFrame:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    cols = [r[1] for r in con.execute("pragma table_info(nba)")]
    want = [
        "game_date",
        "player",
        "team",
        "home_team",
        "away_team",
        "minutes",
        "pts",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "fgm",
        "fga",
        "fg3m",
        "fg3a",
        "fg2m",
        "fg2a",
        "ftm",
        "fta",
        "oreb",
        "dreb",
        "pf",
        "pra",
        "pr",
        "pa",
        "ra",
        "bs",
        "fantasy_score",
        "starter",
        "season_window",
    ]
    use = [c for c in want if c in cols]
    df = pd.read_sql(f"SELECT {','.join(use)} FROM nba", con)
    con.close()
    df["game_date"] = df["game_date"].astype(str).str[:10]
    df["k"] = df["player"].map(nm)
    df["team_n"] = df["team"].map(norm_team)
    for c in use:
        if c in ("game_date", "player", "team", "home_team", "away_team", "season_window"):
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce")
    # Card window: RS + playoffs only (exclude preseason).
    if "season_window" in df.columns:
        sw = df["season_window"].astype(str).str.lower()
        keep = ~sw.str.contains("pre", na=False)
        df = df.loc[keep].copy()
    df = df.sort_values(["k", "game_date"]).reset_index(drop=True)
    return df


def index_boxes(boxes: pd.DataFrame) -> dict[str, dict]:
    """Per-player arrays for fast as-of cuts."""
    out: dict[str, dict] = {}
    for k, g in boxes.groupby("k", sort=False):
        g = g.reset_index(drop=True)
        out[str(k)] = {
            "df": g,
            "dates": g["game_date"].to_numpy(),
        }
    return out


def prior_games(player_rec: dict, before: str) -> pd.DataFrame:
    dates = player_rec["dates"]
    i = int(np.searchsorted(dates, before, side="left"))
    return player_rec["df"].iloc[:i]


def window_hits(
    prior: pd.DataFrame,
    col: str,
    line: float,
    side: str,
    n: int,
) -> tuple[float, float, float, float, float]:
    """Return (hits, decided, avg, gap, gap_sd) on last n decided games vs line."""
    if prior.empty or col not in prior.columns:
        return (np.nan, 0.0, np.nan, np.nan, np.nan)
    sub = prior.tail(n)
    vals = pd.to_numeric(sub[col], errors="coerce")
    mins = pd.to_numeric(sub["minutes"], errors="coerce") if "minutes" in sub.columns else vals * 0 + 1
    ok = vals.notna() & mins.notna() & (mins > 0)
    vals = vals[ok]
    if vals.empty:
        return (np.nan, 0.0, np.nan, np.nan, np.nan)
    if side == "UNDER":
        hits = (vals < line).sum()
    else:
        hits = (vals > line).sum()
    # pushes excluded from decided
    decided = (vals != line).sum()
    hit_count = float(hits) if decided else np.nan
    avg = float(vals.mean())
    gap = avg - float(line)
    gap_sd = float((vals - line).std(ddof=0)) if len(vals) > 1 else 0.0
    return (hit_count, float(decided), avg, gap, gap_sd)


def season_hits(prior: pd.DataFrame, col: str, line: float, side: str, season_open: str) -> tuple[float, float]:
    sub = prior[prior["game_date"] >= season_open]
    if sub.empty or col not in sub.columns:
        return (np.nan, 0.0)
    vals = pd.to_numeric(sub[col], errors="coerce")
    mins = pd.to_numeric(sub["minutes"], errors="coerce") if "minutes" in sub.columns else vals * 0 + 1
    ok = vals.notna() & mins.notna() & (mins > 0) & (vals != line)
    vals = vals[ok]
    if vals.empty:
        return (np.nan, 0.0)
    hits = (vals < line).sum() if side == "UNDER" else (vals > line).sum()
    return (float(hits), float(len(vals)))


def load_export_nba(path: Path) -> pd.DataFrame:
    d = pd.read_csv(path, low_memory=False)
    d = d[d["sport"].astype(str).str.upper() == "NBA"].copy()
    d["pick_type"] = d["pick_type"].astype(str).str.strip()
    d["pick_type"] = d["pick_type"].str.replace("^goblin$", "Goblin", case=False, regex=True)
    d["pick_type"] = d["pick_type"].str.replace("^standard$", "Standard", case=False, regex=True)
    d["pick_type"] = d["pick_type"].str.replace("^demon$", "Demon", case=False, regex=True)
    d = d[d["pick_type"].isin(["Goblin", "Standard", "Demon"])].copy()
    d["side"] = d["direction"].astype(str).str.upper().str.strip()
    d["side"] = d["side"].replace({"OVER": "OVER", "UNDER": "UNDER"})
    d = d[d["side"].isin(["OVER", "UNDER"])].copy()
    # Goblin/Demon Unders are invalid labels — keep Overs only for those.
    d = d[~((d["pick_type"].isin(["Goblin", "Demon"])) & (d["side"] == "UNDER"))].copy()
    proj = d["pp_projection_id"] if "pp_projection_id" in d.columns else pd.Series([""] * len(d))
    proj = (
        proj.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": "", "NaN": ""})
    )
    out = pd.DataFrame(
        {
            "game_date": d["file_date"].astype(str).str[:10],
            "player": d["player"].astype(str),
            "team": d["team"].astype(str),
            "opp_team": d["opp_team"].astype(str),
            "prop": d["prop"].astype(str),
            "pick_type": d["pick_type"].astype(str),
            "side": d["side"].astype(str),
            "line": pd.to_numeric(d["line"], errors="coerce"),
            "projection_id": proj,
            "actual": pd.to_numeric(d["actual_value"], errors="coerce"),
            "result": d["result"].astype(str).str.upper(),
            "void_reason": d.get("void_reason", pd.Series([""] * len(d))).astype(str),
            "source": "graded_export_20260516",
            "season_tag": "2025-26_late",
        }
    )
    out["hit"] = np.where(out["result"] == "HIT", 1, np.where(out["result"] == "MISS", 0, np.nan))
    out["push"] = (out["result"] == "PUSH").astype(int)
    out["void"] = out["result"].isin(["VOID", "DNP"]).astype(int) | out["void_reason"].str.len().gt(0).astype(int)
    return out.dropna(subset=["line"])


def load_season_boards(outputs: Path, boxes_idx: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict] = []
    files = sorted(outputs.glob("*/nba/step1_pp_props_today.csv"))
    for f in files:
        day = f.parent.parent.name
        if len(day) != 10:
            continue
        try:
            d = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)
        except Exception:
            continue
        if "pick_type" not in d.columns:
            continue
        d = d[d["pick_type"].isin(["Standard", "Goblin", "Demon"])].copy()
        if "start_time" in d.columns:
            st = pd.to_datetime(d["start_time"], utc=True, errors="coerce")
            d["_gd"] = st.dt.tz_convert(ET).dt.strftime("%Y-%m-%d")
            d = d[d["_gd"] == day]
        prop_col_name = "prop_type" if "prop_type" in d.columns else "prop"
        d = d.drop_duplicates(["player", prop_col_name, "pick_type", "line"], keep="first")
        for r in d.itertuples(index=False):
            prop = getattr(r, prop_col_name, None)
            col = prop_col(prop)
            if not col:
                continue
            k = nm(getattr(r, "player", ""))
            prior_player = boxes_idx.get(k)
            if prior_player is None:
                continue
            # Need same-day box for grading
            pdf = prior_player["df"]
            same = pdf[pdf["game_date"] == day]
            if same.empty:
                continue
            box = same.iloc[-1]
            mins = box.get("minutes")
            act = box.get(col)
            proj = ""
            for attr in ("projection_id", "pp_projection_id"):
                if hasattr(r, attr):
                    v = getattr(r, attr)
                    if v is not None and str(v) not in ("", "nan", "None"):
                        proj = str(v).split(".")[0]
                        break
            base_ids = {
                "game_date": day,
                "player": r.player,
                "team": getattr(r, "team", ""),
                "opp_team": getattr(r, "opp_team", getattr(r, "opponent", "")),
                "prop": prop,
                "pick_type": r.pick_type,
                "projection_id": proj,
            }
            if pd.isna(mins) or float(mins) <= 0 or pd.isna(act):
                # DNP / missing actual
                for side in (["OVER", "UNDER"] if r.pick_type == "Standard" else ["OVER"]):
                    rows.append(
                        {
                            **base_ids,
                            "side": side,
                            "line": float(r.line),
                            "actual": np.nan,
                            "result": "VOID",
                            "void_reason": "DNP_or_missing_actual",
                            "hit": np.nan,
                            "push": 0,
                            "void": 1,
                            "source": "step1+ref_db",
                            "season_tag": "2026-27",
                        }
                    )
                continue
            act_f = float(act)
            line = float(r.line)
            sides = ["OVER", "UNDER"] if r.pick_type == "Standard" else ["OVER"]
            for side in sides:
                if act_f == line:
                    hit, push, result = np.nan, 1, "PUSH"
                elif (side == "OVER" and act_f > line) or (side == "UNDER" and act_f < line):
                    hit, push, result = 1, 0, "HIT"
                else:
                    hit, push, result = 0, 0, "MISS"
                rows.append(
                    {
                        **base_ids,
                        "side": side,
                        "line": line,
                        "actual": act_f,
                        "result": result,
                        "void_reason": "",
                        "hit": hit,
                        "push": push,
                        "void": 0,
                        "source": "step1+ref_db",
                        "season_tag": "2026-27",
                    }
                )
    return pd.DataFrame(rows)


def attach_line_moves(df: pd.DataFrame, lh_db: Path) -> pd.DataFrame:
    """First/last line per projection_id (one Goblin/Demon rung), never across rungs."""
    df = df.copy()
    if "projection_id" not in df.columns:
        df["projection_id"] = ""
    df["projection_id"] = (
        df["projection_id"]
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": "", "NaN": ""})
    )
    df["first_line"] = df["line"]
    df["last_line"] = df["line"]
    df["n_fetches"] = np.nan
    df["line_move"] = 0.0
    if not lh_db.exists() or df.empty:
        return df
    dates = sorted(
        d for d in df["game_date"].astype(str).unique().tolist() if d >= "2026-10-01"
    )
    if not dates:
        return df
    con = sqlite3.connect(f"file:{lh_db}?mode=ro", uri=True)
    placeholders = ",".join("?" * len(dates))
    hist = pd.read_sql(
        f"""
        SELECT projection_id, player, prop_type AS prop, pick_type, game_date,
               line, fetched_at
        FROM line_history
        WHERE sport='NBA' AND game_date IN ({placeholders})
          AND projection_id IS NOT NULL AND TRIM(projection_id) != ''
        ORDER BY fetched_at
        """,
        con,
        params=dates,
    )
    con.close()
    print(f"  line_history rows pulled={len(hist):,}", flush=True)
    if hist.empty:
        return df
    hist["projection_id"] = hist["projection_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    hist["pick_type"] = hist["pick_type"].astype(str)
    # One rung = one projection_id (lines may still move within that id).
    g = hist.groupby("projection_id", sort=False)
    agg = g.agg(
        first_line=("line", "first"),
        last_line=("line", "last"),
        n_fetches=("line", "size"),
    ).reset_index()
    # Drop prior fill columns before merge
    df = df.drop(columns=["first_line", "last_line", "n_fetches", "line_move"], errors="ignore")
    df = df.merge(agg, how="left", on="projection_id")
    df["first_line"] = pd.to_numeric(df["first_line"], errors="coerce")
    df["last_line"] = pd.to_numeric(df["last_line"], errors="coerce")
    # Rows without a projection match stay NaN (not cross-rung fill).
    matched = df["n_fetches"].notna()
    df.loc[~matched, "first_line"] = np.nan
    df.loc[~matched, "last_line"] = np.nan
    df["line_move"] = np.where(
        matched,
        df["last_line"] - df["first_line"],
        np.nan,
    )
    print(
        f"  line_move matched by projection_id: {int(matched.sum()):,}/{len(df):,}",
        flush=True,
    )
    return df


def attach_spreads(df: pd.DataFrame, gl_db: Path) -> pd.DataFrame:
    df = df.copy()
    df["spread"] = np.nan
    df["total"] = np.nan
    df["favorite"] = ""
    df["home_away"] = ""
    if not gl_db.exists():
        return df
    con = sqlite3.connect(f"file:{gl_db}?mode=ro", uri=True)
    gl = pd.read_sql(
        "SELECT game_date, home_team, away_team, spread, total FROM game_lines WHERE sport='NBA'",
        con,
    )
    con.close()
    if gl.empty:
        return df
    gl["home_n"] = gl["home_team"].map(norm_team)
    gl["away_n"] = gl["away_team"].map(norm_team)
    # index by date+team
    by_home = {
        (r.game_date, r.home_n): r for r in gl.itertuples(index=False)
    }
    by_away = {
        (r.game_date, r.away_n): r for r in gl.itertuples(index=False)
    }
    spreads, totals, favs, ha = [], [], [], []
    for r in df.itertuples(index=False):
        tn = norm_team(r.team)
        key = (r.game_date, tn)
        row = by_home.get(key) or by_away.get(key)
        if row is None:
            spreads.append(np.nan)
            totals.append(np.nan)
            favs.append("")
            ha.append("")
            continue
        sp = float(row.spread) if row.spread is not None else np.nan
        tot = float(row.total) if row.total is not None else np.nan
        spreads.append(sp)
        totals.append(tot)
        # ESPN spread is home-relative; favorite = home if spread<0 else away
        if pd.isna(sp):
            favs.append("")
        elif sp < 0:
            favs.append("HOME" if key in by_home else "AWAY")
        elif sp > 0:
            favs.append("AWAY" if key in by_home else "HOME")
        else:
            favs.append("PICK")
        ha.append("HOME" if key in by_home else "AWAY")
    df["spread"] = spreads
    df["total"] = totals
    df["favorite"] = favs
    df["home_away"] = ha
    return df


def build_team_games(boxes: pd.DataFrame) -> pd.DataFrame:
    """One row per team-game with points for/against (no leakage source for rolling).

    Pair by (game_date, home, away) — never date-only (that cartesian-producted
    multi-game nights and inflated opp_def_rank past 30). Drop non-NBA codes.
    """
    g = (
        boxes.groupby(["game_date", "team_n"], as_index=False)
        .agg(team_pts=("pts", "sum"), home_team=("home_team", "first"), away_team=("away_team", "first"))
    )
    g["home_n"] = g["home_team"].map(norm_team)
    g["away_n"] = g["away_team"].map(norm_team)
    g = g[
        g["team_n"].isin(NBA_TEAMS)
        & g["home_n"].isin(NBA_TEAMS)
        & g["away_n"].isin(NBA_TEAMS)
    ].copy()
    home = g[g["team_n"] == g["home_n"]][["game_date", "home_n", "away_n", "team_pts"]].rename(
        columns={"team_pts": "home_pts"}
    )
    away = g[g["team_n"] == g["away_n"]][["game_date", "home_n", "away_n", "team_pts"]].rename(
        columns={"team_pts": "away_pts"}
    )
    games = home.merge(away, on=["game_date", "home_n", "away_n"], how="inner")
    rows = []
    for r in games.itertuples(index=False):
        rows.append(
            {
                "game_date": r.game_date,
                "team_n": r.home_n,
                "opp_n": r.away_n,
                "team_pts": float(r.home_pts),
                "opp_pts": float(r.away_pts),
                "pace_proxy": float(r.home_pts) + float(r.away_pts),
            }
        )
        rows.append(
            {
                "game_date": r.game_date,
                "team_n": r.away_n,
                "opp_n": r.home_n,
                "team_pts": float(r.away_pts),
                "opp_pts": float(r.home_pts),
                "pace_proxy": float(r.home_pts) + float(r.away_pts),
            }
        )
    out = pd.DataFrame(rows).sort_values(["team_n", "game_date"]).reset_index(drop=True)
    return out


def index_team_games(tg: pd.DataFrame) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for k, g in tg.groupby("team_n", sort=False):
        g = g.reset_index(drop=True)
        out[str(k)] = {
            "dates": g["game_date"].to_numpy(),
            "opp_pts": g["opp_pts"].to_numpy(dtype=float),
            "pace": g["pace_proxy"].to_numpy(dtype=float),
            "opp_n": g["opp_n"].to_numpy(),
            "df": g,
        }
    return out


def _rolling_mean_before(rec: dict, before: str, n: int, field: str) -> float:
    dates = rec["dates"]
    i = int(np.searchsorted(dates, before, side="left"))
    if i <= 0:
        return np.nan
    arr = rec[field][max(0, i - n) : i]
    if arr.size == 0:
        return np.nan
    return float(np.nanmean(arr))


def attach_opp_context(df: pd.DataFrame, boxes: pd.DataFrame, team_idx: dict[str, dict]) -> pd.DataFrame:
    """Fill opp_team from boxes; day-as-of opp defense rank + pace flag (games before prop day only)."""
    df = df.copy()
    # Prefer team_idx df rows for opp lookup
    opp_lookup: dict[tuple[str, str], str] = {}
    for team, rec in team_idx.items():
        for d, o in zip(rec["dates"], rec["opp_n"]):
            opp_lookup[(str(d), str(team))] = str(o)

    opp_filled = []
    for r in df.itertuples(index=False):
        cur = str(getattr(r, "opp_team", "") or "")
        if cur and cur not in ("—", "-", "nan", "None", ""):
            opp_filled.append(norm_team(cur))
            continue
        opp_filled.append(opp_lookup.get((str(r.game_date), norm_team(r.team)), ""))
    df["opp_team"] = opp_filled

    # Precompute for each calendar date in df: team -> L10 opp_pts allowed (before that date)
    dates = sorted(df["game_date"].astype(str).unique().tolist())
    # Cache rolling metrics: (team, date) -> (opp_pts_l10, pace_l10)
    roll: dict[tuple[str, str], tuple[float, float]] = {}
    for d in dates:
        for team, rec in team_idx.items():
            roll[(team, d)] = (
                _rolling_mean_before(rec, d, 10, "opp_pts"),
                _rolling_mean_before(rec, d, 10, "pace"),
            )

    # Day-as-of ranks: among the 30 NBA teams with a value that day (1=stingiest D, ≤30).
    def_rank: dict[tuple[str, str], float] = {}
    pace_flag_map: dict[tuple[str, str], str] = {}
    nba_team_list = [t for t in team_idx if t in NBA_TEAMS]
    for d in dates:
        rows = []
        for team in nba_team_list:
            op, pc = roll[(team, d)]
            if not np.isnan(op):
                rows.append((team, op, pc))
        if not rows:
            continue
        by_def = sorted(rows, key=lambda t: t[1])  # low opp pts = best D
        for i, (team, op, pc) in enumerate(by_def, start=1):
            def_rank[(team, d)] = float(i)
        paces = [pc for _, _, pc in rows if not np.isnan(pc)]
        if len(paces) >= 9:
            q1, q2 = np.quantile(paces, [1 / 3, 2 / 3])
            for team, op, pc in rows:
                if np.isnan(pc):
                    pace_flag_map[(team, d)] = ""
                elif pc >= q2:
                    pace_flag_map[(team, d)] = "Fast"
                elif pc <= q1:
                    pace_flag_map[(team, d)] = "Slow"
                else:
                    pace_flag_map[(team, d)] = "Avg"

    def _tier(x):
        if pd.isna(x):
            return ""
        x = float(x)
        if x <= 6:
            return "HARD"
        if x <= 12:
            return "HARD_MID"
        if x <= 18:
            return "MID"
        if x <= 24:
            return "EASY_MID"
        return "EASY"

    opp_def_rank, opp_def_tier, opp_pts_l10, pace_flag, team_pace_l10 = [], [], [], [], []
    for r in df.itertuples(index=False):
        d = str(r.game_date)
        opp = norm_team(r.opp_team)
        team = norm_team(r.team)
        rk = def_rank.get((opp, d), np.nan)
        opp_def_rank.append(rk)
        opp_def_tier.append(_tier(rk))
        op_l10, _ = roll.get((opp, d), (np.nan, np.nan))
        _, tm_pace = roll.get((team, d), (np.nan, np.nan))
        opp_pts_l10.append(op_l10)
        team_pace_l10.append(tm_pace)
        pace_flag.append(pace_flag_map.get((team, d), ""))

    df["opp_def_rank"] = opp_def_rank
    df["opp_def_tier"] = opp_def_tier
    df["opp_pts_allowed_l10"] = opp_pts_l10
    df["team_pace_l10"] = team_pace_l10
    df["pace_flag"] = pace_flag
    return df


def attach_features(df: pd.DataFrame, boxes_idx: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Faster path: numpy windows per player; progress every 25k rows."""
    n = len(df)
    stat_col = np.empty(n, dtype=object)
    l5_hits = np.full(n, np.nan)
    l5_n = np.zeros(n)
    l10_hits = np.full(n, np.nan)
    l10_n = np.zeros(n)
    l20_hits = np.full(n, np.nan)
    l20_n = np.zeros(n)
    l5_avg = np.full(n, np.nan)
    l10_avg = np.full(n, np.nan)
    gap = np.full(n, np.nan)
    gap_sd = np.full(n, np.nan)
    season_hits_a = np.full(n, np.nan)
    season_n_a = np.zeros(n)
    season_hr = np.full(n, np.nan)
    minutes_l5 = np.full(n, np.nan)
    starter_l5 = np.full(n, np.nan)
    missing_prop = 0

    players = df["player"].to_numpy()
    props = df["prop"].to_numpy()
    lines = pd.to_numeric(df["line"], errors="coerce").to_numpy()
    sides = df["side"].to_numpy()
    dates = df["game_date"].astype(str).to_numpy()

    for i in range(n):
        if i and i % 25000 == 0:
            print(f"  features {i:,}/{n:,}", flush=True)
        col = prop_col(props[i])
        stat_col[i] = col or ""
        if not col or np.isnan(lines[i]):
            missing_prop += int(not col)
            continue
        prec = boxes_idx.get(nm(players[i]))
        if prec is None:
            continue
        prior = prior_games(prec, dates[i])
        if prior.empty or col not in prior.columns:
            continue
        line = float(lines[i])
        side = str(sides[i])
        h5, n5, a5, g5, sd5 = window_hits(prior, col, line, side, 5)
        h10, n10, a10, _, _ = window_hits(prior, col, line, side, 10)
        h20, n20, _, _, _ = window_hits(prior, col, line, side, 20)
        sh, sn = season_hits(prior, col, line, side, season_start(dates[i]))
        l5_hits[i], l5_n[i] = h5, n5
        l10_hits[i], l10_n[i] = h10, n10
        l20_hits[i], l20_n[i] = h20, n20
        l5_avg[i], l10_avg[i] = a5, a10
        gap[i], gap_sd[i] = g5, sd5
        season_hits_a[i], season_n_a[i] = sh, sn
        season_hr[i] = (sh / sn) if sn else np.nan
        last5 = prior.tail(5)
        minutes_l5[i] = float(pd.to_numeric(last5["minutes"], errors="coerce").mean())
        if "starter" in last5.columns:
            starter_l5[i] = float(pd.to_numeric(last5["starter"], errors="coerce").fillna(0).mean())

    out = df.copy()
    out["stat_col"] = stat_col
    out["l5_hits"] = l5_hits
    out["l5_n"] = l5_n
    out["l10_hits"] = l10_hits
    out["l10_n"] = l10_n
    out["l20_hits"] = l20_hits
    out["l20_n"] = l20_n
    out["l5_avg"] = l5_avg
    out["l10_avg"] = l10_avg
    out["gap"] = gap
    out["gap_sd"] = gap_sd
    out["season_hits"] = season_hits_a
    out["season_n"] = season_n_a
    out["season_hit_rate"] = season_hr
    out["minutes_l5_avg"] = minutes_l5
    out["starter_l5_rate"] = starter_l5
    out.attrs["missing_prop_map"] = missing_prop
    return out


def write_readme(path: Path, df: pd.DataFrame, notes: list[str]) -> None:
    lines = [
        "# NBA master graded props",
        "",
        f"Built: {datetime.now(ET).isoformat(timespec='seconds')}",
        f"File: `{path.name}`",
        "",
        "## Row identity",
        "One row per `(player, prop, line, pick_type, side, game_date)`.",
        "Goblin/Demon: OVER only. Standard: OVER and UNDER.",
        "",
        "## Leakage rule",
        "All L5/L10/L20/season/minutes/starter features use only box scores with `game_date` **strictly before** the prop's `game_date`.",
        "",
        "## Columns",
        "| column | meaning |",
        "|---|---|",
        "| game_date | slate / game date (ET) |",
        "| player, team, opp_team | board identity |",
        "| prop, pick_type, side, line | market face |",
        "| projection_id | PrizePicks projection id (one Goblin/Demon rung) |",
        "| first_line, last_line, n_fetches, line_move | line_history joined on projection_id only |",
        "| actual, hit, push, void, void_reason, result | grade |",
        "| l5_hits / l5_n (and L10/L20) | directional clears of **this** line in last N decided games |",
        "| l5_avg, l10_avg, gap, gap_sd | mean vs line; gap = l5_avg - line; gap_sd = sd(values - line) on L5 |",
        "| season_hits, season_n, season_hit_rate | same-season games before prop day |",
        "| minutes_l5_avg, starter_l5_rate | role proxies |",
        "| spread, total, favorite, home_away | from game_lines.db (Favorite derived from home spread) |",
        "| opp_team | board opp, else backfilled from box home/away |",
        "| opp_def_rank, opp_def_tier, opp_pts_allowed_l10 | day-as-of L10 pts allowed; rank 1–30 NBA only |",
        "| team_pace_l10, pace_flag | day-as-of L10 pace proxy; Fast/Avg/Slow vs same-day tertiles |",
        "| line_moved | 1 if abs(line_move)>0 (null line_move → 0) |",
        "| source, season_tag | provenance |",
        "",
        "## Counts",
    ]
    lines.append(f"- total rows: **{len(df):,}**")
    lines.append("")
    lines.append("### by season_tag × pick_type")
    ct = df.groupby(["season_tag", "pick_type"], dropna=False).size().reset_index(name="n")
    for r in ct.itertuples(index=False):
        lines.append(f"- {r.season_tag} / {r.pick_type}: {r.n:,}")
    lines.append("")
    lines.append("### by side")
    for side, n in df["side"].value_counts().items():
        lines.append(f"- {side}: {n:,}")
    lines.append("")
    lines.append("## Notes / missing features")
    for n in notes:
        lines.append(f"- {n}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=date.today().isoformat(), help="Stamp in output filename")
    args = ap.parse_args()
    asof = str(args.asof)[:10]

    export_path = REPO / "data" / "training" / "graded_export_20260516.csv"
    ref_db = REPO / "data" / "cache" / "proporacle_ref.db"
    lh_db = REPO / "data" / "line_history.db"
    gl_db = REPO / "data" / "cache" / "game_lines.db"
    outputs = REPO / "outputs"

    print("[1/6] loading boxes…", flush=True)
    boxes = load_boxes(ref_db)
    boxes_idx = index_boxes(boxes)
    print(f"  boxes={len(boxes):,} players={len(boxes_idx):,}", flush=True)

    print("[2/6] graded export (last season)…", flush=True)
    exp = load_export_nba(export_path)
    print(f"  export NBA rows={len(exp):,}", flush=True)

    print("[3/6] grading this-season step1 boards…", flush=True)
    season = load_season_boards(outputs, boxes_idx)
    print(f"  season graded rows={len(season):,}", flush=True)

    base = pd.concat([exp, season], ignore_index=True)
    # Dedup identity
    base = base.drop_duplicates(
        ["player", "prop", "line", "pick_type", "side", "game_date"], keep="last"
    )
    print(f"  combined unique={len(base):,}", flush=True)

    print("[4/6] features (no leakage)…", flush=True)
    feat = attach_features(base, boxes_idx)
    missing_prop = int(feat.attrs.get("missing_prop_map", 0))

    print("[5/6] spreads + day-as-of opp D/pace + line moves…", flush=True)
    print("  building team-game rolling context…", flush=True)
    team_games = build_team_games(boxes)
    team_idx = index_team_games(team_games)
    feat = attach_opp_context(feat, boxes, team_idx)
    feat = attach_spreads(feat, gl_db)
    feat = attach_line_moves(feat, lh_db)
    feat["line_moved"] = (pd.to_numeric(feat["line_move"], errors="coerce").abs() > 0).astype(int)

    notes = [
        "NBA-only first pass.",
        "pace_flag = Fast/Avg/Slow from team L10 pace_proxy (team_pts+opp_pts) vs same-day tertiles; games before prop day only.",
        "opp_def_rank / opp_pts_allowed_l10 = day-as-of L10 points allowed by opponent before prop day (1=stingiest, max 30 NBA teams).",
        "Team-game pairing is (game_date, home, away); exhibition/foreign codes excluded from ranks.",
        "opp_team backfilled from box home/away when board opp was blank/dash.",
        "line_move first/last are per projection_id (one Goblin/Demon rung); never across rungs. Unmatched → null.",
        "line_history exists mainly for 2026-27; export-era dates usually have null line_move (no PP fetch history).",
        f"props with no box-stat map skipped in feature windows: {missing_prop} rows still kept with null L-windows.",
        "Injury board status not attached this pass (void/DNP from missing minutes only on season boards).",
        "Static nba_defense_by_stat.csv is no longer used for opp_def_rank.",
    ]

    out_parquet = REPO / "data" / "training" / f"master_graded_{asof}.parquet"
    out_readme = REPO / "data" / "training" / f"master_graded_{asof}_README.md"
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    print(f"[6/6] writing {out_parquet}…")
    feat.to_parquet(out_parquet, index=False)
    write_readme(out_readme, feat, notes)

    print("\n=== DONE ===")
    print(out_parquet)
    print(out_readme)
    print(feat.groupby(["season_tag", "pick_type"]).size())
    print("hit rate (non-void decided):")
    decided = feat[feat["void"].fillna(0).eq(0) & feat["push"].fillna(0).eq(0) & feat["hit"].notna()]
    print(decided.groupby("pick_type")["hit"].agg(["count", "mean"]).round(4))

    n = len(feat)
    proj_ok = feat["projection_id"].astype(str).str.len().gt(0) & ~feat["projection_id"].isin(["nan", "None"])
    lm_ok = feat["line_move"].notna()
    def_ok = feat["opp_def_rank"].notna()
    pace_ok = feat["pace_flag"].astype(str).str.len().gt(0)
    print("\n=== FILL RATES ===")
    print(f"  projection_id non-empty: {int(proj_ok.sum()):,}/{n:,} ({100*proj_ok.mean():.1f}%)")
    print(f"  line_move matched:       {int(lm_ok.sum()):,}/{n:,} ({100*lm_ok.mean():.1f}%)")
    print(f"  opp_def_rank filled:     {int(def_ok.sum()):,}/{n:,} ({100*def_ok.mean():.1f}%)")
    print(f"  pace_flag filled:        {int(pace_ok.sum()):,}/{n:,} ({100*pace_ok.mean():.1f}%)")
    print(f"  opp_def_rank min/max:    {feat['opp_def_rank'].min()} / {feat['opp_def_rank'].max()}")
    # Sanity: no cross-rung first/last for Goblin when matched
    g70 = feat[(feat["pick_type"] == "Goblin") & lm_ok].copy()
    if not g70.empty:
        bad = g70[
            (g70["first_line"] - g70["line"]).abs() > 5
        ]
        print(f"  Goblin matched with |first_line-line|>5: {len(bad):,} (should be rare within-rung moves)")
        print(
            "  Goblin line_move describe:\n"
            + g70["line_move"].describe().to_string()
        )
    # Season-only fill (where history exists)
    seas = feat[feat["season_tag"] == "2026-27"]
    if not seas.empty:
        print(
            f"  2026-27 line_move fill: {int(seas['line_move'].notna().sum()):,}/{len(seas):,} "
            f"({100*seas['line_move'].notna().mean():.1f}%)"
        )
        print(
            f"  2026-27 opp_def_rank fill: {int(seas['opp_def_rank'].notna().sum()):,}/{len(seas):,} "
            f"({100*seas['opp_def_rank'].notna().mean():.1f}%)"
        )


if __name__ == "__main__":
    main()
