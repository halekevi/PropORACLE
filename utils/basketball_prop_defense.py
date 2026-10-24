"""Shared basketball prop-by-stat opponent defense (WNBA / NBA / CBB).

Ranks: 1 = stingiest (lowest opp-allowed) for that category.
Tiers: canonical Elite → Weak via ``utils.defense_tiers.def_tier_from_overall_rank``.

CBB: box-score table is often empty offseason — keep overall ``def_tier``
(already 5-bucket) and treat by-stat CSV as optional when present.
"""
from __future__ import annotations

import sqlite3
from functools import lru_cache
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from utils.defense_tiers import def_tier_from_overall_rank, normalize_def_tier_label

# Prop label -> category prefix (rank col = f"{cat}_rank", tier col = f"{cat}_tier")
PROP_TO_CAT: dict[str, str] = {
    # WNBA / shared
    "Points": "pts",
    "Points (Combo)": "pts",
    "Rebounds": "reb",
    "Rebounds (Combo)": "reb",
    "Assists": "ast",
    "Assists (Combo)": "ast",
    "3-PT Made": "fg3m",
    "3-PT Made (Combo)": "fg3m",
    "3-PT Attempted": "fg3a",
    "Steals": "stl",
    "Blocked Shots": "blk",
    "Blks+Stls": "bs",
    "Pts+Rebs+Asts": "pra",
    "Pts+Rebs": "pr",
    "Pts+Asts": "pa",
    "Rebs+Asts": "ra",
    "FG Made": "fgm",
    "FG Attempted": "fga",
    "Two Pointers Made": "fg2m",
    "Two Pointers Attempted": "fg2a",
    "Free Throws Made": "ftm",
    "Free Throws Attempted": "fta",
    "Turnovers": "tov",
    "Defensive Rebounds": "dreb",
    "Offensive Rebounds": "oreb",
    # NBA PrizePicks variants (same cats, alternate labels)
    "Blocked Shots": "blk",
    "Blks+Stls": "bs",
}

_STAT_COLS = [
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
]

_COMBO_COLS = ["pra", "pr", "pa", "ra", "bs"]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_db_path() -> Path:
    return repo_root() / "data" / "cache" / "proporacle_ref.db"


def prop_category(prop: object) -> str:
    p = str(prop or "").strip()
    if p in PROP_TO_CAT:
        return PROP_TO_CAT[p]
    key = " ".join(p.replace("-", " ").split())
    for label, cat in PROP_TO_CAT.items():
        if label.lower() == key.lower():
            return cat
    return ""


def tier_label_from_rank(rank: object, n_teams: int) -> str:
    """Five-bucket Elite→Weak; rank 1 = stingiest."""
    return def_tier_from_overall_rank(rank, n_teams, na_label="Avg")


def coarse_bucket_from_tier(tier: object) -> str:
    """
    Map 5-bucket (or legacy HARD/EASY) → HARD/MID/EASY/UNK for soft nudges.

    HARD ≈ Elite|Above Avg (stingy); EASY ≈ Weak|Below Avg (generous).
    """
    canon = normalize_def_tier_label(tier)
    if canon in ("Elite", "Above Avg"):
        return "HARD"
    if canon in ("Weak", "Below Avg"):
        return "EASY"
    if canon == "Avg":
        return "MID"
    t = str(tier or "").strip().upper().replace(" ", "_")
    if t in ("HARD", "HARD_MID"):
        return "HARD"
    if t in ("EASY", "EASY_MID"):
        return "EASY"
    if t in ("MID", "AVG"):
        return "MID"
    return "UNK"


def coarse_bucket_from_rank(rank: object, n_teams: int) -> str:
    try:
        r = float(rank)
        n = max(int(n_teams), 1)
    except (TypeError, ValueError):
        return "UNK"
    if r != r:
        return "UNK"
    return coarse_bucket_from_tier(tier_label_from_rank(r, n))


def rebuild_defense_by_stat(
    *,
    sport_table: str,
    out_path: Path,
    db_path: Optional[Path] = None,
    team_map: Optional[dict[str, str]] = None,
    exclude_teams: Optional[set[str]] = None,
    overall_rank_hook: Optional[Callable[[pd.DataFrame, Path], pd.Series]] = None,
) -> pd.DataFrame:
    """Build opp-allowed by-stat ranks/tiers from box scores; write CSV; return frame."""
    db_path = Path(db_path or default_db_path())
    out_path = Path(out_path)
    team_map = team_map or {}
    exclude_teams = {t.upper() for t in (exclude_teams or set())}
    if not db_path.is_file():
        return pd.DataFrame()

    con = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query(
            f"""
            SELECT event_id, game_date, team, pts, reb, ast, stl, blk, tov,
                   fgm, fga, fg3m, fg3a, fg2m, fg2a, ftm, fta, oreb, dreb
            FROM {sport_table}
            WHERE team IS NOT NULL AND team != ''
              AND event_id IS NOT NULL
            """,
            con,
        )
    finally:
        con.close()

    if df.empty:
        return df

    df = df.copy()
    df["team"] = (
        df["team"]
        .astype(str)
        .str.strip()
        .str.upper()
        .map(lambda t: team_map.get(t, t))
    )
    if exclude_teams:
        df = df[~df["team"].isin(exclude_teams)].copy()

    for c in _STAT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    gcols = ["event_id", "game_date", "team"] + [c for c in _STAT_COLS if c in df.columns]
    team_game = df[gcols].groupby(["event_id", "game_date", "team"], as_index=False).sum(
        numeric_only=True
    )
    team_game["pra"] = team_game["pts"] + team_game["reb"] + team_game["ast"]
    team_game["pr"] = team_game["pts"] + team_game["reb"]
    team_game["pa"] = team_game["pts"] + team_game["ast"]
    team_game["ra"] = team_game["reb"] + team_game["ast"]
    team_game["bs"] = team_game["stl"] + team_game["blk"]

    allowed_rows: list[dict] = []
    metric_names = [c for c in (_STAT_COLS + _COMBO_COLS) if c in team_game.columns]
    for _eid, grp in team_game.groupby("event_id"):
        teams = grp["team"].tolist()
        if len(teams) != 2:
            continue
        a, b = teams[0], teams[1]
        ra = grp[grp["team"] == a].iloc[0]
        rb = grp[grp["team"] == b].iloc[0]
        row_a: dict = {"team": a, "game_date": ra["game_date"]}
        row_b: dict = {"team": b, "game_date": rb["game_date"]}
        for c in metric_names:
            row_a[f"opp_{c}"] = float(rb[c])
            row_b[f"opp_{c}"] = float(ra[c])
        allowed_rows.extend([row_a, row_b])

    if not allowed_rows:
        return pd.DataFrame()

    allowed = pd.DataFrame(allowed_rows)
    metrics = [c for c in allowed.columns if c.startswith("opp_")]
    summary = allowed.groupby("team", as_index=False).agg(
        **{m: (m, "mean") for m in metrics},
        games=("game_date", "nunique"),
    )
    n_teams = len(summary)
    for m in metrics:
        cat = m.replace("opp_", "")
        summary[f"{cat}_rank"] = summary[m].rank(method="min", ascending=True).astype(int)
        summary[f"{cat}_tier"] = summary[f"{cat}_rank"].map(
            lambda r: tier_label_from_rank(float(r), n_teams)
        )

    summary["overall_rank"] = summary["pts_rank"]
    if overall_rank_hook is not None:
        try:
            override = overall_rank_hook(summary, db_path)
            if override is not None and len(override) == len(summary):
                summary["overall_rank"] = override
        except Exception:
            pass
    summary["n_teams"] = n_teams
    out_path.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_path, index=False)
    clear_defense_table_cache()
    return summary


def remap_legacy_tier_columns(csv_path: Path) -> pd.DataFrame:
    """In-place HARD/EASY* → Elite→Weak on *_tier columns (no rebuild)."""
    path = Path(csv_path)
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path, encoding="utf-8-sig")
    legacy = {
        "HARD": "Elite",
        "HARD_MID": "Above Avg",
        "MID": "Avg",
        "EASY_MID": "Below Avg",
        "EASY": "Weak",
    }
    changed = False
    for col in df.columns:
        if not str(col).endswith("_tier"):
            continue
        before = df[col].astype(str)
        mapped = before.map(
            lambda v: legacy.get(str(v).strip().upper().replace(" ", "_"))
            or normalize_def_tier_label(v)
            or str(v).strip()
        )
        if not mapped.equals(before):
            df[col] = mapped
            changed = True
    if changed:
        df.to_csv(path, index=False)
        clear_defense_table_cache()
    return df


@lru_cache(maxsize=8)
def load_defense_table(csv_path: str) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.is_file() or path.stat().st_size < 50:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if "team" not in df.columns:
        return pd.DataFrame()
    return df.copy()


def clear_defense_table_cache() -> None:
    load_defense_table.cache_clear()


def lookup_stat_defense(
    opp: object,
    prop: object,
    *,
    csv_path: str,
    canon_team_fn: Callable[[object], str],
) -> dict:
    """
    Return category/rank/tier/coarse for opponent × prop.

    ``stat_def_tier`` is the canonical 5-bucket label (Elite→Weak).
    ``stat_def_coarse`` is HARD/MID/EASY for soft-priority nudges.
    """
    cat = prop_category(prop)  # noqa: alias — prop_category()
    team = canon_team_fn(opp)
    empty = {
        "stat_def_category": cat or "",
        "stat_def_rank": None,
        "stat_def_tier": "",
        "stat_def_coarse": "UNK",
        "stat_def_tier_raw": "",
    }
    if not cat or not team:
        return empty
    df = load_defense_table(csv_path)
    if df.empty:
        return empty
    work = df.copy()
    work["team"] = work["team"].astype(str).str.strip().str.upper().map(canon_team_fn)
    sub = work[work["team"] == team]
    if sub.empty:
        return empty
    row = sub.iloc[0]
    rank_col = f"{cat}_rank"
    tier_col = f"{cat}_tier"
    rank = None
    if rank_col in row.index and pd.notna(row[rank_col]):
        try:
            rank = int(float(row[rank_col]))
        except (TypeError, ValueError):
            rank = None
    if rank is None and "overall_rank" in row.index and pd.notna(row["overall_rank"]):
        try:
            rank = int(float(row["overall_rank"]))
        except (TypeError, ValueError):
            rank = None
    used_category = rank is not None and rank_col in row.index and pd.notna(row.get(rank_col))
    tier_raw = str(row[tier_col]) if tier_col in row.index and pd.notna(row.get(tier_col)) else ""
    n_teams = (
        int(row["n_teams"])
        if "n_teams" in row.index and pd.notna(row.get("n_teams"))
        else len(work)
    )
    if rank is None:
        tier = ""
        coarse = "UNK"
    elif used_category and tier_raw:
        tier = normalize_def_tier_label(tier_raw) or tier_label_from_rank(rank, n_teams)
        coarse = coarse_bucket_from_tier(tier)
    else:
        tier = tier_label_from_rank(rank, n_teams)
        coarse = coarse_bucket_from_tier(tier)
    return {
        "stat_def_category": cat,
        "stat_def_rank": rank,
        "stat_def_tier": tier,
        "stat_def_coarse": coarse,
        "stat_def_tier_raw": tier_raw,
    }


def attach_stat_defense_columns(
    df: pd.DataFrame,
    *,
    sport: str,
    csv_path: str,
    canon_team_fn: Callable[[object], str],
    sport_values: Optional[set[str]] = None,
) -> pd.DataFrame:
    """Add stat_def_category / stat_def_rank / stat_def_tier for matching sport rows."""
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    sport_u = str(sport or "").strip().upper()
    allowed = {s.upper() for s in (sport_values or {sport_u})}

    sport_col = next((c for c in ("sport", "Sport") if c in out.columns), None)
    if sport_col is None:
        is_sport = pd.Series(True, index=out.index)
    else:
        is_sport = out[sport_col].astype(str).str.upper().str.strip().isin(allowed)

    opp_col = next((c for c in ("opp", "opp_team") if c in out.columns), None)
    prop_col = next((c for c in ("prop_type", "prop", "Prop") if c in out.columns), None)
    if not opp_col or not prop_col:
        return out

    cats: list[str] = []
    ranks: list[object] = []
    tiers: list[str] = []
    for i, row in out.iterrows():
        if not bool(is_sport.loc[i]):
            cats.append("")
            ranks.append(None)
            tiers.append("")
            continue
        info = lookup_stat_defense(
            row.get(opp_col),
            row.get(prop_col),
            csv_path=csv_path,
            canon_team_fn=canon_team_fn,
        )
        cats.append(info.get("stat_def_category") or "")
        ranks.append(info.get("stat_def_rank"))
        tiers.append(info.get("stat_def_tier") or "")

    out["stat_def_category"] = cats
    out["stat_def_rank"] = ranks
    out["stat_def_tier"] = tiers
    return out
