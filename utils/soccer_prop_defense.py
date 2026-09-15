"""Soccer prop-specific opponent defense ranks.
Prefers Sports/Soccer/soccer_defense_summary.csv (or defense_db) when present;
falls back to OVERALL_DEF_RANK / OPP_PPG from step3.
Maps Shots/Goals/Saves onto overall / shots / saves ranks.
Goals → category \"overall\" → rank column overall_rank (lookup uses f\"{cat}_rank\").
"""
from __future__ import annotations
import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional
import pandas as pd
from utils.prop_defense_common import (
    attach_lookup_columns,
    coarse_bucket_from_rank,
    empty_stat_def,
)
from utils.defense_tiers import def_tier_from_overall_rank, normalize_def_tier_label

PROP_TO_CAT: dict[str, str] = {
    "Goals": "overall",
    "goals": "overall",
    "Goal + Assist": "overall",
    "goal_assist": "overall",
    "Shots": "shots",
    "shots": "shots",
    "Shots On Target": "shots",
    "shots_on_target": "shots",
    "Shots Assisted": "shots",
    "Goalie Saves": "saves",
    "goalie_saves": "saves",
    "Saves": "saves",
    "Assists": "overall",
    "assists": "overall",
    "Fouls": "overall",
    "fouls": "overall",
    "Tackles": "overall",
    "tackles": "overall",
    "Passes Attempted": "overall",
    "passes_attempted": "overall",
    "Clearances": "overall",
    "Crosses": "overall",
    "Attempted Dribbles": "overall",
}
# PrizePicks short names / step8 Opp variants → defense pp_name keys
TEAM_ALIASES: dict[str, str] = {
    "WHITECAPS": "VANCOUVER WHITECAPS",
    "VANCOUVER": "VANCOUVER WHITECAPS",
    "SALT LAKE": "REAL SALT LAKE",
    "RSL": "REAL SALT LAKE",
    "SOUNDERS": "SEATTLE",
    "GALAXY": "LA GALAXY",
    "LAFC": "LOS ANGELES FC",
    "INTER MIAMI": "INTER MIAMI",
    "NYCFC": "NEW YORK CITY FC",
    "RED BULLS": "RED BULL NEW YORK",
    "NEW YORK": "RED BULL NEW YORK",
    "ATLÉTICO": "ATLÉTICO MADRID",
    "ATLETICO": "ATLETICO",
    "JUAREZ": "JUÁREZ",
    "FENERBAHCE": "FENERBAHÇE",
    "UNIV CATOLICA": "UNIV CATÓLICA",
    "IND DEL VALLE": "IND. DEL VALLE",
    "INDEPENDIENTE DEL VALLE": "IND. DEL VALLE",
    "LANUS": "LANÚS",
    "TURKIYE": "TÜRKIYE",
    "N IRELAND": "N. IRELAND",
    "N MACEDONIA": "N. MACEDONIA",
    "REP IRELAND": "REP. IRELAND",
    "KC": "KC CURRENT",
    "CURRENT": "KC CURRENT",
    "ORLANDO PRIDE": "PRIDE",
    "OL REIGN": "REIGN",
    "SEATTLE REIGN": "REIGN",
    "PORTLAND THORNS": "THORNS",
    "WASHINGTON SPIRIT": "SPIRIT",
    "SD WAVE": "SAN DIEGO WAVE",
    "WAVE": "SAN DIEGO WAVE",
    "CHICAGO": "CHICAGO STARS",
}

def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]

def default_csv_path() -> Path:
    return _repo_root() / "Sports" / "Soccer" / "data" / "soccer_defense_by_stat.csv"

def _summary_candidates() -> list[Path]:
    root = _repo_root()
    return [
        root / "Sports" / "Soccer" / "soccer_defense_summary.csv",
        root / "Sports" / "Soccer" / "data" / "soccer_defense_summary.csv",
        root / "Sports" / "Soccer" / "scripts" / "soccer_defense_summary.csv",
    ]

def prop_category(prop: object) -> str:
    p = str(prop or "").strip()
    if p in PROP_TO_CAT:
        return PROP_TO_CAT[p]
    key = " ".join(p.replace("_", " ").split())
    for label, cat in PROP_TO_CAT.items():
        if label.lower() == key.lower():
            return cat
    return ""

def _normalize_team(opp: object) -> str:
    team = str(opp or "").strip().upper()
    if not team or team in {"NAN", "NONE", "UNKNOWN", "UNKNOWN_OPP"}:
        return ""
    return TEAM_ALIASES.get(team, team)

def _tier_label(rank: float, n_teams: int) -> str:
    q = max(n_teams / 5.0, 1.0)
    if rank <= q:
        return "HARD"
    if rank <= 2 * q:
        return "HARD_MID"
    if rank <= 3 * q:
        return "MID"
    if rank <= 4 * q:
        return "EASY_MID"
    return "EASY"

def _series_usable(s: pd.Series) -> bool:
    if s is None or getattr(s, "empty", True):
        return False
    num = pd.to_numeric(s, errors="coerce")
    valid = num.dropna()
    if valid.empty:
        return False
    if float(valid.nunique()) <= 1 and float(valid.iloc[0]) == 0.0:
        return False
    return True

def _load_summary_frame() -> pd.DataFrame:
    for path in _summary_candidates():
        if path.is_file() and path.stat().st_size > 50:
            try:
                return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
            except Exception:
                continue
    try:
        import sys
        scripts = _repo_root() / "scripts"
        if str(scripts) not in sys.path:
            sys.path.insert(0, str(scripts))
        from defense_db import load_defense_from_db  # type: ignore
        db = load_defense_from_db("soccer")
        if isinstance(db, pd.DataFrame) and not db.empty:
            return db
    except Exception:
        pass
    return pd.DataFrame()

def _rebuild_from_summary(summary: pd.DataFrame, out_path: Path) -> pd.DataFrame:
    work = summary.copy()
    team_col = next(
        (c for c in ("pp_name", "team", "TEAM_ABBREVIATION", "TEAM_NAME", "team_name") if c in work.columns),
        None,
    )
    if not team_col:
        return pd.DataFrame()
    work["team"] = work[team_col].map(_normalize_team)
    work = work[work["team"].ne("")]
    rank_src = next(
        (c for c in ("OVERALL_DEF_RANK", "overall_rank", "def_rank") if c in work.columns),
        None,
    )
    if rank_src:
        work["overall_rank"] = pd.to_numeric(work[rank_src], errors="coerce")
    else:
        work["overall_rank"] = pd.NA
    ppg_src = next(
        (c for c in ("goals_conceded_pg", "OPP_PPG", "opp_ppg", "opp_gaa") if c in work.columns),
        None,
    )
    if ppg_src:
        work["opp_ppg"] = pd.to_numeric(work[ppg_src], errors="coerce")
    shots_src = next(
        (c for c in ("shots_conceded_pg", "opp_saa") if c in work.columns),
        None,
    )
    if shots_src:
        work["opp_saa"] = pd.to_numeric(work[shots_src], errors="coerce")
    # If ranks missing, derive from goals conceded/game (lower = stingier = rank 1)
    if work["overall_rank"].isna().all() and "opp_ppg" in work.columns and _series_usable(work["opp_ppg"]):
        work["overall_rank"] = work["opp_ppg"].rank(method="min", ascending=True)
    aggs: dict = {"overall_rank": ("overall_rank", "min")}
    if "opp_ppg" in work.columns:
        aggs["opp_ppg"] = ("opp_ppg", "mean")
    if "opp_saa" in work.columns:
        aggs["opp_saa"] = ("opp_saa", "mean")
    agg = work.groupby("team", as_index=False).agg(**aggs)
    n = len(agg)
    if "opp_saa" in agg.columns and _series_usable(agg["opp_saa"]):
        agg["shots_rank"] = agg["opp_saa"].rank(method="min", ascending=True)
    else:
        # CRITICAL: do not call .rank() on an all-NaN opp_saa column — that yields NaN ranks.
        agg["shots_rank"] = agg["overall_rank"]
    agg["saves_rank"] = agg["shots_rank"]
    for cat, col in (("overall", "overall_rank"), ("shots", "shots_rank"), ("saves", "saves_rank")):
        agg[f"{cat}_tier"] = agg[col].map(lambda r: _tier_label(float(r), n) if pd.notna(r) else "")
    agg["n_teams"] = n
    # Drop teams with no usable rank so lookups don't match empty rows
    agg = agg[agg["overall_rank"].notna() | agg["shots_rank"].notna()].copy()
    if agg.empty:
        return pd.DataFrame()
    agg["n_teams"] = len(agg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(out_path, index=False)
    clear_defense_cache()
    return agg

def _finalize_soccer_agg(agg: pd.DataFrame, out_path: Path) -> pd.DataFrame:
    if agg.empty:
        return agg
    n = len(agg)
    if "overall_rank" not in agg.columns or agg["overall_rank"].isna().all():
        if "opp_ppg" in agg.columns and _series_usable(agg["opp_ppg"]):
            agg["overall_rank"] = agg["opp_ppg"].rank(method="min", ascending=True)
        elif "opp_gaa" in agg.columns and _series_usable(agg["opp_gaa"]):
            agg["overall_rank"] = agg["opp_gaa"].rank(method="min", ascending=True)
        else:
            agg["overall_rank"] = pd.NA
    # Bug fix: column presence is not enough — empty opp_saa.rank() → all NaN shots_rank.
    if "opp_saa" in agg.columns and _series_usable(agg["opp_saa"]):
        agg["shots_rank"] = agg["opp_saa"].rank(method="min", ascending=True)
    else:
        agg["shots_rank"] = agg["overall_rank"]
    agg["saves_rank"] = agg["shots_rank"]
    for cat, col in (("overall", "overall_rank"), ("shots", "shots_rank"), ("saves", "saves_rank")):
        agg[f"{cat}_tier"] = agg[col].map(lambda r: _tier_label(float(r), n) if pd.notna(r) else "")
    ranked = agg["overall_rank"].notna() | agg["shots_rank"].notna()
    agg = agg.loc[ranked].copy()
    if not agg.empty:
        agg["n_teams"] = len(agg)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(out_path, index=False)
    clear_defense_cache()
    return agg


def _harvest_step8_opp_ranks() -> pd.DataFrame:
    """Pull Opp + Def Rank from step8 board when ESPN summary is unavailable."""
    root = _repo_root() / "Sports" / "Soccer"
    candidates = [
        root / "step8_soccer_direction_clean.xlsx",
        root / "step8_soccer_direction_clean.csv",
    ]
    frames: list[pd.DataFrame] = []
    for path in candidates:
        if not path.is_file():
            continue
        try:
            if path.suffix.lower() == ".xlsx":
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
        except Exception:
            continue
        opp_col = next((c for c in ("Opp", "opp", "opp_team") if c in df.columns), None)
        rank_col = next(
            (c for c in ("Def Rank", "def_rank", "OVERALL_DEF_RANK", "opponent_def_rank") if c in df.columns),
            None,
        )
        if not opp_col or not rank_col:
            continue
        part = pd.DataFrame(
            {
                "team": df[opp_col].map(_normalize_team),
                "overall_rank": pd.to_numeric(df[rank_col], errors="coerce"),
            }
        )
        part = part[part["team"].ne("") & part["overall_rank"].notna()]
        if not part.empty:
            frames.append(part)
    if not frames:
        return pd.DataFrame()
    all_rows = pd.concat(frames, ignore_index=True)
    return all_rows.groupby("team", as_index=False).agg(overall_rank=("overall_rank", "min"))


def rebuild_defense_by_stat(out_path: Optional[Path] = None) -> pd.DataFrame:
    out_path = Path(out_path or default_csv_path())
    summary = _load_summary_frame()
    if not summary.empty:
        built = _rebuild_from_summary(summary, out_path)
        if not built.empty:
            return built

    pieces: list[pd.DataFrame] = []
    src = _repo_root() / "Sports" / "Soccer" / "step3_soccer_with_defense.csv"
    if src.is_file():
        df = pd.read_csv(src, encoding="utf-8-sig", low_memory=False)
        team_col = "opp_team" if "opp_team" in df.columns else ("opp" if "opp" in df.columns else None)
        if team_col:
            work = pd.DataFrame({"team": df[team_col].map(_normalize_team)})
            for c in (
                "OVERALL_DEF_RANK",
                "def_rank",
                "OPP_PPG",
                "opp_saa",
                "opp_gaa",
                "goals_conceded_pg",
                "shots_conceded_pg",
            ):
                if c in df.columns:
                    work[c] = pd.to_numeric(df[c], errors="coerce")
            work = work[work["team"].ne("")]
            aggs: dict = {}
            if "OVERALL_DEF_RANK" in work.columns:
                aggs["overall_rank"] = ("OVERALL_DEF_RANK", "min")
            elif "def_rank" in work.columns:
                aggs["overall_rank"] = ("def_rank", "min")
            if "OPP_PPG" in work.columns:
                aggs["opp_ppg"] = ("OPP_PPG", "mean")
            elif "goals_conceded_pg" in work.columns:
                aggs["opp_ppg"] = ("goals_conceded_pg", "mean")
            if "opp_saa" in work.columns:
                aggs["opp_saa"] = ("opp_saa", "mean")
            elif "shots_conceded_pg" in work.columns:
                aggs["opp_saa"] = ("shots_conceded_pg", "mean")
            if "opp_gaa" in work.columns:
                aggs["opp_gaa"] = ("opp_gaa", "mean")
            if aggs:
                pieces.append(work.groupby("team", as_index=False).agg(**aggs))

    step8 = _harvest_step8_opp_ranks()
    if not step8.empty:
        pieces.append(step8)

    if not pieces:
        return pd.DataFrame()

    merged = pieces[0]
    for extra in pieces[1:]:
        merged = merged.merge(extra, on="team", how="outer", suffixes=("", "_s8"))
        if "overall_rank_s8" in merged.columns:
            merged["overall_rank"] = merged["overall_rank"].combine_first(merged["overall_rank_s8"])
            merged = merged.drop(columns=["overall_rank_s8"])
        # Any other *_s8 numeric cols: prefer original then step8
        for c in list(merged.columns):
            if c.endswith("_s8"):
                base = c[:-3]
                if base in merged.columns:
                    merged[base] = merged[base].combine_first(merged[c])
                else:
                    merged[base] = merged[c]
                merged = merged.drop(columns=[c])

    return _finalize_soccer_agg(merged, out_path)

@lru_cache(maxsize=4)

def load_defense_table(csv_path: str = "") -> pd.DataFrame:
    path = Path(csv_path) if csv_path else default_csv_path()
    if not path.is_file() or path.stat().st_size < 50:
        rebuild_defense_by_stat(out_path=path)
    if not path.is_file():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if "team" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["team"] = df["team"].astype(str).str.strip().str.upper()
    # Stale CSV: overall ranks exist but shots/saves are all null → rebuild once
    if (
        not csv_path
        and "overall_rank" in df.columns
        and df["overall_rank"].notna().any()
        and (
            ("shots_rank" in df.columns and df["shots_rank"].isna().all())
            or ("saves_rank" in df.columns and df["saves_rank"].isna().all())
            or "overall_rank" not in df.columns
        )
    ):
        rebuilt = rebuild_defense_by_stat(out_path=path)
        if not rebuilt.empty:
            return rebuilt
    # Legacy misnamed column: "overall" instead of "overall_rank"
    if "overall_rank" not in df.columns and "overall" in df.columns:
        df["overall_rank"] = pd.to_numeric(df["overall"], errors="coerce")
    return df

def clear_defense_cache() -> None:
    load_defense_table.cache_clear()

def lookup_stat_defense(opp: object, prop: object, *, csv_path: str = "") -> dict:
    cat = prop_category(prop)
    team = _normalize_team(opp)
    empty = empty_stat_def(cat)
    if not cat or not team:
        return empty
    df = load_defense_table(csv_path)
    if df.empty:
        return empty
    sub = df[df["team"] == team]
    if sub.empty:
        # soft alias retry already applied; try raw upper without alias reverse
        return empty
    row = sub.iloc[0]
    # Goals→overall must resolve overall_rank (not a bare "overall" col)
    rank_col = f"{cat}_rank"
    rank = None
    candidates = [rank_col]
    if cat == "overall":
        candidates.append("overall")  # legacy misname
    if cat in ("shots", "saves"):
        candidates.extend(["overall_rank", "overall"])
    for col in candidates:
        if col in row.index and pd.notna(row[col]):
            try:
                rank = int(float(row[col]))
                break
            except (TypeError, ValueError):
                continue
    n_teams = int(row["n_teams"]) if "n_teams" in row.index and pd.notna(row.get("n_teams")) else len(df)
    coarse = coarse_bucket_from_rank(rank, n_teams) if rank is not None else "UNK"
    return {
        "stat_def_category": cat,
        "stat_def_rank": rank,
        "stat_def_tier": coarse,
        "stat_def_coarse": coarse,
    }

def suggested_standard_direction(
    *,
    opp: object,
    prop: object,
    edge: object = None,
    l5_over: object = None,
    l5_under: object = None,
) -> str:
    """Standard O/U from that prop's allowed-stat rank, then L5, then edge.

    Goblins stay OVER-only at the caller. Weak/Below Avg → OVER, Elite/Above Avg → UNDER.
    """
    from utils.matchup_edge.stat_defense import display_tier_from_stat

    lu = lookup_stat_defense(opp, prop)
    tier = str(display_tier_from_stat(lu.get("stat_def_tier")) or "").strip().upper()
    if tier in ("WEAK", "BELOW AVG"):
        return "OVER"
    if tier in ("ELITE", "ABOVE AVG"):
        return "UNDER"
    lo = pd.to_numeric(pd.Series([l5_over]), errors="coerce").iloc[0]
    lu5 = pd.to_numeric(pd.Series([l5_under]), errors="coerce").iloc[0]
    if pd.notna(lo) and pd.notna(lu5):
        if float(lu5) >= 4 and float(lu5) > float(lo):
            return "UNDER"
        if float(lo) >= 4:
            return "OVER"
    ev = pd.to_numeric(pd.Series([edge]), errors="coerce").iloc[0]
    if pd.notna(ev) and float(ev) < 0:
        return "UNDER"
    return "OVER"


def attach_stat_defense_columns(df: pd.DataFrame, *, csv_path: str = "") -> pd.DataFrame:
    """Attach for SOCCER/SOC rows (sport label may be either)."""
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    sport_col = next((c for c in ("sport", "Sport") if c in out.columns), None)
    if sport_col is None:
        is_soccer = pd.Series(True, index=out.index)
    else:
        is_soccer = out[sport_col].astype(str).str.upper().str.strip().isin({"SOCCER", "SOC"})
    if sport_col is not None:
        saved = out[sport_col].copy()
        out.loc[is_soccer, sport_col] = "SOCCER"
    out = attach_lookup_columns(
        out,
        sport="SOCCER",
        lookup_fn=lambda opp, prop: lookup_stat_defense(opp, prop, csv_path=csv_path),
    )
    if sport_col is not None:
        out[sport_col] = saved
    return out


# --- Prop-aware tier selection (Shots vs goals-conceded) ---
def _norm_prop(raw: object) -> str:
    s = str(raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def prop_def_metric(prop: object) -> str:
    """Return ``shots_conceded`` or ``goals_conceded`` for this prop."""
    p = _norm_prop(prop)
    if p in SHOT_RELATED_PROPS:
        return "shots_conceded"
    # Any prop whose normalized name is shot-centric (e.g. total_shots).
    if "shot" in p and "goal" not in p:
        return "shots_conceded"
    return "goals_conceded"


def _first_valid_rank(row: pd.Series | dict, keys: Iterable[str]) -> float:
    for k in keys:
        v = row.get(k) if hasattr(row, "get") else None
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f == f and f > 0:
            return f
    return float("nan")


def _first_valid_tier(row: pd.Series | dict, keys: Iterable[str]) -> str:
    for k in keys:
        v = row.get(k) if hasattr(row, "get") else None
        label = normalize_def_tier_label(v)
        if label and label not in ("N/A",):
            return label
    return ""


def select_prop_aware_rank_tier(
    row: pd.Series | dict,
    *,
    prop: object | None = None,
    n_teams: int | None = None,
) -> tuple[float, str, str]:
    """Return (rank, tier, metric) for this prop row.

    Prefers shots ranks for shot props when present; otherwise goals overall.
    """
    if prop is None:
        prop = (
            row.get("prop_norm")
            or row.get("prop_type_normalized")
            or row.get("prop_type")
            or row.get("Prop")
        )
    metric = prop_def_metric(prop)

    if metric == "shots_conceded":
        rank = _first_valid_rank(
            row,
            (
                "SHOTS_DEF_RANK",
                "shots_def_rank",
                "opp_shots_def_rank",
            ),
        )
        tier = _first_valid_tier(
            row,
            (
                "SHOTS_DEF_TIER",
                "shots_def_tier",
                "opp_shots_def_tier",
            ),
        )
        if rank == rank:
            if not tier and n_teams and n_teams >= 2:
                try:
                    tier = def_tier_from_overall_rank(int(rank), int(n_teams))
                except (TypeError, ValueError):
                    tier = ""
            if tier:
                return rank, tier, metric
        # Fall through to goals when shots ranks missing.
        metric = "goals_conceded"

    rank = _first_valid_rank(
        row,
        (
            "OVERALL_DEF_RANK",
            "GOALS_DEF_RANK",
            "goals_def_rank",
            "def_rank",
            "Def Rank",
        ),
    )
    tier = _first_valid_tier(
        row,
        (
            "DEF_TIER",
            "def_tier",
            "GOALS_DEF_TIER",
            "goals_def_tier",
            "Def Tier",
        ),
    )
    if rank == rank and not tier and n_teams and n_teams >= 2:
        try:
            tier = def_tier_from_overall_rank(int(rank), int(n_teams))
        except (TypeError, ValueError):
            tier = ""
    return rank, (tier or ""), metric


def assign_prop_aware_def_tier(
    df: pd.DataFrame,
    *,
    prop_col: str | None = None,
    out_rank_col: str = "OVERALL_DEF_RANK",
    out_tier_col: str = "DEF_TIER",
    metric_col: str = "def_metric",
) -> pd.DataFrame:
    """Overwrite display rank/tier with prop-aware selection; keep source cols intact."""
    out = df.copy()
    if prop_col is None:
        for c in ("prop_norm", "prop_type_normalized", "prop_type", "Prop", "stat_type"):
            if c in out.columns:
                prop_col = c
                break

    # Preserve goals overall before overwrite when not already snapshotted.
    if "GOALS_DEF_RANK" not in out.columns and "OVERALL_DEF_RANK" in out.columns:
        out["GOALS_DEF_RANK"] = out["OVERALL_DEF_RANK"]
    if "GOALS_DEF_TIER" not in out.columns:
        src_tier = "DEF_TIER" if "DEF_TIER" in out.columns else ("def_tier" if "def_tier" in out.columns else None)
        if src_tier:
            out["GOALS_DEF_TIER"] = out[src_tier]

    props = out[prop_col] if prop_col and prop_col in out.columns else pd.Series([""] * len(out), index=out.index)

    # League-scoped n_teams for quintile rebuild when only rank is present.
    n_by_league: dict[str, int] = {}
    if "league" in out.columns and "SHOTS_DEF_RANK" in out.columns:
        for lg, grp in out.groupby(out["league"].astype(str)):
            ranks = pd.to_numeric(grp["SHOTS_DEF_RANK"], errors="coerce")
            n_by_league[str(lg)] = int(ranks.max()) if ranks.notna().any() else 0

    ranks: list[object] = []
    tiers: list[str] = []
    metrics: list[str] = []
    for i in range(len(out)):
        r = out.iloc[i]
        lg = str(r.get("league", "") or "")
        n_teams = n_by_league.get(lg) or None
        if n_teams is None and "OVERALL_DEF_RANK" in out.columns:
            try:
                # crude fallback: max overall rank in frame
                n_teams = int(pd.to_numeric(out["OVERALL_DEF_RANK"], errors="coerce").max())
            except (TypeError, ValueError):
                n_teams = None
        rk, tier, metric = select_prop_aware_rank_tier(
            r, prop=props.iloc[i] if len(props) else None, n_teams=n_teams
        )
        ranks.append(rk if rk == rk else pd.NA)
        tiers.append(tier)
        metrics.append(metric)

    out[out_rank_col] = ranks
    out[out_tier_col] = tiers
    out[metric_col] = metrics
    # Keep lowercase mirror in sync for step7/step8 consumers.
    if "def_tier" in out.columns or out_tier_col == "DEF_TIER":
        out["def_tier"] = out[out_tier_col]
    return out
