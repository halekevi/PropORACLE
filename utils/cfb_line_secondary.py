"""CFB OL / DL / secondary z-score composites from CFBD advanced stats.

Observe-only. Equal-weight means are placeholders — do not live-gate.
Early-season (games <= 3) is flagged; log n_teams and games with every index.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from utils.cfb_playoff_metadata import cfb_abbr_lookup_keys, norm_cfb_team_abbr

_REPO = Path(__file__).resolve().parents[1]
_REF_CSV = _REPO / "Sports" / "CFB" / "data" / "reference" / "cfb_line_secondary.csv"
_RANK_CSV = _REPO / "Sports" / "CFB" / "data" / "reference" / "cfb_team_unit_rankings.csv"
_CACHE_DIR = _REPO / "Sports" / "CFB" / "data" / "cache"

EARLY_SEASON_GAMES = 3
WEIGHTING = "equal_mean_placeholder"

# direction: +1 if higher raw = stronger unit, -1 if lower raw = stronger.
STAT_DIRECTION: dict[str, int] = {
    "off.rushingPlays.lineYardsAverage": 1,
    "off.rushingPlays.stuffRate": -1,
    "off.rushingPlays.powerSuccess": 1,
    "off.rushingPlays.secondLevelYardsAverage": 1,
    "def.rushingPlays.lineYardsAverage": -1,
    "def.rushingPlays.stuffRate": 1,
    "def.havoc.frontSeven": 1,
    "def.havoc.db": 1,
    "def.passingPlays.explosiveness": -1,
    "def.passingPlays.successRate": -1,
}

# Alternate CFBD key paths (v1 nested vs flattened offense.*).
_FIELD_PATHS: dict[str, tuple[tuple[str, ...], ...]] = {
    "off.rushingPlays.lineYardsAverage": (
        ("offense", "rushingPlays", "lineYardsAverage"),
        ("offense", "rushingPlays", "lineYards"),
        ("offense", "lineYards"),
    ),
    "off.rushingPlays.stuffRate": (
        ("offense", "rushingPlays", "stuffRate"),
        ("offense", "stuffRate"),
    ),
    "off.rushingPlays.powerSuccess": (
        ("offense", "rushingPlays", "powerSuccess"),
        ("offense", "powerSuccess"),
    ),
    "off.rushingPlays.secondLevelYardsAverage": (
        ("offense", "rushingPlays", "secondLevelYardsAverage"),
        ("offense", "rushingPlays", "secondLevelYards"),
        ("offense", "secondLevelYards"),
    ),
    "def.rushingPlays.lineYardsAverage": (
        ("defense", "rushingPlays", "lineYardsAverage"),
        ("defense", "rushingPlays", "lineYards"),
        ("defense", "lineYards"),
    ),
    "def.rushingPlays.stuffRate": (
        ("defense", "rushingPlays", "stuffRate"),
        ("defense", "stuffRate"),
    ),
    "def.havoc.frontSeven": (
        ("defense", "havoc", "frontSeven"),
        ("defense", "frontSevenHavoc"),
        ("havoc", "frontSeven"),
    ),
    "def.havoc.db": (
        ("defense", "havoc", "db"),
        ("defense", "dbHavoc"),
        ("havoc", "db"),
    ),
    "def.passingPlays.explosiveness": (
        ("defense", "passingPlays", "explosiveness"),
        ("defense", "explosiveness"),
    ),
    "def.passingPlays.successRate": (
        ("defense", "passingPlays", "successRate"),
        ("defense", "successRate"),
    ),
}

OL_FIELDS = (
    "off.rushingPlays.lineYardsAverage",
    "off.rushingPlays.stuffRate",
    "off.rushingPlays.powerSuccess",
)
DL_FIELDS = (
    "def.rushingPlays.lineYardsAverage",
    "def.rushingPlays.stuffRate",
    "def.havoc.frontSeven",
)
SEC_FIELDS = (
    "def.havoc.db",
    "def.passingPlays.explosiveness",
    "def.passingPlays.successRate",
)


def get_nested(row: dict[str, Any], field: str) -> float | None:
    for path in _FIELD_PATHS.get(field, (tuple(field.split(".")),)):
        cur: Any = row
        ok = True
        for part in path:
            if not isinstance(cur, dict) or part not in cur:
                ok = False
                break
            cur = cur[part]
        if not ok:
            continue
        try:
            v = float(cur)
        except (TypeError, ValueError):
            continue
        if v == v:
            return v
    return None


def compute_league_reference(all_team_stats: list[dict[str, Any]]) -> dict[str, dict[str, float | int]]:
    ref: dict[str, dict[str, float | int]] = {}
    for field in STAT_DIRECTION:
        vals = [get_nested(t, field) for t in all_team_stats]
        vals_f = [v for v in vals if v is not None]
        n = len(vals_f)
        if n == 0:
            ref[field] = {"mean": 0.0, "std": 0.0, "n_teams": 0}
            continue
        mean = sum(vals_f) / n
        var = sum((x - mean) ** 2 for x in vals_f) / n
        ref[field] = {"mean": mean, "std": math.sqrt(var), "n_teams": n}
    return ref


def zscore(value: float | None, ref_entry: dict[str, float | int] | None, direction: int) -> float | None:
    if value is None or not ref_entry:
        return None
    std = float(ref_entry.get("std") or 0.0)
    if std == 0:
        return 0.0
    return float(direction) * (float(value) - float(ref_entry["mean"])) / std


def _composite(row: dict[str, Any], ref: dict[str, dict[str, float | int]], fields: tuple[str, ...]) -> float | None:
    zs: list[float] = []
    for f in fields:
        z = zscore(get_nested(row, f), ref.get(f), STAT_DIRECTION[f])
        if z is not None:
            zs.append(z)
    if not zs:
        return None
    return sum(zs) / len(zs)


def compute_ol_index(row: dict[str, Any], ref: dict[str, dict[str, float | int]]) -> float | None:
    return _composite(row, ref, OL_FIELDS)


def compute_dl_index(row: dict[str, Any], ref: dict[str, dict[str, float | int]]) -> float | None:
    return _composite(row, ref, DL_FIELDS)


def compute_secondary_index(row: dict[str, Any], ref: dict[str, dict[str, float | int]]) -> float | None:
    return _composite(row, ref, SEC_FIELDS)


def _norm_name(s: object) -> str:
    t = re.sub(r"[^a-z0-9]+", " ", str(s or "").lower()).strip()
    return re.sub(r"\s+", " ", t)


def build_team_lookup(rankings: pd.DataFrame) -> dict[str, str]:
    """Normalized CFBD/ESPN name → rankings abbr."""
    out: dict[str, str] = {}
    if rankings.empty:
        return out
    abbr_col = "team_abbr" if "team_abbr" in rankings.columns else None
    name_col = "team_name" if "team_name" in rankings.columns else None
    if not abbr_col:
        return out
    for _, r in rankings.iterrows():
        abbr = norm_cfb_team_abbr(r.get(abbr_col)) or str(r.get(abbr_col) or "").strip().upper()
        if not abbr:
            continue
        for key in cfb_abbr_lookup_keys(abbr):
            out[_norm_name(key)] = abbr
        out[_norm_name(abbr)] = abbr
        if name_col:
            name = str(r.get(name_col) or "").strip()
            if name:
                out[_norm_name(name)] = abbr
                parts = name.split()
                if len(parts) >= 2:
                    out[_norm_name(" ".join(parts[:-1]))] = abbr
    return out


def lookup_abbr(team: object, lookup: dict[str, str]) -> str:
    raw = str(team or "").strip()
    if not raw:
        return ""
    hit = lookup.get(_norm_name(raw)) or lookup.get(_norm_name(norm_cfb_team_abbr(raw)))
    if hit:
        return hit
    # "Ohio State Buckeyes" vs CFBD "Ohio State"
    parts = raw.split()
    if len(parts) >= 2:
        hit = lookup.get(_norm_name(" ".join(parts[:-1])))
        if hit:
            return hit
    return norm_cfb_team_abbr(raw) or raw.upper()


def index_rows(
    all_team_stats: list[dict[str, Any]],
    *,
    lookup: dict[str, str] | None = None,
    games_by_abbr: dict[str, float] | None = None,
    season: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    ref = compute_league_reference(all_team_stats)
    lookup = lookup or {}
    games_by_abbr = games_by_abbr or {}
    rows: list[dict[str, Any]] = []
    for t in all_team_stats:
        if not isinstance(t, dict):
            continue
        school = str(t.get("team") or t.get("school") or "").strip()
        abbr = lookup_abbr(school, lookup) if lookup else (norm_cfb_team_abbr(school) or school)
        games = None
        for k in cfb_abbr_lookup_keys(abbr) | {abbr, str(abbr).upper()}:
            if str(k).upper() in games_by_abbr:
                games = games_by_abbr[str(k).upper()]
                break
        if games is None:
            try:
                games = float(t.get("games") or t.get("gp") or float("nan"))
            except (TypeError, ValueError):
                games = float("nan")
        ol = compute_ol_index(t, ref)
        dl = compute_dl_index(t, ref)
        sec = compute_secondary_index(t, ref)
        early = bool(games == games and games <= EARLY_SEASON_GAMES)
        rec: dict[str, Any] = {
            "team": school,
            "team_abbr": abbr,
            "season": season,
            "games": games if games == games else None,
            "early_season": early,
            "ol_index": None if ol is None else round(ol, 3),
            "dl_index": None if dl is None else round(dl, 3),
            "secondary_index": None if sec is None else round(sec, 3),
            "weighting": WEIGHTING,
        }
        for f in STAT_DIRECTION:
            raw = get_nested(t, f)
            rec[f] = None if raw is None else round(raw, 4)
            z = zscore(raw, ref.get(f), STAT_DIRECTION[f])
            rec[f + "_z"] = None if z is None else round(z, 3)
        rows.append(rec)
    df = pd.DataFrame(rows)
    meta = {
        "observe_only": True,
        "weighting": WEIGHTING,
        "early_season_games": EARLY_SEASON_GAMES,
        "n_teams": int(len(df)),
        "ref": {k: {sk: (int(sv) if sk == "n_teams" else round(float(sv), 6)) for sk, sv in v.items()} for k, v in ref.items()},
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "season": season,
    }
    return df, meta


def rush_line_mismatch(ol_index: float | None, opp_dl_index: float | None) -> float | None:
    """Positive = this offense's OL is stronger than opponent front seven."""
    if ol_index is None or opp_dl_index is None:
        return None
    return round(float(ol_index) - float(opp_dl_index), 3)


def pass_db_mismatch(opp_secondary_index: float | None) -> float | None:
    """Positive = opponent secondary is strong (harder pass/rec OVER)."""
    if opp_secondary_index is None:
        return None
    return round(float(opp_secondary_index), 3)


def load_line_secondary_table(path: Path | None = None) -> pd.DataFrame:
    p = path or _REF_CSV
    if not p.is_file():
        return pd.DataFrame()
    try:
        return pd.read_csv(p)
    except Exception:
        return pd.DataFrame()


def _by_abbr(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if df.empty or "team_abbr" not in df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for rec in df.to_dict(orient="records"):
        abbr = norm_cfb_team_abbr(rec.get("team_abbr")) or str(rec.get("team_abbr") or "").strip().upper()
        if not abbr:
            continue
        row = dict(rec)
        for key in cfb_abbr_lookup_keys(abbr):
            out[str(key).upper()] = row
        out[abbr] = row
    return out


def attach_line_secondary(
    df: pd.DataFrame,
    *,
    table: pd.DataFrame | None = None,
    team_col: str = "Team",
    opp_col: str = "Opp",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Observe-only OL/DL/secondary labels. Does not change rank/ticket columns."""
    out = df.copy()
    labels = [
        "OL Index",
        "DL Index",
        "Secondary Index",
        "Opp OL Index",
        "Opp DL Index",
        "Opp Secondary Index",
        "Rush Line Mismatch",
        "Pass DB Mismatch",
        "Line Games",
        "Line Early Season",
    ]
    for c in labels:
        if c not in out.columns:
            out[c] = pd.NA
    tab = load_line_secondary_table() if table is None else table
    meta = {
        "observe_only": True,
        "weighting": WEIGHTING,
        "rows_matched": 0,
        "rows_total": int(len(out)),
        "table_teams": 0 if tab.empty else int(tab["team_abbr"].nunique()) if "team_abbr" in tab.columns else int(len(tab)),
        "source": str(_REF_CSV) if table is None else "passed",
    }
    if out.empty or tab.empty:
        return out, meta
    by = _by_abbr(tab)
    n_hit = 0
    for idx, row in out.iterrows():
        team = str(row.get(team_col) or "").strip().upper()
        opp = str(row.get(opp_col) or "").strip().upper()
        own = by.get(team) or by.get(norm_cfb_team_abbr(team))
        opp_r = by.get(opp) or by.get(norm_cfb_team_abbr(opp))
        if not own and not opp_r:
            continue
        n_hit += 1
        if own:
            out.at[idx, "OL Index"] = own.get("ol_index")
            out.at[idx, "DL Index"] = own.get("dl_index")
            out.at[idx, "Secondary Index"] = own.get("secondary_index")
            out.at[idx, "Line Games"] = own.get("games")
            out.at[idx, "Line Early Season"] = bool(own.get("early_season"))
        if opp_r:
            out.at[idx, "Opp OL Index"] = opp_r.get("ol_index")
            out.at[idx, "Opp DL Index"] = opp_r.get("dl_index")
            out.at[idx, "Opp Secondary Index"] = opp_r.get("secondary_index")
        ol = None if own is None else own.get("ol_index")
        opp_dl = None if opp_r is None else opp_r.get("dl_index")
        opp_sec = None if opp_r is None else opp_r.get("secondary_index")
        try:
            ol_f = float(ol) if ol is not None and ol == ol else None
        except (TypeError, ValueError):
            ol_f = None
        try:
            dl_f = float(opp_dl) if opp_dl is not None and opp_dl == opp_dl else None
        except (TypeError, ValueError):
            dl_f = None
        try:
            sec_f = float(opp_sec) if opp_sec is not None and opp_sec == opp_sec else None
        except (TypeError, ValueError):
            sec_f = None
        out.at[idx, "Rush Line Mismatch"] = rush_line_mismatch(ol_f, dl_f)
        out.at[idx, "Pass DB Mismatch"] = pass_db_mismatch(sec_f)
    meta["rows_matched"] = int(n_hit)
    return out, meta


def write_outputs(df: pd.DataFrame, meta: dict[str, Any], *, csv_path: Path | None = None) -> None:
    csv_path = csv_path or _REF_CSV
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    season = meta.get("season") or "na"
    ref_path = _CACHE_DIR / f"cfb_line_secondary_ref_{season}.json"
    ref_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
