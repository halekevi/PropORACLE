"""NBA prop-specific opponent defense ranks (allowed PTS/REB/AST/…).

Same Elite→Weak five-bucket scheme as WNBA (via ``utils.basketball_prop_defense``).
Built from box-score game logs in proporacle_ref.db when CSV is missing.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from utils import basketball_prop_defense as bpd

PROP_TO_CAT: dict[str, str] = dict(bpd.PROP_TO_CAT)

TEAM_MAP: dict[str, str] = {
    "ATL": "ATL",
    "BOS": "BOS",
    "BKN": "BKN",
    "BRK": "BKN",
    "CHA": "CHA",
    "CHO": "CHA",
    "CHI": "CHI",
    "CLE": "CLE",
    "DAL": "DAL",
    "DEN": "DEN",
    "DET": "DET",
    "GSW": "GSW",
    "GS": "GSW",
    "HOU": "HOU",
    "IND": "IND",
    "LAC": "LAC",
    "LAL": "LAL",
    "MEM": "MEM",
    "MIA": "MIA",
    "MIL": "MIL",
    "MIN": "MIN",
    "NOP": "NOP",
    "NO": "NOP",
    "NOR": "NOP",
    "NYK": "NYK",
    "NY": "NYK",
    "OKC": "OKC",
    "ORL": "ORL",
    "PHI": "PHI",
    "PHX": "PHX",
    "PHO": "PHX",
    "POR": "POR",
    "SAC": "SAC",
    "SAS": "SAS",
    "SA": "SAS",
    "TOR": "TOR",
    "UTA": "UTA",
    "UTAH": "UTA",
    "WAS": "WAS",
    "WSH": "WAS",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_csv_path() -> Path:
    return _repo_root() / "Sports" / "NBA" / "data" / "nba_defense_by_stat.csv"


def default_db_path() -> Path:
    return bpd.default_db_path()


def canon_team(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s or "/" in s:
        return ""
    return TEAM_MAP.get(s, s)


def prop_category(prop: object) -> str:
    return bpd.prop_category(prop)


def rebuild_defense_by_stat(
    db_path: Optional[Path] = None,
    out_path: Optional[Path] = None,
) -> pd.DataFrame:
    return bpd.rebuild_defense_by_stat(
        sport_table="nba",
        out_path=Path(out_path or default_csv_path()),
        db_path=Path(db_path) if db_path else default_db_path(),
        team_map=TEAM_MAP,
        exclude_teams=set(),
        overall_rank_hook=None,
    )


def clear_defense_cache() -> None:
    bpd.clear_defense_table_cache()


def lookup_stat_defense(
    opp: object,
    prop: object,
    *,
    csv_path: str = "",
) -> dict:
    path = str(csv_path or default_csv_path())
    if not Path(path).is_file() or Path(path).stat().st_size < 50:
        rebuild_defense_by_stat(out_path=Path(path))
        bpd.clear_defense_table_cache()
    return bpd.lookup_stat_defense(
        opp, prop, csv_path=path, canon_team_fn=canon_team
    )


def attach_stat_defense_columns(df: pd.DataFrame, *, csv_path: str = "") -> pd.DataFrame:
    """Add stat_def_* for NBA / NBA1H / NBA1Q rows."""
    path = str(csv_path or default_csv_path())
    if not Path(path).is_file() or Path(path).stat().st_size < 50:
        rebuild_defense_by_stat(out_path=Path(path))
        bpd.clear_defense_table_cache()
    return bpd.attach_stat_defense_columns(
        df,
        sport="NBA",
        csv_path=path,
        canon_team_fn=canon_team,
        sport_values={"NBA", "NBA1H", "NBA1Q"},
    )
