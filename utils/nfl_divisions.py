"""NFL conference / division membership."""

from __future__ import annotations

from utils.nfl_espn_context import canon_nfl_abbr

# 8 divisions, 4 teams each. Abbreviations match ESPN / our defense CSV (WSH not WAS).
NFL_DIVISIONS: dict[str, tuple[str, str, tuple[str, ...]]] = {
    "AFC East": ("AFC", "East", ("BUF", "MIA", "NE", "NYJ")),
    "AFC North": ("AFC", "North", ("BAL", "CIN", "CLE", "PIT")),
    "AFC South": ("AFC", "South", ("HOU", "IND", "JAX", "TEN")),
    "AFC West": ("AFC", "West", ("DEN", "KC", "LAC", "LV")),
    "NFC East": ("NFC", "East", ("DAL", "NYG", "PHI", "WSH")),
    "NFC North": ("NFC", "North", ("CHI", "DET", "GB", "MIN")),
    "NFC South": ("NFC", "South", ("ATL", "CAR", "NO", "TB")),
    "NFC West": ("NFC", "West", ("ARI", "LAR", "SEA", "SF")),
}

DIVISION_ORDER: tuple[str, ...] = tuple(NFL_DIVISIONS.keys())

_TEAM_TO_DIV: dict[str, str] = {}
for _div, (_conf, _reg, _teams) in NFL_DIVISIONS.items():
    for _ab in _teams:
        _TEAM_TO_DIV[_ab] = _div


def nfl_division(abbr: object) -> str:
    return _TEAM_TO_DIV.get(canon_nfl_abbr(abbr), "")


def nfl_conference(abbr: object) -> str:
    div = nfl_division(abbr)
    if not div:
        return ""
    return NFL_DIVISIONS[div][0]


def same_division(a: object, b: object) -> bool:
    da, db = nfl_division(a), nfl_division(b)
    return bool(da and db and da == db)


def same_conference(a: object, b: object) -> bool:
    ca, cb = nfl_conference(a), nfl_conference(b)
    return bool(ca and cb and ca == cb)
