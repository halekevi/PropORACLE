"""Fill missing CBB / WCBB board opponents from the ESPN scoreboard.

PrizePicks often posts one side of a game without the other team on the
board, so step1 cannot infer ``pp_opp_team`` from ``game_teams``. NBA boards
carry both sides / schedule context; this brings CBB closer to that bar.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any

import pandas as pd

# PP abbreviation → ESPN abbreviation (and the reverse via the same map).
_PP_TO_ESPN = {
    "IND": "IU",
    "CCAR": "CCU",
    "L-MD": "L-MD",
    "W&M": "W&M",
}
_ESPN_TO_PP = {v: k for k, v in _PP_TO_ESPN.items()}

_EMPTY = frozenset({"", "nan", "none", "nat", "<na>", "null"})


def _norm_abbr(raw: object) -> str:
    s = str(raw or "").strip()
    if s.lower() in _EMPTY:
        return ""
    return s.upper()


def _is_blank_opp(raw: object) -> bool:
    return _norm_abbr(raw) == ""


def _ymd(slate_date: str | date | datetime | None) -> str | None:
    if slate_date is None:
        return None
    if isinstance(slate_date, datetime):
        return slate_date.date().isoformat().replace("-", "")
    if isinstance(slate_date, date):
        return slate_date.isoformat().replace("-", "")
    s = str(slate_date).strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10].replace("-", "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else None


def _fetch_scoreboard(ymd: str, *, league: str = "mens-college-basketball") -> list[dict[str, Any]]:
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/basketball/"
        f"{league}/scoreboard?dates={ymd}&limit=300&groups=50"
    )
    try:
        with urllib.request.urlopen(url, timeout=25) as resp:
            data = json.load(resp)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError):
        return []
    return list(data.get("events") or [])


def _pair_map_from_events(events: list[dict[str, Any]]) -> dict[str, str]:
    """ESPN abbr → opponent ESPN abbr for every competitor on the board."""
    out: dict[str, str] = {}
    for event in events:
        comps = ((event.get("competitions") or [{}])[0].get("competitors") or [])
        abbrs: list[str] = []
        for c in comps:
            abbr = _norm_abbr((c.get("team") or {}).get("abbreviation"))
            if abbr:
                abbrs.append(abbr)
        if len(abbrs) != 2:
            continue
        a, b = abbrs[0], abbrs[1]
        out[a] = b
        out[b] = a
    return out


def _lookup_opp(team_pp: str, espn_pairs: dict[str, str]) -> str:
    team = _norm_abbr(team_pp)
    if not team:
        return ""
    espn_team = _PP_TO_ESPN.get(team, team)
    espn_opp = espn_pairs.get(espn_team, "")
    if not espn_opp:
        # Try raw PP abbr against ESPN map in case aliases already match.
        espn_opp = espn_pairs.get(team, "")
    if not espn_opp:
        return ""
    return _ESPN_TO_PP.get(espn_opp, espn_opp)


def fill_cbb_opponents(
    df: pd.DataFrame,
    *,
    slate_date: str | date | datetime | None = None,
    league: str = "mens-college-basketball",
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Fill blank / 'nan' opponent fields from ESPN. Returns (df, stats)."""
    if df is None or df.empty:
        return df, {"filled": 0, "still_blank": 0, "events": 0}

    work = df.copy()
    team_col = next(
        (c for c in ("pp_team", "team_abbr", "team", "Team") if c in work.columns),
        None,
    )
    opp_cols = [
        c
        for c in ("pp_opp_team", "opp_team_abbr", "opp", "Opp", "opp_team")
        if c in work.columns
    ]
    if team_col is None or not opp_cols:
        return work, {"filled": 0, "still_blank": 0, "events": 0}

    ymd = _ymd(slate_date)
    if not ymd and "start_time" in work.columns:
        for raw in work["start_time"].astype(str).tolist():
            ymd = _ymd(raw)
            if ymd:
                break
    if not ymd:
        return work, {"filled": 0, "still_blank": 0, "events": 0}

    events = _fetch_scoreboard(ymd, league=league)
    pairs = _pair_map_from_events(events)
    filled = 0
    for idx, row in work.iterrows():
        primary = row.get(opp_cols[0])
        if not _is_blank_opp(primary):
            # Normalize string "nan" leftovers even when one opp col is set.
            continue
        # If any opp col already has a real value, copy it across.
        existing = ""
        for c in opp_cols:
            v = _norm_abbr(row.get(c))
            if v:
                existing = v
                break
        if existing:
            for c in opp_cols:
                if _is_blank_opp(work.at[idx, c]):
                    work.at[idx, c] = existing
            continue
        opp = _lookup_opp(row.get(team_col), pairs)
        if not opp:
            continue
        for c in opp_cols:
            work.at[idx, c] = opp
        filled += 1

    # Clear literal "nan" strings that block D / schedule gates.
    for c in opp_cols:
        s = work[c].astype(str).str.strip()
        mask = s.str.lower().isin(_EMPTY)
        if mask.any():
            work.loc[mask, c] = ""

    still = 0
    primary = opp_cols[0]
    still = int(work[primary].map(_is_blank_opp).sum())
    return work, {"filled": filled, "still_blank": still, "events": len(events)}
