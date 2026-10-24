#!/usr/bin/env python3
"""Shared playoff configuration for NBA-focused pipeline behavior."""

from __future__ import annotations


# Empty during the 2026-27 regular season. Populating this with last spring's
# field made every Oct tip between two "playoff teams" (e.g. OKC@DEN) flag
# is_playoff_game=True and pollute rest/L5 context. Re-fill when the 2027
# postseason field is set.
NBA_PLAYOFF_TEAMS: set[str] = set()


def norm_team_abbr(team: object) -> str:
    raw = str(team or "").strip().upper()
    if not raw:
        return ""
    return raw.split("/")[0].strip()


def is_nba_playoff_team(team: object) -> bool:
    return norm_team_abbr(team) in NBA_PLAYOFF_TEAMS
