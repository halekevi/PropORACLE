"""PrizePicks NBA Fantasy Score.

Weights are the PrizePicks Scoring Chart (Help → Scoring Chart / Playbook),
not DraftKings DFS. Confirmed 2026-10-29 from:

https://www.prizepicks.com/playbook-article/how-to-play-prizepicks-nba-fantasy-scoring-system

  Point      1
  Rebound    1.2
  Assist     1.5
  Steal      3
  Block      3
  Turnover  -1

No 3PM bonus. No double-double / triple-double bonus — those are not on
the PrizePicks chart (they are DraftKings). Overtime counts in full-game.
"""
from __future__ import annotations

from typing import Any

# Keep NBA family rows through fetch/grade; tickets still ban Fantasy Score
# unless a later gift-scan cell clears.
KEEP_NBA_FANTASY_SPORTS = frozenset({"NBA", "NBA1H", "NBA1Q"})

NBA_FANTASY_WEIGHTS = {
    "pts": 1.0,
    "reb": 1.2,
    "ast": 1.5,
    "stl": 3.0,
    "blk": 3.0,
    "tov": -1.0,
}

_REQUIRED = ("pts", "reb", "ast", "stl", "blk", "tov")


def keep_nba_fantasy_sport(sport: object) -> bool:
    return str(sport or "").strip().upper() in KEEP_NBA_FANTASY_SPORTS


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def nba_fantasy_score(
    pts: Any,
    reb: Any,
    ast: Any,
    stl: Any,
    blk: Any,
    tov: Any,
) -> float | None:
    """Return PrizePicks NBA Fantasy Score, or None if any box piece is missing."""
    vals = {
        "pts": _num(pts),
        "reb": _num(reb),
        "ast": _num(ast),
        "stl": _num(stl),
        "blk": _num(blk),
        "tov": _num(tov),
    }
    if any(vals[k] is None for k in _REQUIRED):
        return None
    return sum(vals[k] * NBA_FANTASY_WEIGHTS[k] for k in _REQUIRED)
