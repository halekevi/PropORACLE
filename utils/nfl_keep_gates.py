"""NFL keep gates from Week 1 2026 graded Standard board (post-backfill).

Week 1 ledger uses **unique player-games** (see ``utils.nfl_prop_gate_ledger``).
Books opened high. Standard UNDER cleared on receiving yards / receptions /
longest reception / sacks / kick pts. Pass yards is a fade on both sides until
a Gate70 sample builds.

Ticket priority (UNDER-heavy until Goblins exist):
  1 receiving_yards U
  2 receptions U
  3 longest_reception U
  4 sacks U
  5 kicking_points U
  6 rushing_yards (OVER or UNDER)
  everything else gated out

Active monitors (not allowlisted):
  - sacks_taken UNDER — 3/3 Week 1, ungated; need unique n>=15 + Elite/Above D
    vs high-sack pass rush before diversifying off defensive sacks.
  - fantasy_score skill UNDER — ~78% unique n=27; revisit at n>=40 after Week 3.
    Kicker fantasy is a kick-pts duplicate — stay off.

Goblin OVER (when odds_type is scraped): same universal ticket gate as other
sports — L5=5 + L10>=8 + directional D + cover floor. Week 1 had no Goblin
rows because step1 dropped odds_type; Week 2+ fetches keep pick_type. Goblin
ledger rates must use unique-game dedup (ladder boards concentrate on high-usage
players).
"""

from __future__ import annotations

from typing import Any

# Ticket allowlist + pack order (lower = earlier).
NFL_TICKET_PRIORITY: tuple[tuple[str, str], ...] = (
    ("receiving_yards", "UNDER"),
    ("receptions", "UNDER"),
    ("longest_reception", "UNDER"),
    ("sacks", "UNDER"),
    ("kicking_points", "UNDER"),
    ("rushing_yards", "UNDER"),
    ("rushing_yards", "OVER"),
)

NFL_STD_UNDER_KEEP = frozenset(
    {
        "receiving_yards",
        "receptions",
        "longest_reception",
        "sacks",
        "kicking_points",
        "rushing_yards",
    }
)

NFL_STD_OVER_KEEP = frozenset(
    {
        "rushing_yards",
    }
)

# Pass yards / TDs stay out until Gate70 sample builds (Week 1 U 42%, O 15%).
NFL_STD_PASS_YARDS_OFF = frozenset(
    {
        "passing_yards",
        "passing_tds",
        "pass_attempts",
        "pass_completions",
        "completions",
    }
)

NFL_STD_OVER_FADE = frozenset(
    {
        "passing_yards",
        "passing_tds",
        "player_touchdowns",
        "fg_made",
        "sacks",
        "receiving_yards",
        "receptions",
        "longest_reception",
        "longest_rush",
        "pass_rush_yds",
        "rush_rec_yds",
    }
)

_PRIORITY_INDEX = {
    (prop, side): i for i, (prop, side) in enumerate(NFL_TICKET_PRIORITY)
}


def _tok(v: object) -> str:
    return str(v or "").strip().lower().replace(" ", "_").replace("-", "_")


def _prop(r: dict[str, Any]) -> str:
    for k in ("prop", "Prop", "prop_type", "prop_type_normalized", "stat_type"):
        if k in r and r.get(k) not in (None, ""):
            return _tok(r.get(k))
    return ""


def _side(r: dict[str, Any]) -> str:
    for k in ("side", "Direction", "bet_direction", "recommended_side", "dir"):
        if k in r and r.get(k) not in (None, ""):
            return str(r.get(k) or "").strip().upper()
    return ""


def _pick(r: dict[str, Any]) -> str:
    for k in ("pick_type", "Pick Type"):
        if k in r and r.get(k) not in (None, ""):
            return str(r.get(k) or "").strip().title()
    return "Standard"


def nfl_ticket_priority(r: dict[str, Any]) -> int:
    """Pack order for NFL Standard legs. Unknown keep props sort last."""
    key = (_prop(r), _side(r))
    return _PRIORITY_INDEX.get(key, 99)


def nfl_standard_ticket_eligible(r: dict[str, Any]) -> bool:
    """True when this Standard leg is allowed on Goblin-70 / Flex tickets."""
    if _pick(r) != "Standard":
        return False
    side = _side(r)
    prop = _prop(r)
    if prop in NFL_STD_PASS_YARDS_OFF:
        return False
    if side == "UNDER":
        return prop in NFL_STD_UNDER_KEEP
    if side == "OVER":
        return prop in NFL_STD_OVER_KEEP
    return False


def nfl_standard_over_is_fade(r: dict[str, Any]) -> bool:
    return _side(r) == "OVER" and _prop(r) in NFL_STD_OVER_FADE
