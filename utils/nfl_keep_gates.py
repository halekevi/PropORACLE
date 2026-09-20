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

**NFL Goblin OVER stays unissued** (``NFL_GOBLIN_TICKETS_ENABLED = False``) until
a tagged Goblin unique-game ledger answers:
  1. OVER vs UNDER split by chip — does cross-sport Goblin-70 OVER skew hold,
     or does Week 1 UNDER bias persist on Goblin lines?
  2. Extreme-low lines (sacks/TDs/FG 0.5) — need a minimum line floor separate
     from cover?
  3. Do L10>=8 + D filter, or is the low Goblin line doing all the work?
  4. Cover floor — Standard uses absolute floors (pass +15, rush-rec +8);
     Goblin may need cover as **% of line** (absolute +8 on a 12.5 Goblin is
     meaningful; on a 4.5 reception Goblin it is not).

Active monitors (not allowlisted):
  - sacks_taken UNDER — need unique n>=15 + Elite/Above D vs high-sack pass rush.
  - fantasy_score skill UNDER — revisit at unique n>=40; kicker fantasy = kick-pts
    duplicate, stay off.

Goblin rates must use unique-game dedup (ladder boards concentrate on high-usage
players). Extreme-low Goblin OVER + L5=5 alone is not an edge.
"""

from __future__ import annotations

from typing import Any

# Flip only after Week 2+ Goblin unique-game ledger answers the questions above.
NFL_GOBLIN_TICKETS_ENABLED = False

# Week 2 2026 decision (explicit — do not quietly work around the Sep-1 season cut):
# step8 current-season L5 is honestly thin (often 0/1 / THIN_*). That empty/thin
# answer is correct, same posture as Tennis Standard going to zero after backfill.
# step5 may PRIOR_SEASON_FILL display L5 in weeks 1–3; tickets still use
# nfl_current_season_l5_ok (current n>=5). Do NOT flip
# NFL_WIDER_WINDOW_TICKET_L5_ENABLED to manufacture READY. Sit-list /
# n=1 hand-checks live outside this module;
# any path that bypasses the hand-checked list bypasses that judgment too.
# Flip only after an explicit product decision, not because tonight's pool is thin.
NFL_WIDER_WINDOW_TICKET_L5_ENABLED = False

# Construction sources allowed to mark an NFL slip READY tonight.
NFL_HANDCHECK_TICKET_SOURCES = frozenset({"handcheck", "hand_check", "n1_handcheck"})


class NflWiderWindowTicketBlocked(RuntimeError):
    """Raised when ticket construction tries to use padded/wider-window L5."""


class NflTicketSourceBlocked(RuntimeError):
    """Raised when an NFL slip would be READY without a hand-check source tag."""


def assert_nfl_ticket_l5_source(source: str) -> None:
    """Loud refuse for the unguarded wider-window door into NFL ticket L5.

    Call before any NFL slip build that would compute directional L5 from
    ``nfl_boxscore_cache`` (or any non-step8 season-cut window) for eligibility.
    """
    src = str(source or "").strip().lower().replace("-", "_").replace(" ", "_")
    if src in {
        "wider_window",
        "wider_window_l5",
        "boxscore_cache",
        "last_season",
        "preseason",
        "padded_l5",
        "fallback_l5",
        "cache_l5",
    }:
        if not NFL_WIDER_WINDOW_TICKET_L5_ENABLED:
            raise NflWiderWindowTicketBlocked(
                f"NFL_WIDER_WINDOW_TICKET_BLOCKED source={source!r}. "
                "Season-cut step8 thin/empty is the honest answer; do not rebuild "
                "ticket L5 from boxscore cache. Use the hand-checked list only. "
                "Set NFL_WIDER_WINDOW_TICKET_L5_ENABLED only after an explicit decision."
            )


def assert_nfl_ready_source(source: str) -> None:
    """NFL slips may not print READY unless tagged as hand-check construction."""
    src = str(source or "").strip().lower().replace("-", "_").replace(" ", "_")
    if src in NFL_HANDCHECK_TICKET_SOURCES:
        return
    raise NflTicketSourceBlocked(
        f"NFL_TICKET_SOURCE_BLOCKED source={source!r}. "
        "READY requires ticket_source in NFL_HANDCHECK_TICKET_SOURCES "
        f"({sorted(NFL_HANDCHECK_TICKET_SOURCES)}). "
        "Algorithmic / wider-window / keep-packer fills are not submit-ready."
    )

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

# Extreme-low Goblin OVER chips — evaluate min-line floor in Week 2 ledger.
NFL_GOBLIN_EXTREME_LOW_PROPS = frozenset(
    {
        "sacks",
        "player_touchdowns",
        "fg_made",
        "passing_tds",
        "rushing_tds",
        "receiving_tds",
    }
)
NFL_GOBLIN_EXTREME_LOW_LINE = 0.5

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



def nfl_current_season_l5_ok(r: dict[str, Any]) -> bool:
    """Ticket Gate70 needs current-season sample depth; prior-fill is display-only.

    Weeks 1–3 step5 may set L5=5 via PRIOR_SEASON_FILL (last year mixed in).
    Tickets still require ``l5_sample_n >= 5`` on current-season games, and reject
    PRIOR_* / THIN_* / NO_CURRENT_SEASON flags.
    """
    flag = str(r.get("season_l5_flag") or r.get("Season L5 Flag") or "").strip().upper()
    if flag in {"PRIOR_SEASON_FILL", "PRIOR_SEASON_L5", "NO_CURRENT_SEASON"}:
        return False
    if flag.startswith("THIN_"):
        return False
    raw = r.get("l5_sample_n")
    if raw in (None, ""):
        raw = r.get("L5 Sample N")
    try:
        n = float(raw) if raw not in (None, "") else 0.0
    except (TypeError, ValueError):
        n = 0.0
    return n >= 5.0


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
    ok = False
    if side == "UNDER":
        ok = prop in NFL_STD_UNDER_KEEP
    elif side == "OVER":
        ok = prop in NFL_STD_OVER_KEEP
    if not ok:
        return False
    # Soft-first: participation_gate_check is a no-op until HARD flag flips.
    from utils.nfl_route_participation_gate import participation_gate_check

    allowed, _reason = participation_gate_check(r)
    return bool(allowed)


def nfl_standard_over_is_fade(r: dict[str, Any]) -> bool:
    return _side(r) == "OVER" and _prop(r) in NFL_STD_OVER_FADE


def nfl_goblin_ticket_eligible(r: dict[str, Any]) -> bool:
    """NFL Goblin OVER ticket gate — off until Week 2+ ledger unlocks it."""
    if not NFL_GOBLIN_TICKETS_ENABLED:
        return False
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    return True


def nfl_goblin_is_extreme_low_line(r: dict[str, Any]) -> bool:
    """True for 0.5-class Goblin OVER counting chips (sacks/TDs/FG)."""
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    if _prop(r) not in NFL_GOBLIN_EXTREME_LOW_PROPS:
        return False
    try:
        return float(r.get("line") or r.get("Line") or 0) <= NFL_GOBLIN_EXTREME_LOW_LINE + 1e-9
    except (TypeError, ValueError):
        return False
