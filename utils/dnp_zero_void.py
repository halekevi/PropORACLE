"""DNP-as-zero → VOID only with positive evidence the player did not play.

Void when actual is 0 **and** one of:
  - explicit inactive / DNP / not-in-box flag
  - minutes / snaps / TOI is present and == 0

Unknown playing time → leave the grade as HIT/MISS. Do not invent voids from
actual=0 alone (that deletes real OVER misses when TOI/minutes are missing).
"""
from __future__ import annotations

import math
from typing import Any


VOID_REASON = "DNP_ZERO"

_PLAYING_TIME_KEYS = (
    "minutes",
    "minutes_played",
    "mins",
    "min",
    "mp",
    "snaps",
    "snap_count",
    "snap_pct",
    "offensive_snaps",
    "defensive_snaps",
    "toi",
    "toi_seconds",
    "time_on_ice",
    "ice_time",
)

# Legacy: volume props the old recompute treated as DNP when PT was unknown.
# Kept only so reports can count how many legs that rule would have voided.
_ZERO_SUSPECT_PROPS_LEGACY = frozenset(
    {
        "receiving_yards",
        "rushing_yards",
        "receptions",
        "longest_reception",
        "longest_rush",
        "passing_yards",
        "passing_attempts",
        "rush_attempts",
        "targets",
        "points",
        "rebounds",
        "assists",
        "pts+rebs",
        "pts+asts",
        "pts+rebs+asts",
        "pra",
        "shots",
        "shots_on_goal",
        "shots on goal",
        "saves",
        "sog",
        "shot_attempts",
    }
)

_NATURAL_ZERO_PROPS_LEGACY = frozenset(
    {
        "sacks",
        "sacks_taken",
        "int",
        "interceptions",
        "td",
        "pass_tds",
        "rush_tds",
        "rec_tds",
        "blocked shots",
        "steals",
        "3-pt made",
        "3pt made",
    }
)


def _num(v: object) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null", "—", "-", "n/a"}:
        return None
    if ":" in s:
        try:
            mm, ss = s.split(":", 1)
            return float(mm) + float(ss) / 60.0
        except (TypeError, ValueError):
            return None
    if s.endswith("%"):
        try:
            return float(s[:-1])
        except (TypeError, ValueError):
            return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _get(row: dict[str, Any] | Any, key: str) -> object:
    if isinstance(row, dict):
        return row.get(key)
    if hasattr(row, "get"):
        return row.get(key)
    return None


def playing_time_value(row: dict[str, Any] | Any) -> float | None:
    """Best available minutes/snaps/TOI from a grade row or actuals row."""
    for k in _PLAYING_TIME_KEYS:
        n = _num(_get(row, k))
        if n is not None:
            return n
    return None


def _prop_key(row: dict[str, Any] | Any) -> str:
    for k in ("prop", "prop_type", "Prop", "prop_type_norm"):
        v = _get(row, k)
        if v is not None and str(v).strip():
            return str(v).strip().lower().replace(" ", "_")
    return ""


def _explicit_dnp_flag(row: dict[str, Any] | Any) -> bool:
    """Positive evidence: inactive list, DNP flag, or absent-from-box marker."""
    for k in (
        "dnp",
        "dnp_zero",
        "inactive",
        "did_not_play",
        "not_in_box",
        "absent_from_boxscore",
        "absent_from_box",
    ):
        v = _get(row, k)
        if v is True or str(v).strip().lower() in {
            "1",
            "true",
            "yes",
            "dnp",
            "inactive",
            "absent",
        }:
            return True
    vr = str(_get(row, "void_reason") or "").upper()
    if any(
        tok in vr
        for tok in (
            "DNP",
            "INACTIVE",
            "NOT_ON_GAME_ROSTER",
            "INJURY_REPORT",
            "ESPN_ROSTER_DNP",
            "ABSENT",
        )
    ):
        return True
    status = str(_get(row, "status") or "").strip().lower()
    if status in {"dnp", "inactive", "out", "did not play", "did_not_play"}:
        return True
    return False


def is_dnp_zero_actual(
    actual: float | None,
    row: dict[str, Any] | Any | None = None,
    *,
    playing_time: float | None = None,
) -> bool:
    """True only with positive DNP evidence + actual==0.

    Unknown playing time → False (grade stands).
    """
    if actual is None:
        return False
    try:
        a = float(actual)
    except (TypeError, ValueError):
        return False
    if math.isnan(a) or a != 0.0:
        return False

    row = row or {}
    if _explicit_dnp_flag(row):
        return True

    pt = playing_time if playing_time is not None else playing_time_value(row)
    if pt is not None and pt <= 0.0:
        return True
    return False


def would_old_unknown_pt_void(
    actual: float | None,
    row: dict[str, Any] | Any | None = None,
) -> bool:
    """True if the retired 'unknown PT + volume prop → void' rule would fire.

    Used only for reporting how many OVER misses that rule deleted.
    """
    if is_dnp_zero_actual(actual, row):
        return False  # already a true DNP void
    if actual is None:
        return False
    try:
        a = float(actual)
    except (TypeError, ValueError):
        return False
    if math.isnan(a) or a != 0.0:
        return False
    row = row or {}
    if playing_time_value(row) is not None:
        return False  # PT known — old rule didn't invent here
    prop = _prop_key(row)
    if prop in _NATURAL_ZERO_PROPS_LEGACY:
        return False
    if prop in _ZERO_SUSPECT_PROPS_LEGACY or prop.endswith("_yards") or "reception" in prop:
        return True
    # NHL/soccer shot props often stored with spaces
    compact = prop.replace("_", " ")
    if compact in {"shots", "shots on goal", "saves", "shot attempts"}:
        return True
    return False


def void_if_dnp_zero(
    actual: float | None,
    row: dict[str, Any] | Any,
) -> tuple[str, str] | None:
    """Return (\"VOID\", reason) when positive DNP-zero applies, else None."""
    if is_dnp_zero_actual(actual, row):
        return "VOID", VOID_REASON
    return None


NFL_UNDER_ZERO_NO_SNAPS = "NFL_UNDER_ZERO_NO_SNAPS"


def _sport(row: dict[str, Any] | Any) -> str:
    for k in ("sport", "Sport", "league"):
        v = _get(row, k)
        if v is not None and str(v).strip():
            return str(v).strip().upper()
    return ""


def _direction(row: dict[str, Any] | Any) -> str:
    for k in (
        "bet_direction",
        "final_bet_direction",
        "direction",
        "Direction",
        "over_under",
        "side",
    ):
        v = _get(row, k)
        if v is None:
            continue
        s = str(v).strip().upper()
        if s in {"OVER", "UNDER", "O", "U", "MORE", "LESS"}:
            if s in {"O", "MORE"}:
                return "OVER"
            if s in {"U", "LESS"}:
                return "UNDER"
            return s
    return ""


def _played_box_evidence(row: dict[str, Any] | Any) -> bool:
    """True when another box counting stat proves the player participated."""
    v = _get(row, "played_box_evidence")
    if v is True:
        return True
    if str(v).strip().lower() in {"1", "true", "yes"}:
        return True
    return False


def void_if_nfl_under_zero_no_snaps(
    actual: float | None,
    row: dict[str, Any] | Any,
) -> tuple[str, str] | None:
    """NFL UNDER actual=0 without snap/PT evidence must not count as a HIT.

    Positive DNP evidence still voids via ``void_if_dnp_zero``. When playing
    time is unknown, VOID instead of grading UNDER-HIT on a silent zero.
    Snaps/minutes > 0 (or played_box_evidence from another counting stat)
    → real zero (UNDER HIT stands).
    """
    if _sport(row) not in {"NFL", "NFLP"}:
        return None
    if _direction(row) != "UNDER":
        return None
    if actual is None:
        return None
    try:
        a = float(actual)
    except (TypeError, ValueError):
        return None
    if math.isnan(a) or a != 0.0:
        return None
    if is_dnp_zero_actual(actual, row):
        return "VOID", VOID_REASON
    if _played_box_evidence(row):
        return None
    pt = playing_time_value(row)
    if pt is None:
        return "VOID", NFL_UNDER_ZERO_NO_SNAPS
    if pt <= 0.0:
        return "VOID", VOID_REASON
    return None


SOCCER_UNDER_ZERO_NO_MINUTES = "SOCCER_UNDER_ZERO_NO_MINUTES"


def void_if_soccer_under_zero_no_minutes(
    actual: float | None,
    row: dict[str, Any] | Any,
) -> tuple[str, str] | None:
    """Soccer UNDER actual=0 without minutes evidence must not count as a HIT.

    Same shape as the NFL snap rule. Positive DNP (minutes==0) still voids
    via ``void_if_dnp_zero``. Unknown minutes + a silent zero is VOID, not
    an Under hit that pads L5. A played-box 0 (minutes > 0) stays graded.
    OVER zeros with unknown minutes stay MISS.
    """
    if _sport(row) not in {"SOCCER", "SOC"}:
        return None
    if _direction(row) != "UNDER":
        return None
    if actual is None:
        return None
    try:
        a = float(actual)
    except (TypeError, ValueError):
        return None
    if math.isnan(a) or a != 0.0:
        return None
    if is_dnp_zero_actual(actual, row):
        return "VOID", VOID_REASON
    if _played_box_evidence(row):
        return None
    pt = playing_time_value(row)
    if pt is None:
        return "VOID", SOCCER_UNDER_ZERO_NO_MINUTES
    if pt <= 0.0:
        return "VOID", VOID_REASON
    return None


FACEOFF_PCT_MISMATCH = "FACEOFF_PCT_MISMATCH"


def void_if_nhl_faceoff_pct_vs_count(
    actual: float | None,
    row: dict[str, Any] | Any,
) -> tuple[str, str] | None:
    """VOID when FO win-% is graded against a faceoffs-won count line."""
    prop = _prop_key(row)
    if "faceoff" not in prop.replace(" ", "_"):
        return None
    if actual is None:
        return None
    try:
        a = float(actual)
    except (TypeError, ValueError):
        return None
    line = None
    for k in ("line", "Line", "line_score", "LINE"):
        line = _num(_get(row, k))
        if line is not None:
            break
    if line is None:
        return None
    # Count lines are typically 0.5–15; FO% is typically 20–80.
    if a > 20.0 and line < 20.0:
        return "VOID", FACEOFF_PCT_MISMATCH
    return None
