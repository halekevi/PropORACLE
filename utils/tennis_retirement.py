"""Provisional tennis retirement grades.

Two public quotations of a chart that has not been opened. A retirement leg
is graded only when both readings agree.

Played: the games on the scoreboard.
Credited: the opponent is awarded the rest of the set in progress, not the
rest of the match. 6-4, 3-0 played is 9 games won and 13 total; credited is
12 and 16. A 7-6 retirement before the next set starts credits 6-0 and
totals 19.

If the two readings are the same HIT or the same MISS, that grade is recorded.
If they differ, the leg is UNRESOLVED. Unresolved is not a void and not a grade.

A walkover has no Total Games or Games Won grade until the chart says
otherwise. Those legs are UNRESOLVED.
"""
from __future__ import annotations

from typing import Any

PLAYED = "played"
CREDITED = "set_in_progress"


def _set_won(games: int, other: int) -> bool:
    return (games >= 6 and games - other >= 2) or (games == 7 and other in (5, 6))


def readings(
    player_sets: list[int],
    opp_sets: list[int],
    *,
    player_won: bool | None,
) -> tuple[int, int, int, int]:
    """Return played games won, played total, credited games won, credited total."""
    n = max(len(player_sets), len(opp_sets), 1)
    ps = [int(x) for x in player_sets] + [0] * (n - len(player_sets))
    os_ = [int(x) for x in opp_sets] + [0] * (n - len(opp_sets))
    played_gw = sum(ps)
    played_total = played_gw + sum(os_)
    cp = ps[:]
    co = os_[:]
    unfinished = None
    for i, (a, b) in enumerate(zip(cp, co)):
        if not (_set_won(a, b) or _set_won(b, a)):
            unfinished = i
            break
    if unfinished is None:
        cp.append(0)
        co.append(0)
        unfinished = len(cp) - 1
    a, b = cp[unfinished], co[unfinished]
    if player_won is True or (player_won is None and a > b):
        target = 7 if b >= 5 else 6
        if a < target:
            cp[unfinished] = target
    elif player_won is False or (player_won is None and b > a):
        target = 7 if a >= 5 else 6
        if b < target:
            co[unfinished] = target
    credited_gw = sum(cp)
    credited_total = credited_gw + sum(co)
    return played_gw, played_total, credited_gw, credited_total


def _hit(actual: float, line: float, direction: str) -> str:
    if abs(actual - line) < 1e-9:
        return "PUSH"
    over = actual > line
    side = str(direction or "").strip().upper()
    if side == "UNDER":
        return "HIT" if not over else "MISS"
    return "HIT" if over else "MISS"


def grade_retirement(
    line: float,
    direction: str,
    player_sets: list[int],
    opp_sets: list[int],
    *,
    player_won: bool | None,
    prop: str,
) -> dict[str, Any]:
    """Grade Games Won or Total Games under both readings."""
    played_gw, played_total, credited_gw, credited_total = readings(
        player_sets, opp_sets, player_won=player_won
    )
    key = "games_won" if "won" in prop else "total"
    played = played_gw if key == "games_won" else played_total
    credited = credited_gw if key == "games_won" else credited_total
    left = _hit(played, line, direction)
    right = _hit(credited, line, direction)
    if left == right and left in {"HIT", "MISS"}:
        result = left
    else:
        result = "UNRESOLVED"
    return {
        "result": result,
        "played": played,
        "credited": credited,
        "played_grade": left,
        "credited_grade": right,
        "played_gw": played_gw,
        "played_total": played_total,
        "credited_gw": credited_gw,
        "credited_total": credited_total,
    }


def grade_walkover(prop: str) -> dict[str, Any] | None:
    """No Games Won or Total Games grade for a walkover."""
    if prop not in {"games_won", "match_total_games"}:
        return None
    return {
        "result": "UNRESOLVED",
        "reason": "walkover_chart_silent",
    }
