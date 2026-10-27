"""Within-ticket correlation: hard blocks vs same-game flag.

Hard blocks (always):
  - same player twice on one slip
  - every leg from the same team

Same-game pairs are allowed. Stamp ``same_game_pair`` on the slip and
grade those slips versus slips without it. Do not gate on the flag until
each group has at least SAME_GAME_PAIR_GATE_N graded slips.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

SAME_GAME_PAIR_GATE_N = 100


def player_key(leg: dict[str, Any]) -> str:
    raw = (
        leg.get("player_key")
        or leg.get("player")
        or leg.get("player_name")
        or ""
    )
    return " ".join(str(raw).strip().lower().split())


def team_key(leg: dict[str, Any]) -> str:
    for key in ("team", "player_team", "team_abbr", "abbr"):
        raw = leg.get(key)
        if raw not in (None, ""):
            return str(raw).strip().upper()
    return ""


def game_key(leg: dict[str, Any]) -> str:
    for key in (
        "game_key",
        "espn_event_id",
        "espn_game_id",
        "pp_game_id",
        "event_id",
        "game_id",
        "game_ppid",
    ):
        raw = leg.get(key)
        if raw not in (None, ""):
            return str(raw).strip()
    matchup = " ".join(str(leg.get("matchup") or "").upper().split())
    if matchup:
        return matchup
    team = team_key(leg)
    opp = str(
        leg.get("opp_team") or leg.get("opp") or leg.get("opponent") or ""
    ).strip().upper()
    if team and opp:
        return "|".join(sorted((team, opp)))
    return ""


def has_duplicate_player(legs: Iterable[dict[str, Any]]) -> bool:
    counts: Counter[str] = Counter()
    for leg in legs:
        pk = player_key(leg)
        if pk:
            counts[pk] += 1
    return any(n > 1 for n in counts.values())


def all_same_team(legs: Iterable[dict[str, Any]]) -> bool:
    rows = list(legs)
    if len(rows) < 2:
        return False
    teams = {team_key(leg) for leg in rows}
    teams.discard("")
    return len(teams) == 1


def has_same_game_pair(legs: Iterable[dict[str, Any]]) -> bool:
    counts: Counter[str] = Counter()
    for leg in legs:
        gk = game_key(leg)
        if gk:
            counts[gk] += 1
    return any(n >= 2 for n in counts.values())


def hard_block_reason(legs: Iterable[dict[str, Any]]) -> str | None:
    """Return a block reason, or None when the slip may be placed."""
    rows = list(legs)
    if has_duplicate_player(rows):
        return "same_player_twice"
    if all_same_team(rows):
        return "all_same_team"
    return None


def annotate_slip(slip: dict[str, Any]) -> dict[str, Any]:
    """Stamp same_game_pair (flag only) and hard-block fields on a slip dict."""
    legs = list(slip.get("legs") or [])
    block = hard_block_reason(legs)
    slip["same_game_pair"] = bool(has_same_game_pair(legs))
    slip["correlation_block"] = block
    slip["correlation_ok"] = block is None
    return slip


def same_game_pair_may_gate(*, n_with: int, n_without: int) -> bool:
    """True only when both graded groups clear the sample bar."""
    return int(n_with) >= SAME_GAME_PAIR_GATE_N and int(n_without) >= SAME_GAME_PAIR_GATE_N


def summarize_same_game_pair_grades(
    slips: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Hit-rate split for stamped slips that already carry a decided result.

    Expects ``same_game_pair`` and a truthy/falsey ``paid`` or ``hit`` / ``result``
    of HIT|MISS on each decided slip. Does not apply a gate.
    """
    buckets: dict[str, list[int]] = {"with": [], "without": []}
    for slip in slips:
        if not isinstance(slip, dict):
            continue
        if slip.get("decided") is False:
            continue
        paid = slip.get("paid")
        if paid is None:
            result = str(slip.get("result") or "").strip().upper()
            if result in {"HIT", "WIN", "WON", "PAID"}:
                paid = True
            elif result in {"MISS", "LOSS", "LOST"}:
                paid = False
            elif slip.get("hit") is not None:
                paid = bool(slip.get("hit"))
            else:
                continue
        key = "with" if slip.get("same_game_pair") else "without"
        buckets[key].append(1 if paid else 0)

    def _stat(vals: list[int]) -> dict[str, Any]:
        n = len(vals)
        hits = sum(vals)
        return {
            "n": n,
            "hits": hits,
            "hr": (hits / n) if n else None,
        }

    with_s = _stat(buckets["with"])
    without_s = _stat(buckets["without"])
    return {
        "same_game_pair": with_s,
        "no_same_game_pair": without_s,
        "gate_n": SAME_GAME_PAIR_GATE_N,
        "may_gate": same_game_pair_may_gate(
            n_with=int(with_s["n"]), n_without=int(without_s["n"])
        ),
    }


def pick_legs_without_hard_blocks(
    candidates: list[dict[str, Any]],
    n: int,
    *,
    used_players: set[str] | None = None,
) -> list[dict[str, Any]] | None:
    """Take up to n legs: no duplicate player, final set not all-same-team.

    Same-game legs are allowed. Returns None when n legs cannot be formed.
    """
    if n < 1:
        return []
    used = set(used_players or ())
    chosen: list[dict[str, Any]] = []
    for leg in candidates:
        pk = player_key(leg)
        if pk and pk in used:
            continue
        trial = chosen + [leg]
        if has_duplicate_player(trial):
            continue
        if len(trial) == n and all_same_team(trial):
            continue
        chosen.append(leg)
        if pk:
            used.add(pk)
        if len(chosen) == n:
            return chosen
    return None
