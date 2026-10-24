"""Multi-player PrizePicks combo helpers (``A + B`` player labels).

WNBA/NBA often pre-materialize duo rows in actuals (``append_nba_duo_combo_actual_rows``).
MLB/NHL/Soccer usually only have solo arm rows — graders must split the label, look up
each arm on the *base* prop (strip trailing ``combo``), and sum.

NBA's remaining ~36–46% combo VOID rate is a separate matching leak (nicknames /
suffixes), not this missing-wire failure.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any


def split_combo_players(player: object) -> list[str]:
    """Split ``A + B`` / ``A & B`` style PrizePicks combo player labels."""
    raw = str(player or "").strip()
    if not raw:
        return []
    for sep in (" + ", "+", " & ", "&", " / ", "/", " vs ", " and "):
        if sep in raw:
            parts = [p.strip() for p in raw.split(sep) if p.strip()]
            if len(parts) >= 2:
                return parts
    return [raw]


def split_combo_teams(team: object, n_players: int) -> list[str]:
    """Align ``STL/LAA`` (or single team) with combo arms."""
    if n_players <= 1:
        tok = str(team or "").strip().upper()
        return [tok] if n_players == 1 else []
    raw = str(team or "").strip()
    if not raw or raw.lower() in ("nan", "none", "-", "—"):
        return [""] * n_players
    if "/" in raw:
        parts = [p.strip().upper() for p in raw.split("/") if p.strip()]
        if len(parts) >= n_players:
            return parts[:n_players]
        if len(parts) == 1:
            return parts * n_players
        while len(parts) < n_players:
            parts.append(parts[-1] if parts else "")
        return parts[:n_players]
    tok = raw.upper()
    return [tok] * n_players


def strip_combo_prop_token(prop: object) -> str:
    """
    Remove a trailing combo marker from a normalized prop key or display label.

    Examples:
      pitcherstrikeoutscombo -> pitcherstrikeouts
      pitcher strikeouts (combo) -> pitcher strikeouts
      passes_attempted_combo -> passes_attempted
    """
    s = str(prop or "").strip()
    if not s:
        return ""
    # Display / spaced forms first
    s2 = re.sub(r"\(\s*combo\s*\)", "", s, flags=re.I).strip()
    s2 = re.sub(r"[\s_]+combo$", "", s2, flags=re.I).strip()
    if s2 != s:
        return s2
    # Compact alphanumeric keys (nhl_soccer_grader._norm_prop style)
    compact = re.sub(r"[^a-z0-9]", "", s.lower())
    if compact.endswith("combo") and len(compact) > 5:
        base = compact[: -len("combo")]
        # If input was already compact, return compact base; else prefer spaced strip above
        if re.fullmatch(r"[a-z0-9]+", s.lower()):
            return base
        return s2 or base
    return s2 or s


def prop_lookup_keys(prop: object, extra_aliases: dict[str, list[str]] | None = None) -> list[str]:
    """Ordered unique keys to try when matching an actuals prop to a slate prop."""
    raw = str(prop or "").strip()
    if not raw:
        return []
    keys: list[str] = []
    seen: set[str] = set()

    def add(k: object) -> None:
        t = str(k or "").strip()
        if t and t not in seen:
            seen.add(t)
            keys.append(t)

    add(raw)
    base = strip_combo_prop_token(raw)
    add(base)
    compact = re.sub(r"[^a-z0-9]", "", raw.lower())
    add(compact)
    compact_base = strip_combo_prop_token(compact)
    add(compact_base)

    aliases = extra_aliases or {}
    for seed in list(keys):
        for alt in aliases.get(seed, []):
            add(alt)
        seed_c = re.sub(r"[^a-z0-9]", "", seed.lower())
        for alt in aliases.get(seed_c, []):
            add(alt)
    return keys


def sum_multiplayer_combo_actual(
    player: object,
    team: object,
    prop: object,
    lookup_arm: Callable[[str, str, str], Any],
    *,
    extra_aliases: dict[str, list[str]] | None = None,
) -> float | None:
    """
    Sum per-arm actuals for a multi-player combo label.

    ``lookup_arm(player_name, team_hint, prop_key)`` should return a numeric actual
    or None/NaN when missing. Tries each key from ``prop_lookup_keys``.
    """
    parts = split_combo_players(player)
    if len(parts) < 2:
        return None
    teams = split_combo_teams(team, len(parts))
    keys = prop_lookup_keys(prop, extra_aliases=extra_aliases)
    if not keys:
        return None

    vals: list[float] = []
    for arm_name, arm_team in zip(parts, teams):
        found: float | None = None
        for pk in keys:
            got = lookup_arm(arm_name, arm_team, pk)
            if got is None:
                continue
            try:
                import math

                fv = float(got)
                if math.isnan(fv):
                    continue
            except (TypeError, ValueError):
                continue
            found = fv
            break
        if found is None:
            return None
        vals.append(found)
    return float(sum(vals)) if vals else None
