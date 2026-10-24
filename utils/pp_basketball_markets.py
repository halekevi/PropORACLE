"""Basketball PrizePicks markets that live on the pregame (in_game=false) board.

FG/FT/2PT made and attempted are not on the live Popular feed. Every WNBA / NBA /
CBB / WCBB step1 must fetch in_game=false (then merge in_game=true).
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

# Canonical family -> substrings on PrizePicks prop_type (lowercased).
SHOOTING_FAMILIES: dict[str, tuple[str, ...]] = {
    "fg_attempted": ("fg attempted", "field goals attempted"),
    "fg_made": ("fg made", "field goals made", "fgm"),
    "threes_attempted": (
        "3-pt attempted",
        "3pt attempted",
        "three pointers attempted",
        "3pa",
        "threes attempted",
    ),
    "threes_made": (
        "3-pt made",
        "3pt made",
        "three pointers made",
        "3pm",
        "threes made",
        "3pts made",
    ),
    "ft_attempted": ("free throws attempted",),
    "ft_made": ("free throws made",),
    "two_attempted": (
        "two pointers attempted",
        "2-pt attempted",
        "2pt attempted",
        "2pa",
        "2pts attempted",
        "2 pts attempted",
        "two pts attempted",
    ),
    "two_made": (
        "two pointers made",
        "2-pt made",
        "2pt made",
        "2pm",
        "2pts made",
        "2 pts made",
        "two pts made",
        "2-points made",
    ),
}

# Pregame first. Live board second. Never fetch live-only for full-game hoops.
BASKETBALL_IN_GAME_FLAGS: tuple[str, ...] = ("false", "true")

FULL_GAME_HOOPS: frozenset[str] = frozenset({"WNBA", "NBA", "CBB", "WCBB"})
# PrizePicks league_id → full-game hoops tag (period boards are omitted).
HOOPS_SPORT_BY_LEAGUE_ID: dict[str, str] = {
    "3": "WNBA",
    "7": "NBA",
    "237": "NBA",  # NBAP preseason — same full-game shooting markets
    "20": "CBB",
    "176": "WCBB",
}


def is_full_game_hoops(sport: str) -> bool:
    return str(sport or "").strip().upper() in FULL_GAME_HOOPS


def hoops_sport_for_league(league_id: Any, fallback: str = "NBA") -> str:
    lid = str(league_id or "").strip()
    return HOOPS_SPORT_BY_LEAGUE_ID.get(lid, str(fallback or "NBA").strip() or "NBA")


def _prop_text(v: Any) -> str:
    return str(v or "").strip().lower()


def shooting_family_counts(props: Iterable[Any]) -> dict[str, int]:
    counts = {k: 0 for k in SHOOTING_FAMILIES}
    for raw in props:
        p = _prop_text(raw)
        if not p:
            continue
        for fam, needles in SHOOTING_FAMILIES.items():
            if any(n in p for n in needles):
                counts[fam] += 1
    return counts


def missing_core_shooting(counts: Mapping[str, int]) -> list[str]:
    """2PT is often a 4-row market; FG/FT/3s absence with a big board is the miss."""
    core = (
        "fg_attempted",
        "fg_made",
        "threes_attempted",
        "threes_made",
        "ft_attempted",
        "ft_made",
    )
    return [k for k in core if int(counts.get(k) or 0) == 0]


def log_shooting_coverage(
    props: Iterable[Any],
    *,
    sport: str,
    n_points: int | None = None,
) -> dict[str, int]:
    counts = shooting_family_counts(props)
    bits = " ".join(f"{k}={v}" for k, v in counts.items())
    print(f"  [shooting {sport}] {bits}")
    pts = n_points
    if pts is None:
        pts = sum(1 for p in props if "point" in _prop_text(p) and "quarter" not in _prop_text(p))
    missing = missing_core_shooting(counts)
    if is_full_game_hoops(sport) and missing and (pts or 0) >= 20:
        print(
            f"  [WARN] {sport} board has {pts} points rows but missing {', '.join(missing)}. "
            "Those markets are on in_game=false (pregame). Re-fetch both flags; do not keep a live-only pull."
        )
    return counts
