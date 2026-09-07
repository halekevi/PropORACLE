"""Which grade_history tracks count as the playable /tickets card for Income."""
from __future__ import annotations

from typing import Any

G70_START = "2026-08-26"

CARD_TRACKS = frozenset(
    {
        "",
        "legacy",
        "unknown",
        "graded_main",
        "goblin70",
        "goblin_only_3leg",
    }
)


def norm_track(raw: object) -> str:
    return str(raw or "").strip().lower()


def is_card_track(raw: object) -> bool:
    """MAIN / Goblin-70 / unlabeled early days. Not shadows, long parlays, or YOLO."""
    t = norm_track(raw)
    if "yolo" in t:
        return False
    if t in {"long_parlay", "high_prob_std_gob", "goblin70_yolo"}:
        return False
    if t.startswith("strong_") or t.startswith("winrate_"):
        return False
    return t in CARD_TRACKS


def row_in_scope(row: dict[str, Any], *, scope: str) -> bool:
    """scope: card | g70 | all."""
    s = str(scope or "card").strip().lower()
    if s == "all":
        return True
    if not is_card_track(row.get("track")):
        return False
    if s == "g70":
        return str(row.get("date") or "")[:10] >= G70_START
    return True
