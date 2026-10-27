"""Shared injury status gate for lists, tickets, and slip_check.

Phase 1 ticket rule: OUT (and IR / PUP / NFI / suspended / inactive) hard-drop.
Questionable / GTD / Doubtful / Probable are flag-only.
Nothing here promotes teammate or opponent effects onto tickets.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

# Canonical display statuses for injuries_latest.csv
CANON_STATUSES = (
    "OUT",
    "Doubtful",
    "Questionable",
    "Probable",
    "GTD",
)

_OUT_EXACT = frozenset(
    {
        "OUT",
        "O",
        "IR",
        "INACTIVE",
        "SUSPENDED",
        "SUSP",
        "PUP",
        "NFI",
        "OUT FOR SEASON",
        "OUT INDEFINITELY",
    }
)
_DOUBTFUL = frozenset({"DOUBTFUL", "D"})
_QUESTIONABLE = frozenset({"QUESTIONABLE", "Q"})
_GTD = frozenset(
    {
        "GTD",
        "DAY-TO-DAY",
        "DAY TO DAY",
        "DAY_TO_DAY",
        "DTD",
        "GAME TIME DECISION",
        "GAMETIME DECISION",
    }
)
_PROBABLE = frozenset({"PROBABLE", "P", "AVAILABLE"})

STALE_HOURS_DEFAULT = 6.0


def _raw_status(row_or_raw: object) -> str:
    if isinstance(row_or_raw, dict):
        for key in (
            "inj_status",
            "player_injury_status",
            "injury_status",
            "Injury Status",
            "Injury",
            "injury",
            "status",
            "player_status",
        ):
            v = row_or_raw.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
        return ""
    return str(row_or_raw or "").strip()


def normalize_inj_status(raw: object) -> str:
    """Map free-text to OUT / Doubtful / Questionable / Probable / GTD / ''."""
    s = _raw_status(raw).upper()
    if not s or s in {"NAN", "NONE", "HEALTHY", "ACTIVE", "-", "—"}:
        return ""
    # Exact / token hits first (avoid "TIMEOUT"-style false positives later).
    token = s.replace("_", " ").strip()
    if token in _OUT_EXACT or token.startswith("OUT ") or token.startswith("OUT/"):
        return "OUT"
    if token in _DOUBTFUL:
        return "Doubtful"
    if token in _GTD:
        return "GTD"
    if token in _QUESTIONABLE:
        return "Questionable"
    if token in _PROBABLE:
        return "Probable"
    # NFL practice-report game-status style
    if any(k in token for k in ("IR", "PUP", "NFI")) and "QUESTION" not in token:
        return "OUT"
    if "SUSP" in token:
        return "OUT"
    if "DOUBT" in token:
        return "Doubtful"
    if "QUESTION" in token:
        return "Questionable"
    if "PROBABLE" in token or token == "AVAILABLE":
        return "Probable"
    if "DAY" in token and "TO" in token:
        return "GTD"
    if "GTD" in token or "GAME TIME" in token or "GAMETIME" in token:
        return "GTD"
    if token == "OUT" or token.startswith("OUT"):
        return "OUT"
    return token.title() if token else ""


_LATEST_OUT_KEYS: set[tuple[str, str, str]] | None = None


def _load_latest_out_keys() -> set[tuple[str, str, str]]:
    """(sport, fold_player, team_canon) for OUT rows in injuries_latest.csv."""
    global _LATEST_OUT_KEYS
    if _LATEST_OUT_KEYS is not None:
        return _LATEST_OUT_KEYS
    keys: set[tuple[str, str, str]] = set()
    try:
        import sys
        from pathlib import Path

        _root = Path(__file__).resolve().parents[1]
        _scripts = _root / "scripts"
        for _p in (_root, _scripts):
            if str(_p) not in sys.path:
                sys.path.insert(0, str(_p))
        from espn_injuries import canon_team_abbr
        from player_name_norm import fold_player_name
        from utils.injury_collect import load_latest_rows

        for r in load_latest_rows():
            if normalize_inj_status(r.get("status")) != "OUT":
                continue
            sport = str(r.get("sport") or "").strip().upper()
            player = fold_player_name(r.get("player", ""))
            team = canon_team_abbr(sport, r.get("team", ""))
            if player:
                keys.add((sport, player, team))
                keys.add((sport, player, ""))  # name-only fallback key
    except Exception:
        keys = set()
    _LATEST_OUT_KEYS = keys
    return keys


def reset_injury_out_cache() -> None:
    """Test helper — clear injuries_latest OUT index."""
    global _LATEST_OUT_KEYS
    _LATEST_OUT_KEYS = None


def is_injury_out(row_or_raw: object) -> bool:
    """Hard ticket / list drop: player cannot play.

    Uses the row's injury columns first; if blank, falls back to
    ``data/injuries/injuries_latest.csv`` so Goblin-70 / mixer still drop
    OUT when step8 was built before the injury collect.
    """
    if normalize_inj_status(row_or_raw) == "OUT":
        return True
    if not isinstance(row_or_raw, dict):
        return False
    # Already classified non-OUT on the row — trust the board.
    raw = _raw_status(row_or_raw)
    if raw and normalize_inj_status(raw):
        return False
    try:
        keys = _load_latest_out_keys()
        if not keys:
            return False
        import sys
        from pathlib import Path

        _root = Path(__file__).resolve().parents[1]
        _scripts = _root / "scripts"
        for _p in (_root, _scripts):
            if str(_p) not in sys.path:
                sys.path.insert(0, str(_p))
        from espn_injuries import canon_team_abbr
        from player_name_norm import fold_player_name

        sport = str(
            row_or_raw.get("sport") or row_or_raw.get("Sport") or row_or_raw.get("league") or ""
        ).strip().upper()
        player = fold_player_name(
            row_or_raw.get("player")
            or row_or_raw.get("player_name")
            or row_or_raw.get("Player")
            or ""
        )
        team = canon_team_abbr(
            sport,
            row_or_raw.get("team") or row_or_raw.get("team_abbr") or row_or_raw.get("Team") or "",
        )
        if not player:
            return False
        return (sport, player, team) in keys or (sport, player, "") in keys
    except Exception:
        return False


def is_injury_soft_flag(row_or_raw: object) -> bool:
    """Questionable / GTD / Doubtful — warn only, still ticketable."""
    return normalize_inj_status(row_or_raw) in {"Questionable", "GTD", "Doubtful"}


def injury_captured_at(row: dict[str, Any] | None) -> str:
    if not isinstance(row, dict):
        return ""
    for key in ("inj_captured_at", "injury_captured_at", "captured_at", "fetched_at"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def parse_captured_at(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def injury_snapshot_stale(
    captured_at: object,
    *,
    now: datetime | None = None,
    max_hours: float = STALE_HOURS_DEFAULT,
) -> bool:
    """True when missing timestamp or older than max_hours (fail closed warn)."""
    dt = parse_captured_at(captured_at)
    if dt is None:
        return True
    ref = now or datetime.now(timezone.utc)
    if ref.tzinfo is None:
        ref = ref.replace(tzinfo=timezone.utc)
    return (ref - dt) > timedelta(hours=float(max_hours))
