"""Load config/active_sports.json — SSOT for in-season boards vs off-season hide."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_PATH = _REPO_ROOT / "config" / "active_sports.json"


@lru_cache(maxsize=1)
def load_active_sports_config() -> dict[str, Any]:
    if not _CONFIG_PATH.is_file():
        return {"active": [], "inactive": [], "board_only_not_wired": []}
    with _CONFIG_PATH.open(encoding="utf-8") as f:
        raw = json.load(f)
    return raw if isinstance(raw, dict) else {"active": [], "inactive": [], "board_only_not_wired": []}


def _entries(section: str) -> list[dict[str, Any]]:
    cfg = load_active_sports_config()
    rows = cfg.get(section) or []
    return [r for r in rows if isinstance(r, dict) and str(r.get("key") or "").strip()]


def active_entries(*, cards_only: bool = False) -> list[dict[str, Any]]:
    rows = _entries("active")
    if cards_only:
        return [r for r in rows if r.get("card", True)]
    return rows


def inactive_entries(*, cards_only: bool = False) -> list[dict[str, Any]]:
    rows = _entries("inactive")
    if cards_only:
        return [r for r in rows if r.get("card", True)]
    return rows


def active_keys(*, cards_only: bool = False, upper: bool = True) -> list[str]:
    out: list[str] = []
    for r in active_entries(cards_only=cards_only):
        k = str(r.get("key") or "").strip()
        if not k:
            continue
        out.append(k.upper() if upper else k.lower())
    return out


def inactive_keys(*, cards_only: bool = False, upper: bool = True) -> list[str]:
    out: list[str] = []
    for r in inactive_entries(cards_only=cards_only):
        k = str(r.get("key") or "").strip()
        if not k:
            continue
        out.append(k.upper() if upper else k.lower())
    return out


def league_id_for(key: str) -> str | None:
    want = str(key or "").strip().upper()
    for r in active_entries() + inactive_entries():
        if str(r.get("key") or "").strip().upper() == want:
            lid = str(r.get("league_id") or "").strip()
            return lid or None
    return None


def fetch_boards() -> list[dict[str, Any]]:
    """Active boards that should be fetched (includes soccer league children)."""
    return active_entries(cards_only=False)


def card_sport_ids() -> list[str]:
    """Lowercase sport ids for home Prop Explorer cards (active + inactive with card:true)."""
    ids: list[str] = []
    for r in active_entries(cards_only=True) + inactive_entries(cards_only=True):
        ids.append(str(r.get("key") or "").strip().lower())
    return ids
