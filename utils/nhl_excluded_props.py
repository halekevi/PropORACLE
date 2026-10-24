"""NHL props kept out of cells, gates, and tickets."""
from __future__ import annotations

import json
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_AGREEMENT = _ROOT / "data" / "reports" / "nhl_prop_agreement.json"

# Faceoffs were graded from faceoff win percentage. They stay out after the
# box regrade. The agreement report adds any other prop under 98%.
ALWAYS_EXCLUDED = frozenset({"faceoffs_won"})

# 2026-10-19. The shots cache for Logan Cooley (8483431, season 20262027)
# is 0, 1, 0, 1, 0. The live NHL log and the PrizePicks card are 1, 3, 3, 0, 0.
# Step5 copied that cache onto a NO_DATA row. Every NHL five-game row stays
# out of the pools until that attach is fixed and each row is re-checked.
NHL_SERIES_HELD = True


def nhl_series_held(row: dict | None) -> bool:
    if not NHL_SERIES_HELD:
        return False
    return str((row or {}).get("sport") or "").strip().upper() == "NHL"


def nhl_cell_excluded_props() -> frozenset[str]:
    extra: set[str] = set()
    try:
        payload = json.loads(_AGREEMENT.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        payload = {}
    for prop in payload.get("excluded_props") or []:
        extra.add(str(prop))
    return frozenset(ALWAYS_EXCLUDED | extra)
