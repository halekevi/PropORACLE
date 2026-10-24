"""End-to-end style check: nhl_soccer_grader prop keys resolve MLB combo VOIDs."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from utils.combo_actuals import prop_lookup_keys, sum_multiplayer_combo_actual  # noqa: E402


def _norm_prop(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def test_mlb_pitcher_ks_combo_resolves_from_solo_arms():
    MLB_PROP_ALIASES = {
        "pitcherstrikeouts": ["pitcherstrikeouts", "strikeouts"],
        "strikeouts": ["strikeouts", "pitcherstrikeouts"],
    }
    # Mimic actuals CSV (solo arms only — no A+B row, no *combo prop key).
    store = {
        ("tarik skubal", "pitcherstrikeouts"): 7.0,
        ("nick lodolo", "pitcherstrikeouts"): 2.0,
    }
    sprop = _norm_prop("Pitcher Strikeouts (Combo)")
    assert sprop == "pitcherstrikeoutscombo"

    def lookup(pname: str, tm: str, pk: str):
        key = (pname.lower(), pk)
        if key in store:
            return store[key]
        # Accept any alias key the helper tries
        for k in prop_lookup_keys(pk, extra_aliases=MLB_PROP_ALIASES):
            if (pname.lower(), k) in store:
                return store[(pname.lower(), k)]
        return None

    total = sum_multiplayer_combo_actual(
        "Tarik Skubal + Nick Lodolo",
        "LAD/CIN",
        sprop,
        lookup,
        extra_aliases=MLB_PROP_ALIASES,
    )
    assert total == 9.0


def test_nhl_sog_combo_resolves_from_solo_arms():
    store = {
        ("player a", "shotsongoal"): 4.0,
        ("player b", "shotsongoal"): 1.0,
    }
    sprop = _norm_prop("Shots On Goal (Combo)")
    assert sprop == "shotsongoalcombo"

    def lookup(pname: str, tm: str, pk: str):
        return store.get((pname.lower(), pk))

    total = sum_multiplayer_combo_actual(
        "Player A + Player B",
        "BOS/TOR",
        sprop,
        lookup,
    )
    assert total == 5.0
