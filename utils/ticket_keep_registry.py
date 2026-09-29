"""Which sports have a ticket rule, and whether it replaces L5/L10/D.

A sport with no entry is off. NHL, NBA, CBB, and WCBB are out of season,
so they are not registered here. Holds (thin competitions such as a World
Cup) are applied by the caller before this lookup.
"""
from __future__ import annotations

from typing import Any, Callable

from utils.cfb_keep_gates import cfb_ticket_fade
from utils.golf_keep_gates import golf_ticket_eligible
from utils.mlb_keep_gates import mlb_goblin_keep_eligible
from utils.nfl_keep_gates import nfl_goblin_ticket_eligible, nfl_standard_ticket_eligible
from utils.soccer_keep_gates import soccer_ticket_gate_passes
from utils.tennis_keep_gates import tennis_ticket_gate_passes

RuleFn = Callable[[dict[str, Any]], bool]


def nfl_keep_allows(r: dict[str, Any]) -> bool:
    """NFL add-rule extra: Goblin uses its flag; Standard must be on the keep list.

    The default L5/L10/D stack still runs. This only decides which NFL props
    are allowed to reach it. Goblin wave-1 is on when NFL_GOBLIN_TICKETS_ENABLED
    is True (pass-side Goblin still staged).
    is false.
    """
    pick = str(r.get("pick_type") or "").strip().lower()
    if "goblin" in pick:
        return nfl_goblin_ticket_eligible(r)
    if "standard" in pick:
        return nfl_standard_ticket_eligible(r)
    return False


# mode "replace": the keep function is the whole gate. Default L5/L10/D does not run.
# mode "add": default L5=5 + L10>=8 + directional D runs, then the keep function
# may fade a prop. A keep function of None means the default stack alone.
SPORT_RULES: dict[str, tuple[str, RuleFn | None]] = {
    # Per-prop gates. MLB Standard is off inside the function.
    "MLB": ("replace", mlb_goblin_keep_eligible),
    # Games Won / Total Games. Replaces L5/L10/D.
    "TENNIS": ("replace", tennis_ticket_gate_passes),
    # Shots / SOT / Saves. List may enter SOT/Saves at L5>=4; tickets need L5=5.
    "SOCCER": ("replace", soccer_ticket_gate_passes),
    # L5=5 + L10>=8, no opponent D.
    "GOLF": ("replace", golf_ticket_eligible),
    "PGA": ("replace", golf_ticket_eligible),
    # Keep-list props, then the default stack (some Unders skip L5=5).
    "NFL": ("add", nfl_keep_allows),
    # Default stack, then fade player TDs, rec-yards Under, rush att, kick pts.
    "CFB": ("add", lambda r: not cfb_ticket_fade(r)),
    "CFB1H": ("add", lambda r: not cfb_ticket_fade(r)),
    # Default stack. WNBA line cuts (assists, rebounds, PRA, cover) stay in
    # goblin_70_eligible on top of this.
    "WNBA": ("add", None),
    "WNBA1Q": ("add", None),
    "WNBA1H": ("add", None),
}


def rule_for(sport: object) -> tuple[str, RuleFn | None] | None:
    key = str(sport or "").strip().upper()
    return SPORT_RULES.get(key)
