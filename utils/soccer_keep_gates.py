"""Soccer keep props and per-prop best gates.

**FACE AUDIT 2026-10-24 (``SOCCER_KEEP_GATE_FACES_VERIFIED = False``).**
Soccer graded rows carry default labels (zero Goblin/Demon) on 35 dates
2026-05-09..06-16 and laddered Standard (36%) on 11 dates 03-26..04-11
(``utils.face_ladder``; 28,301 decided legs). The Goblin OVER cells below
(Shots 16/21, SOT 46/68, Saves 205/343 on Sep 1–27) were taken from
Goblin-labeled rows, so the default-label dates could not inflate them by
inclusion — but any soccer **Standard** cell from Mar 26–Jun 16 is label-only
(Standard UNDER Shots 136/195 = 69.7% on those dates vs 183/344 = 53.2% on
real faces), and the Shots / SOT L5 cells could not be re-measured from the
leg ledger (soccer stores hits in ``last5_over``; the ledger ``l5_over`` is
blank for most soccer rows — verified L5=5 Shots Goblin is 7/11). Re-measure
the three keep cells from a face-verified step8 join before quoting them as
prices again. Report: ``data/reports/face_contamination_2026-10-24.md``,
``data/reports/keep_gate_face_split.json``.

Graded Std+Goblin combo catalog (listed L5/L10/D/Off stacks):

- Shots: L5=5 and L10>=8 (76.2% 16/21). D is thinner (4/5) — not required.
- Shots On Target (sog): L5>=4 and Off (67.6% 46/68). Off = STARTER or
  HIGH_VOL or team top-3. D does not beat that cell at n>=20.
- Goalie Saves: off tickets. Goblin 1.5 is 205/343 (59.8%, band 55–65%),
  under the ~77% a 3-leg needs at 2.2x. The L5>=4 cell 21/26 (80.8%) is
  not a price. Own-defense stays off: within a posted line the tier split
  is about one standard error, and shots-on-target faced does not follow
  the goals-conceded tier (Elite 4.57, Weak 5.43, Avg 5.91). Opponent
  attack is retired. List entry is L5>=4 plus settlement (past boxes for
  the competition; a posted keeper must be the starter).
- Goals / G+A / assists / fouls / volume props: best gated cell is 20-41%.
  Fade. Demons fade. Combos fade.

List / pool entry uses the per-prop gates above (SOT may clear at L5>=4;
Saves may list at L5>=4 with settlement and do not ticket). Goblin-70 /
Standard Flex tickets still require directional **L5 = 5** on Shots and
SOT. Saves are off every ticket path, including the mixer.

Thin-history competitions (World Cup, Olympics, …) are blocked via
``utils.competition_history`` when ``league`` has too little prior depth —
not a hardcoded WORLDCUP allowlist.
"""
from __future__ import annotations

from typing import Any

from utils.competition_history import row_competition_ok
from utils.defense_tiers import d_aligned, normalize_def_tier_label
from utils.prop_norm import canon_prop as _canon
from utils.soccer_board_coverage import game_on_covered_board, saves_leg_settleable

# False until Shots / SOT / Saves keep cells are re-measured on face-verified
# sport-dates (see module docstring). Read-only; does not change ticket output.
SOCCER_KEEP_GATE_FACES_VERIFIED = False
SOCCER_UNVERIFIED_FACE_WINDOWS = (("2026-03-26", "2026-04-11"), ("2026-05-09", "2026-06-16"))

SHOTS = "shots"
SOG = "sog"
SAVES = "saves"
KEEP_PROPS = frozenset({SHOTS, SOG, SAVES})
_STRONG_OFF = frozenset({"Elite", "Above Avg"})
_WEAK_OFF = frozenset({"Weak", "Below Avg"})


def _num(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _side(r: dict[str, Any]) -> str:
    s = str(r.get("side") or r.get("direction") or r.get("over_under") or "").strip().upper()
    if s in {"OVER", "UNDER"}:
        return s
    d = str(r.get("final_bet_direction") or "").strip().upper()
    return d if d in {"OVER", "UNDER"} else ""


def _pick(r: dict[str, Any]) -> str:
    p = str(r.get("pick_type") or "").strip()
    low = p.lower()
    if "goblin" in low:
        return "Goblin"
    if "demon" in low:
        return "Demon"
    if "standard" in low:
        return "Standard"
    return p


def _prop(r: dict[str, Any]) -> str:
    raw = r.get("prop") or r.get("prop_type") or r.get("Prop") or ""
    return _canon("Soccer", str(raw))


def _l5(r: dict[str, Any]) -> float | None:
    if _side(r) == "UNDER":
        keys = ("l5_under", "last5_under", "L5 Under", "l5")
    else:
        keys = ("l5_over", "last5_over", "L5 Over", "l5")
    for k in keys:
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _l10(r: dict[str, Any]) -> float | None:
    if _side(r) == "UNDER":
        keys = ("l10_under", "last10_under", "line_hits_under_10", "L10 Under", "l10")
    else:
        keys = ("l10_over", "last10_over", "line_hits_over_10", "L10 Over", "l10")
    for k in keys:
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _sport_ok(r: dict[str, Any]) -> bool:
    s = str(r.get("sport") or "").strip().upper()
    return s in {"SOCCER", "SOC", ""} or s.startswith("SOCCER")


def soccer_keep_prop(r: dict[str, Any]) -> bool:
    return _prop(r) in KEEP_PROPS


def _off_ok(r: dict[str, Any]) -> bool:
    starter = str(
        r.get("starter_tier") or r.get("Starter Tier") or ""
    ).strip().upper()
    if starter == "STARTER":
        return True
    vol = str(
        r.get("shot_volume")
        or r.get("Shot Volume")
        or r.get("shot_role")
        or r.get("Shot Role")
        or ""
    ).strip().upper()
    if vol == "HIGH_VOL":
        return True
    if r.get("off_top3") in (True, 1, "1", "true", "True"):
        return True
    rank = _num(r.get("team_top3_rank") or r.get("Team Top3 Rank") or r.get("rank_on_team"))
    return rank is not None and rank <= 3


def _opp_off_tier(r: dict[str, Any]) -> str:
    for k in (
        "opp_off_tier",
        "OPP_OFF_TIER",
        "Opp Off Tier",
        "OFF_TIER",
        "GOALS_OFF_TIER",
    ):
        label = normalize_def_tier_label(r.get(k))
        if label:
            return label
    return ""


def _own_def_tier(r: dict[str, Any]) -> str:
    for k in ("own_def_tier", "OWN_DEF_TIER", "Own Def Tier"):
        label = normalize_def_tier_label(r.get(k))
        if label:
            return label
    return ""


def _saves_own_d_ok(r: dict[str, Any]) -> bool:
    """Retired from the gate. OVER Weak|Below; UNDER Elite|Above. Own team only."""
    return d_aligned("Soccer", _side(r), _own_def_tier(r), "saves")


def _saves_opp_off_ok(r: dict[str, Any]) -> bool:
    """Retired. Filled on 40/630 Sep keeper-days and the bands overlapped."""
    tier = _opp_off_tier(r)
    if not tier or tier in ("N/A", "Avg"):
        return True
    side = _side(r)
    if side == "OVER":
        return tier in _STRONG_OFF
    if side == "UNDER":
        return tier in _WEAK_OFF
    return False


def _shots_gate(r: dict[str, Any]) -> bool:
    l5 = _l5(r)
    l10 = _l10(r)
    return l5 is not None and l5 >= 5 and l10 is not None and l10 >= 8


def _sot_gate(r: dict[str, Any]) -> bool:
    l5 = _l5(r)
    return l5 is not None and l5 >= 4 and _off_ok(r)


def _saves_gate(r: dict[str, Any]) -> bool:
    """List only. Own-defense and opponent attack are off.

    Settlement still requires past boxes for the competition and, when a
    lineup is posted, a starting keeper.
    """
    l5 = _l5(r)
    if l5 is None or l5 < 4:
        return False
    return saves_leg_settleable(r)


def _prop_gate(r: dict[str, Any]) -> bool:
    prop = _prop(r)
    if prop == SHOTS:
        return _shots_gate(r)
    if prop == SOG:
        return _sot_gate(r)
    if prop == SAVES:
        return _saves_gate(r)
    return False


def soccer_keep_eligible(r: dict[str, Any]) -> bool:
    """Goblin OVER or Standard O/U on Shots / SOT / Saves with that prop's best gate."""
    if not _sport_ok(r):
        return False
    if not row_competition_ok(r):
        return False
    # Named matches must appear on a covered scoreboard. Rows with no matchup
    # (unit fixtures, partial joins) keep the prop gates only.
    if game_on_covered_board(r) is False:
        return False
    pick = _pick(r)
    if pick == "Demon":
        return False
    side = _side(r)
    if pick == "Goblin" and side != "OVER":
        return False
    if pick not in {"Goblin", "Standard"}:
        return False
    if side not in {"OVER", "UNDER"}:
        return False
    if not soccer_keep_prop(r):
        return False
    return _prop_gate(r)


def soccer_list_eligible(r: dict[str, Any]) -> bool:
    """Printed best-props list / pool. SOT and Saves may enter at L5>=4."""
    return soccer_keep_eligible(r)


def soccer_ticket_gate_passes(r: dict[str, Any]) -> bool:
    """Goblin-70 + Standard Flex.

    Shots / SOT clear their list gate, then directional L5=5. Saves never
    ticket: own-defense has the wrong sign on the Sep 1–27 boxes.
    """
    if _prop(r) == SAVES:
        return False
    if not soccer_keep_eligible(r):
        return False
    l5 = _l5(r)
    return l5 is not None and l5 >= 5
