"""Soccer keep props and per-prop best gates.

Graded Std+Goblin combo catalog (listed L5/L10/D/Off stacks):

- Shots: L5=5 and L10>=8 (76.2% 16/21). D is thinner (4/5) — not required.
- Shots On Target (sog): L5>=4 and Off (67.6% 46/68). Off = STARTER or
  HIGH_VOL or team top-3. D does not beat that cell at n>=20.
- Goalie Saves: L5>=4 + own D (OWN_DEF_TIER: Weak|Below OVER, Elite|Above
  UNDER). Own-team DEF is attached in soccer step3. Opponent DEF_TIER is
  the wrong polarity (leaky opp D cuts shots faced) — never fall back to
  it. Optional OPP_OFF_TIER / opp attack (Elite|Above for OVER; Weak|Below
  for UNDER) tightens when filled; missing or Avg still pass.
  Watch (2026-09-24): rebuilt-history own-leaky OVER was weak (5/14) while
  legacy opp-D blanks looked strong — likely join contamination + possible
  goals-against capping of saves. Do not revert; recheck with
  ``scripts/_soccer_best_gate_od_alignment.py --forward-from 2026-09-15
  --own-d-only`` once forward OWN_DEF n is 30-40+.
- Goals / G+A / assists / fouls / volume props: best gated cell is 20-41%.
  Fade. Demons fade. Combos fade.

Same gates for the printed list and Goblin-70 / Standard Flex.
Mixer hygiene still allows these three props without the L5/L10/D/Off
cut (combined seed stays L5>=4 until that mixer is retuned).

Thin-history competitions (World Cup, Olympics, …) are blocked via
``utils.competition_history`` when ``league`` has too little prior depth —
not a hardcoded WORLDCUP allowlist.
"""
from __future__ import annotations

from typing import Any

from utils.competition_history import row_competition_ok
from utils.defense_tiers import d_aligned, normalize_def_tier_label
from utils.prop_norm import canon_prop as _canon

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
    """Required: OVER Weak|Below; UNDER Elite|Above. Own D only — never opp DEF_TIER."""
    return d_aligned("Soccer", _side(r), _own_def_tier(r), "saves")


def _saves_opp_off_ok(r: dict[str, Any]) -> bool:
    """Optional: OVER wants strong opp attack; UNDER wants weak. Missing = pass."""
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
    l5 = _l5(r)
    if l5 is None or l5 < 4:
        return False
    if not _saves_own_d_ok(r):
        return False
    return _saves_opp_off_ok(r)


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
    return soccer_keep_eligible(r)


def soccer_ticket_gate_passes(r: dict[str, Any]) -> bool:
    """Goblin-70 + Standard Flex. Same per-prop gates as the list."""
    return soccer_keep_eligible(r)
