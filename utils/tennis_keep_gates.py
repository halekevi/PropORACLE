"""Tennis keep props and per-prop gates.

Graded catalog + Standard-Line join (Games Won Goblin discount window
2026-07-13 to 2026-09-14) and Sackmann factor join (restated 2026-09-15):

- Goblin OVER Games Won keep if ANY of:
  - Standard Line − Goblin line >= 4 (4–6 band 82.6% 246/298; 6–8 band
    87.9% 29/33; combined 83.1% 275/331). No L5/L10/D.
  - L10 >= 8 and (vs lefty OR L5 2nd-serve points won % >= 45.7)
    (lefty 86.3% 88/102; 2nd-won 83.6% 371/444; union 82.6% 414/501).
  Cover floor skipped — mean L5 gap in the juiced bands is under the
  3.3 games_won unit.
- Goblin OVER Total Games keep if ANY of:
  - L5>=4 and L10>=8 (78.7% 48/61)
  - L10>=8 and vs lefty (80.0% 68/85; union with L5 path 79.3% 107/135)
  No D.
- Standard UNDER Aces: ungated 90.6% (87/96).
- Standard UNDER Double Faults: ungated 90.0% (45/50).
- Standard OVER Games Won keep if ANY of:
  - L10>=8 and L5 avg−line >=5 (75.0% 12/16, thin)
  - L10>=8 and L5 2nd-won% >=45.7 (75.0% 30/40)
- Goblin OVER / Standard OVER Aces/DF and Demons: fade.

Same gates for the printed list and Goblin-70 / Standard Flex.

Row fields for the new cuts (filled in tennis step6):
  opp_lefty / vs_lefty / opponent_hand
  l5_second_won_pct / second_won_l5
"""
from __future__ import annotations

from typing import Any

from utils.prop_norm import canon_prop as _canon

KEEP_PROPS = frozenset({"games_won", "match_total_games"})
SERVE_UNDER_PROPS = frozenset({"aces", "double_faults"})
GAMES_WON = "games_won"
MATCH_TOTAL = "match_total_games"
# Juiced Goblin Games Won: 4–6 under Standard 82.6%; 6–8 under 87.9%.
GAMES_WON_STD_DISCOUNT_MIN = 4.0
# Thin Standard OVER Games Won keep (75% n=16).
GAMES_WON_STD_OVER_L10_MIN = 8.0
GAMES_WON_STD_OVER_L5_GAP_MIN = 5.0
# L5 mean 2nd-serve points won % (Sackmann). Median split on Goblin GW L10>=8.
GAMES_WON_L5_SECOND_WON_MIN = 45.7
GAMES_WON_FACTOR_L10_MIN = 8.0


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
    return str(r.get("pick_type") or "").strip()


def _prop(r: dict[str, Any]) -> str:
    return _canon("Tennis", str(r.get("prop") or r.get("prop_type") or ""))


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


def _standard_line(r: dict[str, Any]) -> float | None:
    for k in ("standard_line", "Standard Line", "std_line"):
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _l5_avg(r: dict[str, Any]) -> float | None:
    for k in ("stat_last5_avg", "Last 5 Avg", "last5_avg", "l5_avg"):
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def tennis_l5_avg_gap(r: dict[str, Any]) -> float | None:
    """Signed L5 avg − line. Play-side OVER gap is this value; UNDER is −this."""
    for k in ("dist_l5", "Dist_L5"):
        v = _num(r.get(k))
        if v is not None:
            return v
    avg = _l5_avg(r)
    line = _num(r.get("line"))
    if avg is None or line is None:
        return None
    return avg - line


def tennis_std_discount(r: dict[str, Any]) -> float | None:
    """Play-side Standard − posted line. OVER: std − line; UNDER: line − std."""
    line = _num(r.get("line"))
    std = _standard_line(r)
    if line is None or std is None:
        return None
    if _side(r) == "UNDER":
        return line - std
    return std - line


def tennis_opp_lefty(r: dict[str, Any]) -> bool | None:
    """True if today's (or last-match) opponent is left-handed. None if unknown."""
    for k in ("opp_lefty", "vs_lefty", "opponent_lefty"):
        raw = r.get(k)
        if raw is None or raw == "":
            continue
        if isinstance(raw, bool):
            return raw
        s = str(raw).strip().upper()
        if s in {"1", "Y", "YES", "TRUE", "L", "LEFT", "LEFTY"}:
            return True
        if s in {"0", "N", "NO", "FALSE", "R", "RIGHT"}:
            return False
    for k in ("opponent_hand", "opp_hand", "Opp Hand"):
        s = str(r.get(k) or "").strip().upper()
        if s == "L":
            return True
        if s in {"R", "U"}:
            return False
    return None


def tennis_l5_second_won_pct(r: dict[str, Any]) -> float | None:
    for k in ("l5_second_won_pct", "second_won_l5", "L5 2nd Won %", "l5_2nd_won_pct"):
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def tennis_games_won_factor_keep(r: dict[str, Any]) -> bool:
    """L10>=8 and (vs lefty OR L5 2nd-won% >= 45.7)."""
    l10 = _l10(r)
    if l10 is None or l10 < GAMES_WON_FACTOR_L10_MIN:
        return False
    if tennis_opp_lefty(r) is True:
        return True
    sw = tennis_l5_second_won_pct(r)
    return sw is not None and sw >= GAMES_WON_L5_SECOND_WON_MIN


def tennis_keep_prop(r: dict[str, Any]) -> bool:
    return _prop(r) in KEEP_PROPS


def tennis_goblin_keep_eligible(r: dict[str, Any]) -> bool:
    """Goblin OVER Games Won / Total Games keep (gap, L5/L10, lefty, 2nd-won)."""
    if _pick(r) != "Goblin":
        return False
    if _side(r) != "OVER":
        return False
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in {"TENNIS", "Tennis", ""}:
        return False
    prop = _prop(r)
    if prop not in KEEP_PROPS:
        return False
    if prop == GAMES_WON:
        disc = tennis_std_discount(r)
        if disc is not None and disc >= GAMES_WON_STD_DISCOUNT_MIN:
            return True
        return tennis_games_won_factor_keep(r)
    if prop == MATCH_TOTAL:
        l5 = _l5(r)
        l10 = _l10(r)
        if l10 is not None and l10 >= 8 and l5 is not None and l5 >= 4:
            return True
        return l10 is not None and l10 >= 8 and tennis_opp_lefty(r) is True
    return False


def tennis_standard_under_serve_eligible(r: dict[str, Any]) -> bool:
    """Standard UNDER Aces / Double Faults. Ungated 90%+ at n>=40."""
    if _pick(r) != "Standard":
        return False
    if _side(r) != "UNDER":
        return False
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in {"TENNIS", "Tennis", ""}:
        return False
    return _prop(r) in SERVE_UNDER_PROPS


def tennis_standard_games_won_over_eligible(r: dict[str, Any]) -> bool:
    """Standard OVER Games Won: L10>=8 + (L5gap>=5 OR L5 2nd-won%>=45.7)."""
    if _pick(r) != "Standard":
        return False
    if _side(r) != "OVER":
        return False
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in {"TENNIS", "Tennis", ""}:
        return False
    if _prop(r) != GAMES_WON:
        return False
    l10 = _l10(r)
    if l10 is None or l10 < GAMES_WON_STD_OVER_L10_MIN:
        return False
    gap = tennis_l5_avg_gap(r)
    if gap is not None and gap >= GAMES_WON_STD_OVER_L5_GAP_MIN:
        return True
    sw = tennis_l5_second_won_pct(r)
    return sw is not None and sw >= GAMES_WON_L5_SECOND_WON_MIN


def tennis_list_eligible(r: dict[str, Any]) -> bool:
    return (
        tennis_goblin_keep_eligible(r)
        or tennis_standard_under_serve_eligible(r)
        or tennis_standard_games_won_over_eligible(r)
    )


def tennis_ticket_gate_passes(r: dict[str, Any]) -> bool:
    """Goblin-70 + Standard Flex keep props."""
    return tennis_list_eligible(r)


def tennis_skip_cover_floor(r: dict[str, Any]) -> bool:
    """Games Won keep paths; cover unit 3.3 would wipe the juiced Goblin band."""
    return _prop(r) == GAMES_WON and tennis_goblin_keep_eligible(r)
