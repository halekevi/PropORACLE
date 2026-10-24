"""CFB keep/fade from graded tape (not a new layer).

All season: step5b pads the face window from this player's last-year
boxes until it holds 8 games. ``l5_sample_n`` / ``games_used`` stay
current-season. Tickets use the filled face L5=5. L10>=8 is not a CFB
gate. Directional L8>=6 on a full 8-game window is a bonus
(``cfb_l8_bonus``), not a block and not a new price. Diamond/Platinum
stay capped on PRIOR_* in the ranker.

1. Pass Yards Goblin OVER — drop D. 74% ungated; D-fail hits 90%.
   Tickets need L5=5 + cover. L10>=8 is not a CFB gate. The face window
   is this season, then this player's last year, out to 8 games. Directional
   L8>=6 on a full 8-game window is a bonus, not a block. List stays L5>=4;
   badge D is N/A via the same cfb_pass_yards_goblin_skip_d (not a Silver miss).
2. Player TDs — fade from the L5=5 ticket stack. 0.5-line; stack is
   scored-5-straight, not yards (0/2, 0/1). L5>=4 list is unchanged.
3. Standard Rec Yards UNDER — fade from the cover-stack ticket path.
   L5=5 is 0/5 against a 41% base. List L5>=4 is unchanged.
4. Standard Rush Yards UNDER — keep L5=5+D+cover (71%, 2/2). L8>=6 is a
   bonus. Do not lump with Rec Yards UNDER. L10>=8 is not required.
5. Rush Attempts / Kicking Points Goblin OVER — off the printed list
   and tickets (33% / 20% ungated).

D paired controls (do not re-derive independently):
  Pass Yards Goblin OVER = D hurts (skip D).
  Rush Yards Goblin OVER = D helps (84% at L5=5+D, 16/19; keep D).

Script vs D (observe, not a gate). Full tape = Week 0 + Week 1 + 9/12
joined to that date's step8 Projected Margin. Week 0 step8 has no margin
column — those decided rows do not feed these cuts. 9/19 is a permanent
hole (no step8, line_history last CFB archive 9/11, PrizePicks lines gone).
  Pass Yards D+RH-in-blowout fade: Week 1 cell is the 9/05 trio only
  (2/3 unique). 9/12 had run-heavy blow-fav (Becht, Stockton, both D-
  HIT) and zero D+ rows — the veto intersection was not re-tested, not
  reconfirmed. Run-heavy blow-fav (any D) is 8/9 unique. Do not revive.
  Rush Yards: D still does not absorb script. Unique = (date, player),
  majority on mixed lines. Close = Week 1 12/15 + Week 2 2/2 = 14/17
  (14/17 is not a restatement of 12/15). Blow-fav = Week 1 7/15
  (Turbo Richard mixed majority MISS; last-line used to read 8/15) +
  Week 2 7/13 = 14/28. n=28 unique is still not a gate. Blow-dog thin.

Same rules for CFB1H (same prop families). Mixer seed order is untouched.
"""

from __future__ import annotations

from typing import Any

from utils.prop_norm import canon_prop as _canon

CFB_SPORTS = frozenset({"CFB", "CFB1H"})
PASS_YDS = "pass_yds"
PLAYER_TD = "player_td"
REC_YDS = "rec_yds"
RUSH_YDS = "rush_yds"
LIST_FADE_GOBLIN_OVER = frozenset({"rush_att", "kick_pts"})
# 6 of 8 on the assembled window (this season, then last year). Bonus only.
CFB_L8_BONUS_MIN = 6


def _sport(r: dict[str, Any]) -> str:
    return str(r.get("sport") or "").strip().upper()


def _is_cfb(r: dict[str, Any]) -> bool:
    return _sport(r) in CFB_SPORTS


def _side(r: dict[str, Any]) -> str:
    for k in ("side", "Direction", "bet_direction", "recommended_side", "dir"):
        if k in r and r.get(k) not in (None, ""):
            s = str(r.get(k) or "").strip().upper()
            if s in {"OVER", "UNDER"}:
                return s
    return ""


def _pick(r: dict[str, Any]) -> str:
    p = str(r.get("pick_type") or r.get("Pick Type") or "").strip()
    low = p.lower()
    if "goblin" in low:
        return "Goblin"
    if "demon" in low:
        return "Demon"
    if "standard" in low:
        return "Standard"
    return p.title() if p else "Standard"


def _prop(r: dict[str, Any]) -> str:
    raw = r.get("prop") or r.get("prop_type") or r.get("Prop") or ""
    sport = _sport(r) or "CFB"
    # CFB1H uses the same fold table as CFB.
    return _canon("CFB" if sport in CFB_SPORTS else sport, str(raw))


def cfb_list_eligible(r: dict[str, Any]) -> bool:
    """False only for Goblin OVER rush_att / kick_pts. Else leave L5>=4 alone."""
    if not _is_cfb(r):
        return True
    if _pick(r) == "Demon":
        return False
    if _pick(r) == "Goblin" and _side(r) == "OVER" and _prop(r) in LIST_FADE_GOBLIN_OVER:
        return False
    return True


def cfb_ticket_fade(r: dict[str, Any]) -> bool:
    """True = do not ticket. Does not change the printed L5>=4 list except via list fade."""
    if not _is_cfb(r):
        return False
    prop = _prop(r)
    pick = _pick(r)
    side = _side(r)
    if prop == PLAYER_TD:
        return True
    if pick == "Standard" and side == "UNDER" and prop == REC_YDS:
        return True
    if pick == "Goblin" and side == "OVER" and prop in LIST_FADE_GOBLIN_OVER:
        return True
    return False


def cfb_pass_yards_goblin_skip_d(r: dict[str, Any]) -> bool:
    """Pass Yards Goblin OVER skips directional D.

    Negative control vs Rush Yards Goblin OVER, which keeps D (84% 16/19
    at L5=5+D). Do not copy this skip onto rush_yds.
    """
    if not _is_cfb(r):
        return False
    if _prop(r) == RUSH_YDS:
        return False
    return _pick(r) == "Goblin" and _side(r) == "OVER" and _prop(r) == PASS_YDS


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    if n != n:
        return None
    return n


def _l8_hits(r: dict[str, Any]) -> float | None:
    if _side(r) == "UNDER":
        keys = ("l8_under", "last8_under", "line_hits_under_8")
    else:
        keys = ("l8_over", "last8_over", "line_hits_over_8")
    for key in keys:
        n = _num(r.get(key))
        if n is not None:
            return n
    return None


def cfb_l8_bonus(r: dict[str, Any]) -> bool:
    """True when the assembled 8-game window has 6+ directional hits.

    Does not decide ticket eligibility and is not a priced cell.
    A window shorter than 8 games, even after last year, has no bonus.
    """
    if not _is_cfb(r):
        return False
    window = _num(r.get("l8_window_n"))
    if window is None or window < 8:
        return False
    hits = _l8_hits(r)
    return hits is not None and hits >= CFB_L8_BONUS_MIN
