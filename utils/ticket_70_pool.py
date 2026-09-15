"""Ticket-leg pools: Goblin-70 + Standard O/U under one recency/D gate.

Ticket gate (all sports except tennis/MLB/soccer): directional L5 = 5, L10 >= 8, and
directional D (OVER Weak|Below Avg; UNDER Elite|Above Avg; Avg/unknown fail;
MLB hitter Ks invert). Tennis Goblin OVER uses utils.tennis_keep_gates
(Games Won Std−Goblin>=4 or L10>=8+(lefty|2nd-won>=45.7); Total Games
L5>=4+L10>=8 or L10>=8+lefty; no D). Tennis Standard UNDER Aces / Double
Faults is faded after serve-actual backfill (missing→0 had inflated ~90%).
Standard OVER Games Won uses L10>=8+(gap|2nd-won). Other tennis Standard
stays off. Soccer uses utils.soccer_keep_gates
(Shots L5=5+L10>=8; SOT L5>=4+Off; Saves L5>=4+D). Golf has no opponent D,
so it uses L5 = 5 + L10 >= 8 without D.

MLB Goblin OVER uses the locked keep props 1–10 in utils.mlb_keep_gates
(not the global L5=5 card). H+R+RBI / Hits / TB: BA>=.275 + L5=5 + Opp
pitch Weak|Below. Hitter Ks stay on the list keep-gate but are hard-faded
from Goblin-70 tickets (52% n=23 under the stack). MLB Standard stays off.
Cover floor still applies on other sports. WNBA Goblin PRA needs Off
(usage HIGH/STAR or minutes HIGH) AND prop_tier S/A on top of
L5=5+L10>=8+D (Off-only residual was 69% n=310). Premium YOLO sleeve:
WNBA FGA / reb+ast / threes / threes_att. No Demons, no shadow. NFLP stays
on its own playing-time track. NFL regular-season Standard tickets are
UNDER-heavy (rec yards / receptions / sacks / kick pts / pass+rush yards)
until a Goblin sample exists; Standard OVER stays off except rush yards.

List gate remains L5 >= 4 (D badge-only) except MLB and soccer, which use the
same keep gates as Goblin-70. Tennis list uses tennis keep gates. Live
PrizePicks fill must use live_board_fill_ok:
same player + same canon prop + same line as a goblin_70_eligible row.
Do not take L5=4 Gold from the printed list, and do not swap Total Games
Won onto a Total Games ticket row.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(_ROOT / "scripts"))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import prop_hit_tiers as T  # noqa: E402
from utils.defense_tiers import d_aligned  # noqa: E402
from utils.mlb_keep_gates import mlb_goblin_keep_eligible  # noqa: E402
from utils.nfl_keep_gates import (  # noqa: E402
    NFL_GOBLIN_TICKETS_ENABLED,
    nfl_goblin_ticket_eligible,
    nfl_standard_ticket_eligible,
    nfl_ticket_priority,
)
from utils.soccer_keep_gates import soccer_ticket_gate_passes  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    tennis_skip_cover_floor,
    tennis_standard_games_won_over_eligible,
    tennis_standard_under_serve_eligible,
    tennis_ticket_gate_passes,
)

ACTIVE = T.ACTIVE
TICKET_SPORTS = frozenset(
    {
        "WNBA",
        "WNBA1Q",
        "WNBA1H",
        "MLB",
        "Soccer",
        "Tennis",
        "NBA",
        "NBA1Q",
        "NBA1H",
        "NFL",
        "CFB",
        "CFB1H",
        "CBB",
        "WCBB",
        "Golf",
        "NHL",
    }
)
TENNIS_SPORTS = frozenset({"Tennis", "TENNIS"})
GOLF_SPORTS = frozenset({"Golf", "GOLF", "PGA"})
TIER_RANK = T.TIER_RANK
canon_prop = T.canon_prop
cover_clears_floor = T.cover_clears_floor
is_shadow = T.is_shadow
norm_sport = T.norm_sport

# Historical hit rates used as ticket p, not board hit_rate.
P_GOBLIN_COVER = 0.734
P_GOBLIN_SA = 0.774
P_GOBLIN_L5EQ5 = 0.763
P_GOBLIN_STRICT = 0.745
P_WNBA_STEALS_UNDER = 0.727
P_WNBA_ASSISTS_UNDER = 0.631
P_MLB_HRRBI_UNDER_L5EQ5 = 0.660
P_WNBA_COMBO_OVER = 0.644
P_STANDARD_GATE = 0.70
P_TENNIS_SERVE_UNDER = 0.90
P_TENNIS_GAMES_WON_OVER = 0.75

PITCHER_PROPS = frozenset(
    {
        "pitcher_ks",
        "hits allowed",
        "walks allowed",
        "earned runs allowed",
        "pitches thrown",
        "pitching outs",
    }
)
WNBA_COMBO_OVER = frozenset({"pra", "pts+ast", "points_combo", "points (combo)"})
HRRBI = frozenset({"hits+runs+rbis", "h+r+rbi"})

STD_KIND_ORDER = (
    "tennis_serve_under",
    "tennis_games_won_over",
    "wnba_steals_under",
    "wnba_combo_over",
    "wnba_assists_under",
    "mlb_hrrbi_under_l5eq5",
)


def _pick(r: dict[str, Any]) -> str:
    return str(r.get("pick_type") or "").strip()


def _side(r: dict[str, Any]) -> str:
    return str(r.get("side") or "").strip().upper()


def _sport(r: dict[str, Any]) -> str:
    return norm_sport(str(r.get("sport") or ""))


def _prop(r: dict[str, Any]) -> str:
    return canon_prop(_sport(r), str(r.get("prop") or ""))


def _l5(r: dict[str, Any]) -> float | None:
    side = _side(r)
    v = r.get("l5_over") if side == "OVER" else r.get("l5_under")
    if v is None:
        v = r.get("l5")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def directional_l5(r: dict[str, Any]) -> float | None:
    return _l5(r)


def _l10(r: dict[str, Any]) -> float | None:
    side = _side(r)
    keys = (
        ("l10_over", "last10_over", "line_hits_over_10", "l10")
        if side == "OVER"
        else ("l10_under", "last10_under", "line_hits_under_10", "l10")
    )
    for k in keys:
        v = r.get(k)
        if v is None or v == "":
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def directional_l10(r: dict[str, Any]) -> float | None:
    return _l10(r)


def skip_combo_player(r: dict[str, Any]) -> bool:
    return "+" in str(r.get("player") or "")


def _is_earned_runs(r: dict[str, Any]) -> bool:
    if _prop(r) == "earned_runs":
        return True
    raw = str(r.get("prop") or "").lower().replace("_", " ")
    return "earned run" in raw


def skip_earned_runs(r: dict[str, Any]) -> bool:
    """Ban ERA except Goblin OVER 0.5. 1.5+ is the Hughes miss (62% n=1330)."""
    if not _is_earned_runs(r):
        return False
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return True
    try:
        line = float(r.get("line"))
    except (TypeError, ValueError):
        return True
    return abs(line - 0.5) > 1e-9


def skip_era_half(r: dict[str, Any]) -> bool:
    """Backward-compatible alias. 1.5+ ERA is skipped; Goblin 0.5 is not."""
    return skip_earned_runs(r)


def skip_hitter_counting(r: dict[str, Any]) -> bool:
    """True for TB/Hits. Keep gate is BA>=.275 + L5=5 + leaky opp pitch."""
    return _prop(r) in {"total_bases", "hits"}


def _cover_gap(r: dict[str, Any]) -> float | None:
    gap = r.get("dist_l5")
    if gap is None:
        gap = r.get("cover")
    try:
        return float(gap)
    except (TypeError, ValueError):
        return None


def hitter_counting_gate(r: dict[str, Any]) -> bool:
    """Hits/TB Goblin: BA>=.275 + L5=5 + Opp pitch Weak|Below."""
    if not skip_hitter_counting(r):
        return False
    rec = dict(r)
    rec.setdefault("sport", "MLB")
    return mlb_goblin_keep_eligible(rec)


def era_half_gate(r: dict[str, Any]) -> bool:
    """Goblin ERA 0.5 + L5=5 + L10>=8 (87.9% n=149). Ban 1.5+."""
    if skip_earned_runs(r) or not _is_earned_runs(r):
        return False
    l5 = _l5(r)
    l10 = _l10(r)
    return l5 is not None and l5 >= 5 and l10 is not None and l10 >= 8


def nflp_ticket_eligible(r: dict[str, Any]) -> bool:
    """NFLP week-3 Goblin OVER for a separate /tickets group (not the 70% book).

    Kickers: L5 >= 4. Backup skill: D pass. Sit/cameo skill overs stay off.
    """
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    if _sport(r) != "NFL":
        return False
    from utils.nflp_playing_time import is_nflp, nflp_list_eligible, policy_from_row

    if not is_nflp(r.get("league")):
        return False
    d_ok = bool((r.get("checks") or {}).get("D") is True)
    policy = str(r.get("starter_policy") or "") or policy_from_row(r)
    return nflp_list_eligible(
        policy=policy,
        side="OVER",
        pick_type="Goblin",
        d_ok=d_ok,
        l5_over=r.get("l5_over"),
        l5_under=r.get("l5_under"),
    )


def nflp_std_over_eligible(r: dict[str, Any]) -> bool:
    """NFLP Standard OVER for a separate /tickets group when Goblins are absent."""
    if _pick(r) != "Standard" or _side(r) != "OVER":
        return False
    if _sport(r) != "NFL":
        return False
    from utils.nflp_playing_time import is_nflp, nflp_list_eligible, policy_from_row

    if not is_nflp(r.get("league")):
        return False
    d_ok = bool((r.get("checks") or {}).get("D") is True)
    policy = str(r.get("starter_policy") or "") or policy_from_row(r)
    return nflp_list_eligible(
        policy=policy,
        side="OVER",
        pick_type="Standard",
        d_ok=d_ok,
        l5_over=r.get("l5_over"),
        l5_under=r.get("l5_under"),
    )


def nflp_ticket_p(r: dict[str, Any]) -> float:
    from utils.nflp_playing_time import POLICY_PLAYS

    if str(r.get("starter_policy") or "") == POLICY_PLAYS:
        return 0.70
    return 0.62


def _is_tennis(sport: str) -> bool:
    return sport in TENNIS_SPORTS or str(sport or "").upper() == "TENNIS"


def _is_golf(sport: str) -> bool:
    return sport in GOLF_SPORTS or str(sport or "").upper() in {"GOLF", "PGA"}


def _is_nflp_row(r: dict[str, Any]) -> bool:
    if _sport(r) != "NFL":
        return False
    from utils.nflp_playing_time import is_nflp

    return bool(is_nflp(r.get("league")))


def _d_ok(r: dict[str, Any]) -> bool:
    checks = r.get("checks") or {}
    if checks.get("D") is True:
        return True
    if checks.get("D") is False:
        return False
    raw = r.get("def") or r.get("d") or r.get("def_tier")
    return d_aligned(_sport(r), _side(r), raw, _prop(r))


def _d_ok_over(r: dict[str, Any]) -> bool:
    """OVER-only D (kept for callers). Prefer _d_ok for both sides."""
    if _side(r) != "OVER":
        return False
    return _d_ok(r)


def wnba_pra_off_ok(r: dict[str, Any]) -> bool:
    """High usage/STAR or HIGH minutes. Missing both fails."""
    usg = str(
        r.get("usage_tier") or r.get("Usage Tier") or r.get("star_tier") or r.get("Star Tier") or ""
    ).strip().lower()
    if usg in {"high", "star"}:
        return True
    star = str(r.get("is_franchise_star") or "").strip().lower()
    if star in {"1", "true", "yes"}:
        return True
    mt = str(
        r.get("minutes_tier") or r.get("min_tier") or r.get("Min Tier") or ""
    ).strip().upper()
    return mt == "HIGH"


def wnba_pra_sa_ok(r: dict[str, Any]) -> bool:
    """Prop-tier S or A (catalog / stamped). B-tier PRA fails even with Off."""
    stamped = str(r.get("prop_tier") or "").strip().upper()
    if stamped in {"S", "A"}:
        return True
    if stamped:
        return False
    cover = r.get("cover")
    if cover is None:
        cover = r.get("dist_l5")
    info = T.assign_tier(
        sport=_sport(r),
        pick_type=_pick(r) or "Goblin",
        side=_side(r) or "OVER",
        prop=_prop(r),
        cover=cover,
        d_ok=_d_ok(r),
    )
    return str(info.get("prop_tier") or "") in {"S", "A"}


def _wnba_pra_ticket_cut(r: dict[str, Any]) -> bool:
    """Goblin OVER PRA on full-game WNBA needs Off + S/A on top of L5/L10/D."""
    if _sport(r) != "WNBA":
        return False
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    return _prop(r) == "pra"


PREMIUM_WNBA_PROPS = frozenset({"fga", "reb+ast", "threes", "threes_att"})


def ticket_gate_passes(r: dict[str, Any]) -> bool:
    """L5=5 + L10>=8 + directional D. Tennis: keep gates. Golf: L5=5 + L10>=8.

    MLB Goblin OVER uses keep props 1–10. MLB Standard never passes.
    Tennis Standard UNDER Aces/DF pass ungated; other tennis Standard never passes.
    """
    sport = _sport(r)
    if sport == "MLB":
        if _pick(r) == "Goblin" and _side(r) == "OVER":
            return mlb_goblin_keep_eligible(r)
        return False
    if _is_tennis(sport):
        return tennis_ticket_gate_passes(r)
    if sport == "Soccer":
        return soccer_ticket_gate_passes(r)
    if skip_earned_runs(r):
        return False
    if _is_earned_runs(r):
        return era_half_gate(r)
    if skip_hitter_counting(r):
        return hitter_counting_gate(r)
    if sport not in TICKET_SPORTS:
        return False
    l5 = _l5(r)
    if l5 is None or l5 < 5:
        return False
    l10 = _l10(r)
    if l10 is None or l10 < 8:
        return False
    if _is_golf(sport):
        return True
    if not _d_ok(r):
        return False
    if _wnba_pra_ticket_cut(r):
        if not wnba_pra_off_ok(r) or not wnba_pra_sa_ok(r):
            return False
    return True


def goblin_70_eligible(r: dict[str, Any]) -> bool:
    """Goblin OVER ticket gate: L5=5+L10>=8+D (tennis keep gates; golf no D).

    WNBA Goblin PRA also needs Off (high usage or HIGH minutes) AND prop_tier
    S/A. MLB uses keep props 1-10 on the list, but hitter_ks is hard-faded
    from Goblin-70 tickets. ``ml_prob`` is never a gate or sort key. A 0.99
    score cannot rescue a failed L5/L10/D/cover row; a 0.10 score cannot
    drop a clear one.
    """
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    if _is_nflp_row(r):
        return False
    if skip_combo_player(r):
        return False
    sport = _sport(r)
    prop = _prop(r)
    # NFL Goblin OVER held until tagged unique-game ledger answers keep questions.
    if sport == "NFL":
        if not NFL_GOBLIN_TICKETS_ENABLED:
            return False
        if not nfl_goblin_ticket_eligible(r):
            return False
    if sport == "MLB":
        # Keep-gate still admits hitter_ks for the printed list; tickets fade it.
        if prop == "hitter_ks":
            return False
        return mlb_goblin_keep_eligible(r)
    if skip_earned_runs(r):
        return False
    if not ticket_gate_passes(r):
        return False
    if is_shadow(sport, "Goblin OVER", prop):
        return False
    if prop == "hitter_ks":
        return False
    if _is_earned_runs(r) or skip_hitter_counting(r):
        return True
    if tennis_skip_cover_floor(r):
        return True
    gap = _cover_gap(r)
    if not cover_clears_floor(sport, gap, "OVER", prop):
        return False
    return True


def premium_goblin_eligible(r: dict[str, Any]) -> bool:
    """WNBA FGA / reb+ast / threes / threes_att that already clear Goblin-70."""
    if _sport(r) != "WNBA":
        return False
    if _prop(r) not in PREMIUM_WNBA_PROPS:
        return False
    return goblin_70_eligible(r)


def _player_fill_key(name: str) -> str:
    s = str(name or "").casefold().replace(".", " ")
    for tok in (" jr", " sr", " ii", " iii"):
        if s.endswith(tok):
            s = s[: -len(tok)]
    return " ".join(s.split())


def _line_fill_eq(a: Any, b: Any) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return False


def live_board_fill_ok(
    pool: list[dict[str, Any]],
    *,
    player: str,
    prop: str,
    line: Any,
) -> tuple[bool, str]:
    """True only if this live PP card is an exact Goblin-70 row.

    The printed list is L5>=4. Live Goblin chips can move (16.5 -> 21) or
    swap markets (Total Games -> Games Won). Neither is a ticket fill.
    """
    want_player = _player_fill_key(player)
    if not want_player:
        return False, "no_player"
    sport_hint = ""
    for row in pool:
        if _player_fill_key(str(row.get("player") or "")) == want_player:
            sport_hint = _sport(row)
            break
    want_prop = canon_prop(sport_hint or "Tennis", str(prop or ""))
    matches = [
        r
        for r in pool
        if _player_fill_key(str(r.get("player") or "")) == want_player
        and _prop(r) == want_prop
        and _line_fill_eq(r.get("line"), line)
    ]
    if not matches:
        same_player = [
            r for r in pool if _player_fill_key(str(r.get("player") or "")) == want_player
        ]
        if same_player and not any(_prop(r) == want_prop for r in same_player):
            return False, "prop_mismatch"
        if same_player and any(_prop(r) == want_prop for r in same_player):
            return False, "line_mismatch"
        return False, "not_in_pool"
    if any(goblin_70_eligible(r) for r in matches):
        return True, "ok"
    return False, "not_goblin70"


def standard_ticket_eligible(r: dict[str, Any]) -> bool:
    """Standard OVER or UNDER that clears the same L5/L10/D ticket gate."""
    if _pick(r) != "Standard":
        return False
    side = _side(r)
    if side not in {"OVER", "UNDER"}:
        return False
    if _sport(r) == "MLB":
        return False
    if _is_nflp_row(r):
        return False
    # Week 1 RS board: UNDER-heavy until a Goblin sample exists.
    if _sport(r) == "NFL" and not nfl_standard_ticket_eligible(r):
        return False
    if skip_combo_player(r) or skip_earned_runs(r) or skip_hitter_counting(r):
        return False
    if not ticket_gate_passes(r):
        return False
    # Ungated 90%+ catalog cells. Shadow tag and tennis cover floor (need 2.0)
    # would drop almost every Ace/DF under; skip both.
    if tennis_standard_under_serve_eligible(r):
        return True
    if tennis_standard_games_won_over_eligible(r):
        return True
    sport = _sport(r)
    prop = _prop(r)
    book = f"Standard {side}"
    if is_shadow(sport, book, prop):
        return False
    gap = r.get("dist_l5")
    if gap is None:
        gap = r.get("cover")
    if not cover_clears_floor(sport, gap, side, prop):
        return False
    return True


def goblin_ticket_p(_r: dict[str, Any]) -> float:
    return P_GOBLIN_STRICT


def standard_flex_kind(r: dict[str, Any]) -> str | None:
    """Allowlist for Flex-only Standard fill. None = do not ticket."""
    if _pick(r) != "Standard":
        return None
    sport = _sport(r)
    if sport not in ACTIVE:
        return None
    if sport == "MLB":
        return None
    if skip_combo_player(r):
        return None
    if tennis_standard_under_serve_eligible(r):
        return "tennis_serve_under"
    if tennis_standard_games_won_over_eligible(r):
        return "tennis_games_won_over"
    side = _side(r)
    prop = _prop(r)
    book = f"Standard {side}"
    if is_shadow(sport, book, prop):
        return None
    l5 = _l5(r)
    if l5 is None:
        return None
    if sport == "WNBA" and side == "UNDER" and prop == "steals" and l5 >= 4:
        return "wnba_steals_under"
    if sport == "WNBA" and side == "OVER" and prop in WNBA_COMBO_OVER and l5 >= 4:
        return "wnba_combo_over"
    if sport == "WNBA" and side == "UNDER" and prop == "assists" and l5 >= 4:
        return "wnba_assists_under"
    if sport == "MLB" and side == "UNDER" and prop in HRRBI and l5 == 5:
        return "mlb_hrrbi_under_l5eq5"
    return None


def standard_ticket_p(kind: str) -> float:
    return {
        "gate": P_STANDARD_GATE,
        "tennis_serve_under": P_TENNIS_SERVE_UNDER,
        "tennis_games_won_over": P_TENNIS_GAMES_WON_OVER,
        "wnba_steals_under": P_WNBA_STEALS_UNDER,
        "wnba_combo_over": P_WNBA_COMBO_OVER,
        "wnba_assists_under": P_WNBA_ASSISTS_UNDER,
        "mlb_hrrbi_under_l5eq5": P_MLB_HRRBI_UNDER_L5EQ5,
    }.get(kind, P_STANDARD_GATE)


def is_pitcher_prop(r: dict[str, Any]) -> bool:
    return _prop(r) in PITCHER_PROPS


def goblin_sort_key(r: dict[str, Any]) -> tuple:
    """Pack order: Diamond/Platinum first, then Gold, then S–D tier, L5, cover.

    Not ml_prob. 70% gate is eligibility; badge is the extra Gold-stack
    filter (season HR and/or L10 already required for G70).
    Tennis games_won fills last: one tier bucket worse than stamped tier.
    """
    tier = str(r.get("prop_tier") or "")
    tier_rank = TIER_RANK.get(tier, 9)
    # Soft fade: treat Games Won as one letter worse so Total Games / other
    # keep props pack ahead of it on the short card.
    if _is_tennis(_sport(r)) and _prop(r) == "games_won":
        tier_rank = min(tier_rank + 1, 5)
    l5 = int(_l5(r) or 0)
    cover = r.get("cover")
    try:
        cov = -float(cover)
    except (TypeError, ValueError):
        cov = 0.0
    promo = str(r.get("promo") or r.get("badge") or "")
    if promo in {"Diamond", "Platinum"}:
        badge_band = 0
    elif promo == "Gold":
        badge_band = 1
    else:
        badge_band = 2
    promo_rank = {"Diamond": 0, "Platinum": 1, "Gold": 2, "Silver": 3, "Bronze": 4}.get(
        promo, 9
    )
    return (
        badge_band,
        tier_rank,
        0 if l5 >= 5 else 1,
        promo_rank,
        cov,
        str(r.get("player") or ""),
    )


def premium_goblin_sort_key(r: dict[str, Any]) -> tuple:
    """YOLO pack order: premium WNBA sleeve first, then normal goblin_sort_key.

    Premium = WNBA FGA / reb+ast / threes / threes_att (79–83% under the stack).
    Leading 0 puts them above Diamond/Platinum on the YOLO path only.
    """
    prem = 0 if _prop(r) in PREMIUM_WNBA_PROPS and _sport(r) == "WNBA" else 1
    return (prem,) + goblin_sort_key(r)


def ticket_excluded_from_winrate(t: dict[str, Any], group_name: str = "") -> bool:
    """YOLO Power 4/5/6 (and any flagged slip) stay off ticket win-rate / grade_history."""
    if t.get("exclude_from_winrate") or t.get("winrate_exclude"):
        return True
    track = str(t.get("ticket_track") or "").strip().lower()
    if track in {"goblin70_yolo", "yolo"}:
        return True
    recipe = str(t.get("core_recipe") or "").strip().lower()
    if "yolo" in recipe:
        return True
    name = str(
        group_name
        or t.get("web_group_name")
        or t.get("web_group")
        or t.get("_group_name")
        or ""
    )
    if "YOLO" in name.upper():
        return True
    tid = str(t.get("id") or t.get("ticket_id") or "").upper()
    if tid.startswith("Y") and ("POWER" in str(t.get("product") or t.get("play") or "").upper()):
        n = int(t.get("n_legs") or 0)
        if n >= 4:
            return True
    return False


def standard_sort_key(r: dict[str, Any]) -> tuple:
    kind = r.get("std_kind") or standard_flex_kind(r) or ""
    try:
        ki = STD_KIND_ORDER.index(kind)
    except ValueError:
        ki = 99
    l5 = int(_l5(r) or 0)
    # NFL Week-1 priority: rec U → receptions U → longest rec U → sacks U →
    # kick U → rush bidirectional; pass yards gated out.
    nfl_pri = nfl_ticket_priority(r) if _sport(r) == "NFL" else 50
    return (ki, nfl_pri, -l5, str(r.get("player") or ""))
