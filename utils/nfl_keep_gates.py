"""NFL keep gates from Week 1–2 2026 unique-game ledger.

Week 1+2 ledger uses **unique player-games** (see ``utils.nfl_prop_gate_ledger``).
Standard UNDER keep: receptions (base-rate, skip L5=5) / longest reception /
sacks / tackles_ast / kick pts / receiving yards (base-rate, skip L5=5) /
pass volume UNDER with soft unit gates (Pass D + line-inflation).
**Tackles_ast Def Axis = opp rush offense** (``opp_rush_off_rank``), not pass D —
see ``Sports/NFL/data/nfl_keep_fade.md``. Soft W1+W2 polarity is flat/anti;
do not soft-gate yet.

Soft-gated pass UNDER fail **closed** when ``opp_pass_d_tier`` (or rank) is
missing — missing context never tickets.

**Watch (not ticketable) until Week 3 gated volume:**
  - rushing_yards OVER — 40% ungated; L5=5 only 2/3; soft Elite OL+Weak Rush D
  - passing_yards / passing_tds Standard OVER — L5=5 is 1/1; need n≥10 at L5=5
  - rush_attempts / rushing_yards / longest_rush UNDER (existing watches)

Fantasy score is off entirely (Standard UNDER + Goblin OVER).
Standard OVER keep (live): punts / YPC / longest_completion (+ soft).

O/D unit ranks are soft ticket conditions only where listed in
``NFL_SOFT_UNIT_TICKET_GATES`` — elsewhere badges/display only.

Ticket priority (UNDER-heavy, then soft-gated OVER):
  1–6 classic UNDER keep → 7–11 soft pass UNDER → 12–14 soft Standard OVER
  (punts / YPC / longest_completion). Everything else gated out.

**NFL Goblin OVER is live** (``NFL_GOBLIN_TICKETS_ENABLED = True``) for
wave-1 only (fg_made / receiving_yards / longest_reception / rushing_yards).
Pass-side stays in ``NFL_GOBLIN_OVER_KEEP_PASS_LATER``. Week-3 binary
``nfl_goblin_wave1_flip_go`` remains the audit check for those four chips.

Active monitors (not allowlisted):
  - rush_attempts UNDER — 3/3 at L5=5; watch.
  - rushing_yards UNDER — 5/6 at L5=5; watch cell:
    NOT Elite Rush Off + NOT Elite RB + Elite Rush D (`opp_rush_d_tier`) 9/9.
  - rushing_yards OVER — watch; gate on Elite OL + Weak Rush D after W3 volume.
  - passing_yards / passing_tds Standard OVER — watch; reopen at L5=5 n≥10.
  - Goblin receptions / rush_attempts — watch.
  - longest_rush UNDER — 18/29 base 62%; Elite Rush D 11/14=79%.
  - receiving UNDER stack (soft): Elite Pass D primary; best
    NOT Elite Pass Off + Elite Pass D (receptions 41/44=93%; rec yds 44/55=80%);
    WOPR low + Elite Pass D on receptions 42/44=96%. Secondary alone is noise.
  - **Receptions UNDER Option C (pack only):** keep full allowlist (base ~75%
    n=88). ``standard_sort_key`` bumps legs with WOPR low + opp Pass D
    Elite|Above ahead of other receptions UNDER at equal L5. Not a gate.
    Revisit Option A harden only after ``opp_pass_d_tier`` is the ticket D
    axis and Week 3+ holds ~96% at larger n.
  - receiving OVER stack (soft): Weak Pass D primary; Fast pace + Weak Pass D
    on rec yds 14/23=61%. Weak Sec alone hurts receptions OVER.
  - sacks_taken UNDER — soft-split by ``qb_mobility_tier`` + opp ``dl_pass_tier``
    Weak (4/5) / own Elite pass block (6/9). Soft only.
  - rush_tds / rec_tds — soft-split by ``own_rz_*`` / ``player_rz_*``. Soft only;
    player_touchdowns OVER still dead (~14%).
  - completion_percentage UNDER — Elite DL pass lifts to 67% (8/12); still weak.
  - Fantasy score — off; WOPR high lifts Goblin but do not re-allowlist.

Goblin rates must use unique-game dedup. Extreme-low Goblin OVER + L5=5 alone
is not an edge.
"""

from __future__ import annotations

from typing import Any

# Wave-1 Goblin OVER tickets on (fg_made / receiving_yards / longest_reception /
# rushing_yards). Pass-side still blocked via NFL_GOBLIN_OVER_KEEP_PASS_LATER.
# Audit with ``nfl_goblin_wave1_flip_go`` — do NOT use overall Goblin HR.
NFL_GOBLIN_TICKETS_ENABLED = True

# Binary Week-3 Goblin flag flip (wave-1 chips only):
#   GO  = every prop in NFL_GOBLIN_OVER_KEEP_FLIP has unique-game Goblin OVER
#         HR >= NFL_GOBLIN_WAVE1_FLIP_MIN_HR with n >= NFL_GOBLIN_WAVE1_FLIP_MIN_N
#   NO-GO = any wave-1 chip below HR, or n < min (insufficient) → leave flag False
# Not a judgment call after the ledger prints — read the four cells and flip or don't.
NFL_GOBLIN_WAVE1_FLIP_MIN_HR = 0.75
NFL_GOBLIN_WAVE1_FLIP_MIN_N = 5

# Weeks 1–4 G70 / list L5: fill the 5-game window from current RS, then
# current preseason, then prior RS, then prior preseason. Env override:
# PROPORACLE_ALLOW_CROSS_SEASON_L5=0. Does not print READY and does not
# flip NFL_WIDER_WINDOW_TICKET_L5_ENABLED.
ALLOW_CROSS_SEASON_L5 = True

# Week 2 2026 decision (explicit — do not quietly work around the Sep-1 season cut):
# step8 current-season L5 is honestly thin (often 0/1 / THIN_*). That empty/thin
# answer is correct, same posture as Tennis Standard going to zero after backfill.
# step5 may PRIOR_SEASON_FILL display L5 in weeks 1–3; tickets still use
# nfl_current_season_l5_ok (current n>=5). Do NOT flip
# NFL_WIDER_WINDOW_TICKET_L5_ENABLED to manufacture READY. Sit-list /
# n=1 hand-checks live outside this module;
# any path that bypasses the hand-checked list bypasses that judgment too.
# Flip only after an explicit product decision, not because tonight's pool is thin.
NFL_WIDER_WINDOW_TICKET_L5_ENABLED = False

# Construction sources allowed to mark an NFL slip READY tonight.
NFL_HANDCHECK_TICKET_SOURCES = frozenset({"handcheck", "hand_check", "n1_handcheck"})


class NflWiderWindowTicketBlocked(RuntimeError):
    """Raised when ticket construction tries to use padded/wider-window L5."""


class NflTicketSourceBlocked(RuntimeError):
    """Raised when an NFL slip would be READY without a hand-check source tag."""


def assert_nfl_ticket_l5_source(source: str) -> None:
    """Loud refuse for the unguarded wider-window door into NFL ticket L5.

    Call before any NFL slip build that would compute directional L5 from
    ``nfl_boxscore_cache`` (or any non-step8 season-cut window) for eligibility.
    """
    src = str(source or "").strip().lower().replace("-", "_").replace(" ", "_")
    if src in {
        "wider_window",
        "wider_window_l5",
        "boxscore_cache",
        "last_season",
        "preseason",
        "padded_l5",
        "fallback_l5",
        "cache_l5",
    }:
        if not NFL_WIDER_WINDOW_TICKET_L5_ENABLED:
            raise NflWiderWindowTicketBlocked(
                f"NFL_WIDER_WINDOW_TICKET_BLOCKED source={source!r}. "
                "Season-cut step8 thin/empty is the honest answer; do not rebuild "
                "ticket L5 from boxscore cache. Use the hand-checked list only. "
                "Set NFL_WIDER_WINDOW_TICKET_L5_ENABLED only after an explicit decision."
            )


def assert_nfl_ready_source(source: str) -> None:
    """NFL slips may not print READY unless tagged as hand-check construction."""
    src = str(source or "").strip().lower().replace("-", "_").replace(" ", "_")
    if src in NFL_HANDCHECK_TICKET_SOURCES:
        return
    raise NflTicketSourceBlocked(
        f"NFL_TICKET_SOURCE_BLOCKED source={source!r}. "
        "READY requires ticket_source in NFL_HANDCHECK_TICKET_SOURCES "
        f"({sorted(NFL_HANDCHECK_TICKET_SOURCES)}). "
        "Algorithmic / wider-window / keep-packer fills are not submit-ready."
    )

# Ticket allowlist + pack order (lower = earlier).
NFL_TICKET_PRIORITY: tuple[tuple[str, str], ...] = (
    ("receptions", "UNDER"),  # base-rate; skips L5=5
    ("longest_reception", "UNDER"),
    ("sacks", "UNDER"),
    ("tackles_ast", "UNDER"),
    ("kicking_points", "UNDER"),
    ("receiving_yards", "UNDER"),  # base-rate; skips L5=5
    # Soft-gated pass UNDER (W1+W2 ≥70% with unit stack; fail-closed if missing)
    ("passing_yards", "UNDER"),
    ("pass_attempts", "UNDER"),
    ("pass_completions", "UNDER"),
    ("completion_percentage", "UNDER"),
    ("passing_tds", "UNDER"),
    # Soft-gated Standard OVER (thin L5=5 OVER chips moved to watch)
    ("punts", "OVER"),
    ("rush_yards_per_carry", "OVER"),
    ("longest_completion", "OVER"),
)

NFL_STD_UNDER_KEEP = frozenset(
    {
        "receptions",
        "longest_reception",
        "sacks",
        "tackles_ast",
        "kicking_points",
        "receiving_yards",
        "passing_yards",
        "pass_attempts",
        "pass_completions",
        "completion_percentage",
        "passing_tds",
    }
)

# L5=5 *hurts* these UNDER chips vs open base — ticket_gate skips L5=5 only.
# Still require L10>=8 + directional D when present.
NFL_STD_UNDER_SKIP_L5_EQ5 = frozenset(
    {
        "receiving_yards",
        "receptions",
    }
)

# Watch-only (not ticket-eligible). Documented for Week 3+ revisit.
NFL_STD_UNDER_WATCH = frozenset(
    {
        "rush_attempts",
        "rushing_yards",
        "longest_rush",
        "yards_on_first_rush_attempt",
    }
)

# Fantasy score off all books (UNDER + Goblin OVER). Kicker fantasy = kick pts.
NFL_FANTASY_OFF = frozenset({"fantasy_score"})

# Wave-1 Goblin OVER when ``NFL_GOBLIN_TICKETS_ENABLED`` flips (cleanest L5=5).
NFL_GOBLIN_OVER_KEEP_FLIP = frozenset(
    {
        "fg_made",  # 6/6 L5=5
        "receiving_yards",  # 17/23=74%
        "longest_reception",  # 9/12=75%
        "rushing_yards",  # 10/14=71%
    }
)

# Pass-side Goblin — L5=5 degrades vs ungated; validate separately, not on flip.
NFL_GOBLIN_OVER_KEEP_PASS_LATER = frozenset(
    {
        "passing_yards",
        "pass_attempts",
        "pass_completions",
        "longest_completion",
        "passing_tds",
    }
)

# Union used by staged docs / helpers; ticket eligibility uses FLIP only.
NFL_GOBLIN_OVER_KEEP = NFL_GOBLIN_OVER_KEEP_FLIP | NFL_GOBLIN_OVER_KEEP_PASS_LATER

NFL_GOBLIN_OVER_WATCH = frozenset(
    {
        "receptions",
        "rush_attempts",
    }
)

NFL_STD_OVER_KEEP = frozenset(
    {
        "punts",
        "rush_yards_per_carry",
        "longest_completion",
    }
)

# Thin L5=5 / soft-heavy — Week 3 review (need gated n, pass OVER L5=5 n≥10).
NFL_STD_OVER_WATCH = frozenset(
    {
        "rushing_yards",  # 40% ungated; L5=5 2/3; soft Elite OL+Weak Rush D
        "passing_yards",  # L5=5 1/1
        "passing_tds",  # L5=5 1/1
    }
)

# Remaining pass aliases still hard-off (not on keep).
NFL_STD_PASS_YARDS_OFF = frozenset(
    {
        "completions",  # alias — use pass_completions
    }
)

NFL_STD_OVER_FADE = frozenset(
    {
        "player_touchdowns",
        "fg_made",
        "sacks",
        "receiving_yards",
        "receptions",
        "longest_reception",
        "longest_rush",
        "pass_rush_yds",
        "rush_rec_yds",
        "pass_attempts",
        "pass_completions",
    }
)

# Soft unit stacks required for ticket eligibility (W1+W2 ≥70%).
# Key = (prop, side). Missing required tiers → fail closed (never ticket).
# OVER soft gates for watch chips stay here for Week 3 reopen checks.
NFL_SOFT_UNIT_TICKET_GATES: dict[tuple[str, str], str] = {
    ("passing_yards", "UNDER"): "pass_d_e+pass_off_ne",
    ("pass_attempts", "UNDER"): "pass_d_e",
    ("pass_completions", "UNDER"): "pass_d_e",
    ("completion_percentage", "UNDER"): "pass_d_e+dlp_e",
    ("passing_tds", "UNDER"): "pass_d_e",
    ("passing_yards", "OVER"): "pass_d_w+pass_off_ne",  # watch; Goblin later uses dlp_w+pass_off_ne
    ("passing_tds", "OVER"): "dlp_w",  # watch
    ("rushing_yards", "OVER"): "rush_d_w+ol_e",  # watch — Elite OL + Weak Rush D
    ("longest_completion", "OVER"): "mobile",
    ("rush_yards_per_carry", "OVER"): "rush_d_w+rush_off_ne",
    ("punts", "OVER"): "rush_d_e",
}

# Goblin-specific overrides when stack differs from Standard OVER.
NFL_GOBLIN_SOFT_UNIT_TICKET_GATES: dict[str, str] = {
    "passing_yards": "dlp_w+pass_off_ne",
    "pass_attempts": "pass_d_w+pass_off_ne",
    "pass_completions": "pass_d_w+pass_off_ne",
    "longest_completion": "pass_d_w+pass_off_ne",
    "passing_tds": "pass_off_ne+ol_e",
}

# Extreme-low Goblin OVER chips — evaluate min-line floor in Week 2 ledger.
NFL_GOBLIN_EXTREME_LOW_PROPS = frozenset(
    {
        "sacks",
        "player_touchdowns",
        "fg_made",
        "passing_tds",
        "rushing_tds",
        "receiving_tds",
    }
)
NFL_GOBLIN_EXTREME_LOW_LINE = 0.5

_PRIORITY_INDEX = {
    (prop, side): i for i, (prop, side) in enumerate(NFL_TICKET_PRIORITY)
}


def _tok(v: object) -> str:
    return str(v or "").strip().lower().replace(" ", "_").replace("-", "_")


def _prop(r: dict[str, Any]) -> str:
    for k in ("prop", "Prop", "prop_type", "prop_type_normalized", "stat_type"):
        if k in r and r.get(k) not in (None, ""):
            return _tok(r.get(k))
    return ""


def _side(r: dict[str, Any]) -> str:
    for k in ("side", "Direction", "bet_direction", "recommended_side", "dir"):
        if k in r and r.get(k) not in (None, ""):
            return str(r.get(k) or "").strip().upper()
    return ""


def _pick(r: dict[str, Any]) -> str:
    for k in ("pick_type", "Pick Type"):
        if k in r and r.get(k) not in (None, ""):
            return str(r.get(k) or "").strip().title()
    return "Standard"



def _l5_window_n(r: dict[str, Any]) -> float:
    raw = r.get("l5_window_n")
    if raw in (None, ""):
        raw = r.get("L5 Window N")
    try:
        return float(raw) if raw not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def nfl_current_season_l5_ok(r: dict[str, Any]) -> bool:
    """Ticket Gate70 L5 depth.

    When ``ALLOW_CROSS_SEASON_L5`` and the assembled window has 5 decided
    rows (tagged current / preseason / prior), that window counts — no
    extra penalty for the mix. Otherwise require current-season
    ``l5_sample_n >= 5`` and reject PRIOR_* / THIN_* / NO_CURRENT_SEASON.
    READY is still hand-check only.
    """
    from utils.nfl_cross_season_l5 import allow_cross_season_l5

    if allow_cross_season_l5() and _l5_window_n(r) >= 5.0:
        return True
    flag = str(r.get("season_l5_flag") or r.get("Season L5 Flag") or "").strip().upper()
    if flag in {
        "PRIOR_SEASON_FILL",
        "PRIOR_SEASON_L5",
        "NO_CURRENT_SEASON",
        "CROSS_SEASON_L5",
    }:
        return False
    if flag.startswith("THIN_"):
        return False
    raw = r.get("l5_sample_n")
    if raw in (None, ""):
        raw = r.get("L5 Sample N")
    try:
        n = float(raw) if raw not in (None, "") else 0.0
    except (TypeError, ValueError):
        n = 0.0
    return n >= 5.0


def nfl_ticket_priority(r: dict[str, Any]) -> int:
    """Pack order for NFL Standard legs. Unknown keep props sort last."""
    key = (_prop(r), _side(r))
    return _PRIORITY_INDEX.get(key, 99)


_ELITE_PASS_D = frozenset({"Elite", "Above Avg", "Above", "above", "elite"})
_WEAK_D = frozenset({"Weak", "Below Avg", "Below", "weak", "below"})


def _num_field(r: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        v = r.get(k)
        if v is None or v == "":
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f == f:
            return f
    return None


def _tier_from_rank(rank: float | None) -> str:
    if rank is None:
        return ""
    if rank <= 8:
        return "Elite"
    if rank <= 14:
        return "Above"
    if rank <= 20:
        return "Avg"
    if rank <= 26:
        return "Below"
    return "Weak"


def _opp_pass_d_tier(r: dict[str, Any]) -> str:
    """Soft badge: prefer explicit tier, else map opp_pass_def_rank (1=best)."""
    for k in (
        "opp_pass_d_tier",
        "Opp Pass D Tier",
        "opp_pass_def_tier",
        "pass_d_tier",
    ):
        t = str(r.get(k) or "").strip()
        if t:
            return t
    return _tier_from_rank(
        _num_field(r, "opp_pass_def_rank", "pass_def_rank", "Opp Pass Def Rank")
    )


def _opp_rush_d_tier(r: dict[str, Any]) -> str:
    for k in ("opp_rush_d_tier", "Opp Rush D Tier", "opp_rush_def_tier", "rush_d_tier"):
        t = str(r.get(k) or "").strip()
        if t:
            return t
    return _tier_from_rank(
        _num_field(r, "opp_rush_def_rank", "rush_def_rank", "Opp Rush Def Rank")
    )


def _own_pass_off_tier(r: dict[str, Any]) -> str:
    for k in ("own_pass_off_tier", "pass_off_tier", "Own Pass Off Tier"):
        t = str(r.get(k) or "").strip()
        if t:
            return t
    return _tier_from_rank(
        _num_field(r, "own_pass_off_rank", "pass_off_rank", "Pass Off Rank")
    )


def _own_rush_off_tier(r: dict[str, Any]) -> str:
    for k in ("own_rush_off_tier", "rush_off_tier", "Own Rush Off Tier"):
        t = str(r.get(k) or "").strip()
        if t:
            return t
    return _tier_from_rank(
        _num_field(r, "own_rush_off_rank", "rush_off_rank", "Rush Off Rank")
    )


def _own_ol_tier(r: dict[str, Any]) -> str:
    for k in ("own_ol_tier", "ol_tier", "Own OL Tier"):
        t = str(r.get(k) or "").strip()
        if t:
            return t
    return _tier_from_rank(_num_field(r, "own_ol_rank", "ol_rank", "OL Rank"))


def _opp_dl_pass_tier(r: dict[str, Any]) -> str:
    for k in ("opp_dl_pass_tier", "dl_pass_tier", "Opp DL Pass Tier"):
        t = str(r.get(k) or "").strip()
        if t:
            return t
    return _tier_from_rank(
        _num_field(r, "opp_pass_rush_rank", "pass_rush_rank", "Pass Rush Rank")
    )


def _wopr_low(r: dict[str, Any]) -> bool:
    """True when WOPR soft badge is low (<0.50)."""
    flag = r.get("wopr_low")
    if flag is True or str(flag).strip().lower() in {"1", "true", "yes", "y"}:
        return True
    if flag is False or str(flag).strip().lower() in {"0", "false", "no", "n"}:
        return False
    w = _num_field(r, "wopr_L3", "wopr", "wopr_season", "WOPR", "WOPR_L3")
    return w is not None and w < 0.50


def _qb_mobile(r: dict[str, Any]) -> bool:
    t = str(r.get("qb_mobility_tier") or r.get("QB Mobility Tier") or "").strip()
    return t == "Mobile"


def _soft_gate_passes(r: dict[str, Any], gate_id: str) -> bool:
    """Evaluate a soft unit stack id. Missing context → fail closed.

    Empty / unknown ``opp_pass_d_tier`` (and rank fallback) never satisfies
    ``pass_d_e`` / ``pass_d_w`` — tickets are suppressed, not passed through.
    """
    pass_d = _opp_pass_d_tier(r)
    rush_d = _opp_rush_d_tier(r)
    pass_off = _own_pass_off_tier(r)
    rush_off = _own_rush_off_tier(r)
    ol = _own_ol_tier(r)
    dlp = _opp_dl_pass_tier(r)

    # Explicit presence: a required tier string must be non-empty.
    # Membership in Elite/Weak alone would also fail on "", but document it.
    pass_d_e = bool(pass_d) and pass_d in _ELITE_PASS_D
    pass_d_w = bool(pass_d) and pass_d in _WEAK_D
    rush_d_e = bool(rush_d) and rush_d in _ELITE_PASS_D
    rush_d_w = bool(rush_d) and rush_d in _WEAK_D
    pass_off_ne = bool(pass_off) and pass_off not in _ELITE_PASS_D
    rush_off_ne = bool(rush_off) and rush_off not in _ELITE_PASS_D
    ol_e = bool(ol) and ol in _ELITE_PASS_D
    dlp_e = bool(dlp) and dlp in _ELITE_PASS_D
    dlp_w = bool(dlp) and dlp in _WEAK_D

    checks = {
        "pass_d_e": pass_d_e,
        "pass_d_w": pass_d_w,
        "rush_d_e": rush_d_e,
        "rush_d_w": rush_d_w,
        "pass_off_ne": pass_off_ne,
        "rush_off_ne": rush_off_ne,
        "ol_e": ol_e,
        "dlp_e": dlp_e,
        "dlp_w": dlp_w,
        "mobile": _qb_mobile(r),
        "pass_d_e+pass_off_ne": pass_d_e and pass_off_ne,
        "pass_d_e+dlp_e": pass_d_e and dlp_e,
        "pass_d_w+pass_off_ne": pass_d_w and pass_off_ne,
        "dlp_w+pass_off_ne": dlp_w and pass_off_ne,
        "pass_off_ne+ol_e": pass_off_ne and ol_e,
        "rush_d_w+rush_off_ne": rush_d_w and rush_off_ne,
        "rush_d_w+ol_e": rush_d_w and ol_e,
    }
    return bool(checks.get(gate_id, False))


def nfl_soft_unit_ticket_ok(r: dict[str, Any]) -> bool:
    """True when soft unit stack is satisfied (or prop has no soft stack).

    Fail closed when a soft-gated prop is missing the required context
    (e.g. no ``opp_pass_d_tier`` and no ``opp_pass_def_rank``).

    Goblin OVER only uses ``NFL_GOBLIN_SOFT_UNIT_TICKET_GATES`` — never the
    Standard OVER soft map (wave-1 flip chips have no soft stack).
    """
    prop, side = _prop(r), _side(r)
    pick = _pick(r)
    if pick == "Goblin" and side == "OVER":
        gate_id = NFL_GOBLIN_SOFT_UNIT_TICKET_GATES.get(prop)
        if not gate_id:
            return True
        return _soft_gate_passes(r, gate_id)
    gate_id = NFL_SOFT_UNIT_TICKET_GATES.get((prop, side))
    if not gate_id:
        return True
    return _soft_gate_passes(r, gate_id)


def nfl_receptions_under_soft_rank_bump(r: dict[str, Any]) -> int:
    """Option C pack bump — not a gate.

    Receptions UNDER with WOPR low + opp Pass D Elite|Above sorts ahead of
    other receptions UNDER (return 0). Everyone else returns 1. Does not
    change ticket eligibility.
    """
    if _sport_is_nfl(r) and _pick(r) == "Standard" and _side(r) == "UNDER" and _prop(r) == "receptions":
        if _wopr_low(r) and _opp_pass_d_tier(r) in _ELITE_PASS_D:
            return 0
    return 1


def _sport_is_nfl(r: dict[str, Any]) -> bool:
    s = str(r.get("sport") or r.get("Sport") or r.get("league") or r.get("League") or "").upper()
    return s in {"NFL", "NFLP"} or "NFL" in s


def nfl_standard_ticket_eligible(r: dict[str, Any]) -> bool:
    """True when this Standard leg is allowed on Goblin-70 / Flex tickets."""
    if _pick(r) != "Standard":
        return False
    side = _side(r)
    prop = _prop(r)
    if prop in NFL_FANTASY_OFF:
        return False
    if prop in NFL_STD_PASS_YARDS_OFF:
        return False
    ok = False
    if side == "UNDER":
        ok = prop in NFL_STD_UNDER_KEEP
    elif side == "OVER":
        ok = prop in NFL_STD_OVER_KEEP
    if not ok:
        return False
    if not nfl_soft_unit_ticket_ok(r):
        return False
    # Soft-first: participation_gate_check is a no-op until HARD flag flips.
    from utils.nfl_route_participation_gate import participation_gate_check

    allowed, _reason = participation_gate_check(r)
    return bool(allowed)


def nfl_standard_over_is_fade(r: dict[str, Any]) -> bool:
    return _side(r) == "OVER" and _prop(r) in NFL_STD_OVER_FADE


def nfl_std_under_skips_l5_eq5(r: dict[str, Any]) -> bool:
    """True when this NFL Standard UNDER keep skips the L5=5 ticket cut."""
    return (
        _pick(r) == "Standard"
        and _side(r) == "UNDER"
        and _prop(r) in NFL_STD_UNDER_SKIP_L5_EQ5
    )


def nfl_goblin_ticket_eligible(r: dict[str, Any]) -> bool:
    """NFL Goblin OVER ticket gate — wave-1 keep when flag is on.

    Only ``NFL_GOBLIN_OVER_KEEP_FLIP`` clears. Pass-side chips in
    ``NFL_GOBLIN_OVER_KEEP_PASS_LATER`` stay blocked for a later validation.
    """
    if not NFL_GOBLIN_TICKETS_ENABLED:
        return False
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    if _prop(r) in NFL_FANTASY_OFF:
        return False
    if _prop(r) not in NFL_GOBLIN_OVER_KEEP_FLIP:
        return False
    return nfl_soft_unit_ticket_ok(r)


def nfl_goblin_wave1_skip_d(r: dict[str, Any]) -> bool:
    """Wave-1 Goblin OVER skips directional D on tickets (cover still required).

    Early-season Def tiers are thin/noisy (many Avg). Same shape as CFB Pass
    Yards Goblin OVER — D is N/A for the ticket gate, not a Silver miss on the
    list. Pass-side chips stay on the later validation path (no skip).
    """
    if not NFL_GOBLIN_TICKETS_ENABLED:
        return False
    if not _sport_is_nfl(r):
        return False
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    return _prop(r) in NFL_GOBLIN_OVER_KEEP_FLIP


def nfl_goblin_wave1_flip_go(
    rates: dict[str, tuple[int, int]],
    *,
    min_hr: float = NFL_GOBLIN_WAVE1_FLIP_MIN_HR,
    min_n: int = NFL_GOBLIN_WAVE1_FLIP_MIN_N,
) -> tuple[bool, dict[str, Any]]:
    """Binary Week-3 Goblin flag decision from wave-1 unique-game rates.

    ``rates`` maps prop → (hits, n) for Goblin OVER unique games on Week 3
    (or W1–W3 combined if that is the published flip ledger — same four props).

    Returns (go, detail). ``go`` is True only when every wave-1 chip clears
    ``min_hr`` at ``min_n``. Missing prop or n < min_n → NO-GO.
    """
    detail: dict[str, Any] = {"min_hr": min_hr, "min_n": min_n, "chips": {}}
    go = True
    for prop in sorted(NFL_GOBLIN_OVER_KEEP_FLIP):
        hits, n = rates.get(prop, (0, 0))
        try:
            h, nn = int(hits), int(n)
        except (TypeError, ValueError):
            h, nn = 0, 0
        hr = (h / nn) if nn else 0.0
        ok = nn >= min_n and hr >= min_hr - 1e-12
        detail["chips"][prop] = {"hits": h, "n": nn, "hr": round(hr, 4), "ok": ok}
        if not ok:
            go = False
    detail["go"] = go
    return go, detail


def nfl_goblin_is_extreme_low_line(r: dict[str, Any]) -> bool:
    """True for 0.5-class Goblin OVER counting chips (sacks/TDs/FG)."""
    if _pick(r) != "Goblin" or _side(r) != "OVER":
        return False
    if _prop(r) not in NFL_GOBLIN_EXTREME_LOW_PROPS:
        return False
    try:
        return float(r.get("line") or r.get("Line") or 0) <= NFL_GOBLIN_EXTREME_LOW_LINE + 1e-9
    except (TypeError, ValueError):
        return False
