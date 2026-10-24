"""Leg flags vs hard blocks for L5=5 pool slips.

Flags stay on the leg and slip. They are graded after the slate. They do
not drop a leg or change price (R7: nothing promotes from a flag until
the forward sample says so).

Hard blocks (elsewhere): card side/line missing, game started, slip
N-correct under 1.4x, EV non-positive at scraped quote with cell−2pts,
same player twice on one day's slips, Demons, fantasy.
"""
from __future__ import annotations

import math
from typing import Any, Sequence

from utils.roster_change import roster_change_from_row

# Season-opener / gap flag: line sits this many L5 spreads from the mean.
STALE_GAP_Z = 1.5
# Wilson bars used only for the thin_slice flag (not a drop).
STD_WILSON_BAR = 0.50
GOB_WILSON_BAR = 0.65
MIN_SLICE_N = 30
# EV prices each leg at cell hit rate minus this (absolute points).
EV_CELL_HAIRCUT = 0.02
# Scraped N-correct floor ($28 on $20).
MIN_PAYOUT_X = 1.4

KNOWN_FLAGS = (
    "thin_slice",
    "no_five_game_file",
    "dnp_risk",
    "stale_gap",
    "l5_is_playoffs",
    "prior_season_l5",
    "roster_changed",
    "postseason_priced",
    "line_under_8",
    "zero_spread",
    "minutes_role_check",
    "not_primary_playmaker",
    "big_man",
    "role_changed",
)


def _finite(values: Sequence[object]) -> list[float]:
    out: list[float] = []
    for raw in values:
        try:
            v = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isnan(v):
            continue
        out.append(v)
    return out


def _wilson_bar(pick: str) -> float:
    return GOB_WILSON_BAR if str(pick or "").lower().startswith("gob") else STD_WILSON_BAR


def collect_leg_flags(
    *,
    values: Sequence[object] | None,
    line: float | None,
    side: str,
    pick: str,
    sport: str = "",
    prop: str = "",
    wilson: float | None = None,
    gap_scaled: float | None = None,
    slice_n: int | None = None,
    row: dict[str, Any] | None = None,
    has_stored_five: bool | None = None,
) -> list[str]:
    """Return ordered unique flag names. Never drops the leg."""
    row = row or {}
    flags: list[str] = []
    vals = _finite(list(values or []))
    pick_u = "Goblin" if "goblin" in str(pick or "").lower() else "Standard"
    side_u = str(side or "").strip().upper()
    sport_u = str(sport or row.get("sport") or "").strip().upper()
    prop_f = str(prop or row.get("prop") or row.get("prop_type") or "").lower()

    stored = has_stored_five
    if stored is None:
        stored = len(vals) >= 5
    if not stored:
        flags.append("no_five_game_file")

    # thin_slice: L5=5 cell n under 30, or Wilson under the bar
    if slice_n is not None and int(slice_n) < MIN_SLICE_N:
        flags.append("thin_slice")
    bar = _wilson_bar(pick_u)
    if wilson is not None and wilson < bar:
        if "thin_slice" not in flags:
            flags.append("thin_slice")

    # dnp_risk: all-zero / zero-minute series, or soccer shots Under
    if vals and all(v == 0 for v in vals[:5]):
        flags.append("dnp_risk")
    status = str(row.get("stat_status") or row.get("minutes_status") or "").lower()
    if "dnp" in status or "zero_minute" in status or "did not play" in status:
        if "dnp_risk" not in flags:
            flags.append("dnp_risk")
    if any(str(row.get(f"stat_status_g{i}") or "").lower() == "dnp" for i in range(1, 6)):
        if "dnp_risk" not in flags:
            flags.append("dnp_risk")
    if sport_u == "SOCCER" and side_u == "UNDER" and "shot" in prop_f.replace(" ", ""):
        if "dnp_risk" not in flags:
            flags.append("dnp_risk")

    # stale_gap
    if line is not None and len(vals) >= 2:
        avg = sum(vals[:5]) / min(len(vals), 5)
        sample = vals[:5] if len(vals) >= 5 else vals
        if len(sample) >= 2:
            var = sum((v - avg) ** 2 for v in sample) / (len(sample) - 1)
            sd = math.sqrt(var) if var > 0 else 0.0
            if sd > 0 and abs(avg - float(line)) > STALE_GAP_Z * sd:
                flags.append("stale_gap")
    if gap_scaled is not None and gap_scaled >= STALE_GAP_Z:
        if "stale_gap" not in flags:
            flags.append("stale_gap")

    # l5_is_playoffs — L5 window is April/May postseason boxes (NBA opener tape)
    if str(row.get("l5_is_playoffs") or "") in {"1", "True", "true"}:
        flags.append("l5_is_playoffs")
    else:
        playoff_hits = 0
        for i in range(1, 6):
            gd = str(row.get(f"game_date_g{i}") or row.get(f"stat_date_g{i}") or "")
            # NBA playoffs typically Apr–Jun
            if len(gd) >= 7 and gd[5:7] in {"04", "05", "06"}:
                playoff_hits += 1
        if playoff_hits >= 3:
            flags.append("l5_is_playoffs")
        src = " ".join(
            str(row.get(k) or "")
            for k in ("l5_source", "l5_window_note", "l5_source_counts", "sample_tag")
        ).lower()
        if "playoff" in src or "postseason" in src:
            if "l5_is_playoffs" not in flags:
                flags.append("l5_is_playoffs")

    # prior_season_l5
    prior = " ".join(
        str(row.get(k) or "")
        for k in ("l5_source", "l5_fill", "sample_tag", "l5_window_note", "l5_source_counts")
    )
    if "PRIOR" in prior.upper() or "prior_season" in prior.lower():
        flags.append("prior_season_l5")

    # roster_changed — L5 predates trade / team churn (Brown→PHI, Robinson→BOS, …)
    if roster_change_from_row(row):
        flags.append("roster_changed")
    elif str(row.get("roster_change_reason") or "").strip():
        flags.append("roster_changed")
    # role_changed — preseason vs season mpg ≥8 AND starter/rotation/bench flip
    min_flag = str(row.get("minutes_flag") or "").lower()
    if "role_changed" in {t.strip() for t in min_flag.split("|")}:
        flags.append("role_changed")

    # postseason_priced
    if str(row.get("postseason") or "") in {"1", "True", "true"} or int(row.get("postseason") or 0) == 1:
        flags.append("postseason_priced")
    if "postseason" in str(row.get("p_source") or "").lower():
        if "postseason_priced" not in flags:
            flags.append("postseason_priced")

    # line_under_8 (Goblin role lines)
    if pick_u == "Goblin" and line is not None and float(line) < 8:
        flags.append("line_under_8")

    # zero_spread: L5 sd is 0 (identical series) — unscaled gap undefined
    if len(vals) >= 5:
        avg = sum(vals[:5]) / 5.0
        var = sum((v - avg) ** 2 for v in vals[:5]) / 4.0
        if var <= 0:
            flags.append("zero_spread")

    # Early-season role check — flag only (Keldon / Towns style)
    pname = " ".join(str(row.get("player") or row.get("player_name") or "").lower().split())
    if pick_u == "Standard" and side_u == "UNDER" and (
        "keldon johnson" in pname or "karl-anthony towns" in pname
    ):
        flags.append("minutes_role_check")

    # Assist Unders: not_primary_playmaker / big_man (track only; gate at n>=100)
    prop_fold = prop_f.replace(" ", "").replace("_", "").replace("+", "")
    is_assistish = (
        "ast" in prop_fold
        or prop_fold in {"assists", "pa", "ptsasts", "ptsast"}
        or prop_f.strip().lower() in {"assists", "pts+asts", "pts+ast", "pa"}
    )
    if side_u == "UNDER" and is_assistish:
        try:
            from utils.role_context import (
                enrich_leg_row,
                is_big_man,
                not_primary_playmaker,
            )

            enriched = enrich_leg_row(row, sport=sport_u, prop=prop_f)
            if not_primary_playmaker(row, ast_team_rank=enriched.get("ast_team_rank")):
                flags.append("not_primary_playmaker")
            if is_big_man(row) or enriched.get("big_man"):
                flags.append("big_man")
        except Exception:
            pass

    # stable order
    order = {name: i for i, name in enumerate(KNOWN_FLAGS)}
    uniq = []
    seen = set()
    for f in flags:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    uniq.sort(key=lambda f: order.get(f, 99))
    return uniq


def flag_count(flags: Sequence[str] | str | None) -> int:
    if flags is None:
        return 0
    if isinstance(flags, str):
        parts = [p for p in flags.split("|") if p]
        return len(parts)
    return len([f for f in flags if f])


def ev_at_scraped(
    cell_hrs: Sequence[float],
    quote: float,
    *,
    haircut: float = EV_CELL_HAIRCUT,
) -> dict[str, float]:
    """EV = (Π max(0, cell_hr − haircut)) × quote − 1."""
    ps = []
    for hr in cell_hrs:
        try:
            p = max(0.0, float(hr) - float(haircut))
        except (TypeError, ValueError):
            return {"p_win": 0.0, "ev": -1.0, "quote": float(quote)}
        ps.append(min(p, 1.0))
    p_win = 1.0
    for p in ps:
        p_win *= p
    q = float(quote)
    return {"p_win": p_win, "ev": p_win * q - 1.0, "quote": q}


def clears_payout_floor(quote: float | None, *, floor: float = MIN_PAYOUT_X) -> bool:
    try:
        return float(quote) >= float(floor)
    except (TypeError, ValueError):
        return False


def placeable_at_quote(
    cell_hrs: Sequence[float],
    quote: float | None,
    *,
    floor: float = MIN_PAYOUT_X,
    haircut: float = EV_CELL_HAIRCUT,
) -> tuple[bool, dict[str, float]]:
    """Hard place test: scraped N-correct >= floor AND EV > 0 at cell−haircut."""
    if quote is None or not clears_payout_floor(quote, floor=floor):
        return False, {"p_win": 0.0, "ev": -1.0, "quote": float(quote or 0.0)}
    scored = ev_at_scraped(cell_hrs, float(quote), haircut=haircut)
    return scored["ev"] > 0.0, scored
