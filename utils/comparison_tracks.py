"""Three comparison tracks on one slate.

Gated Goblin keeps the current all-Goblin build: placed when N-correct is
at least 1.4x, and the dollar test is a label. Gated Mixed adds one Standard
leg that has its own regular-season cell. Gated Standard is all-Standard
from those cells. No cell means that track is empty. A player used on one
track is off the others. Flat $20, two slips per track.
"""
from __future__ import annotations

import json
import math
from itertools import combinations
from pathlib import Path
from typing import Any, Callable

from utils.category_hit_rate import lookup_category_hr
from utils.postseason_slate import season_class
from utils.scraped_quote import (  # noqa: F401 — re-export for callers/tests
    SCRAPED_PAYOUT_SOURCES,
    is_real_scraped_quote,
    real_scraped_payout_x,
)
from utils.ticket_70_pool import directional_l5
from utils.ticket_ev_build import leg_id, score_slip

TRACK_GOBLIN = "gated_goblin"
TRACK_MIXED = "gated_mixed"
TRACK_STANDARD = "gated_standard"
COMPARISON_TRACKS = (TRACK_GOBLIN, TRACK_MIXED, TRACK_STANDARD)
MAX_SLIPS_PER_TRACK = 2
STANDARD_CELL_MIN_N = 30
STANDARD_SUSPECT_HR = 0.75
STAKE = 20
_LOG = Path(__file__).resolve().parents[1] / "data" / "reports" / "track_comparison.jsonl"


def qualifying_standard_cell(
    row: dict[str, Any],
    index: dict,
) -> dict[str, float] | None:
    """Own Standard cell. Regular season, directional L5=5, n>=30.

    The Goblin cell for the same prop is not a substitute.
    """
    if season_class(row) != "regular":
        return None
    try:
        if int(directional_l5(row) or 0) != 5:
            return None
    except (TypeError, ValueError):
        return None
    cell = lookup_category_hr(
        index,
        sport=row.get("sport"),
        prop=row.get("prop") or row.get("prop_type"),
        pick_type="Standard",
        direction=row.get("side") or row.get("direction"),
    )
    if not cell or cell.get("hr") is None:
        return None
    try:
        n = float(cell.get("n") or 0)
        hr = float(cell["hr"])
    except (TypeError, ValueError):
        return None
    if n < STANDARD_CELL_MIN_N or hr <= 0 or hr > 1:
        return None
    return {"hr": hr, "n": n, "hit": float(cell.get("hit") or 0)}


def stamp_standard_cell(row: dict[str, Any], index: dict) -> bool:
    """Price the row from its Standard cell. False leaves the row unpriced."""
    cell = qualifying_standard_cell(row, index)
    if cell is None:
        return False
    row["p"] = cell["hr"]
    row["p_n"] = cell["n"]
    row["p_source"] = "standard_cell"
    row["standard_cell_n"] = cell["n"]
    if cell["hr"] > STANDARD_SUSPECT_HR:
        row["standard_cell_flag"] = "above_75_holdout"
    else:
        row["standard_cell_flag"] = ""
    return True


def _open_rows(
    rows: list[dict[str, Any]],
    *,
    taken: set[str],
    blocked_players: set[str],
    fold: Callable[[object], str],
) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for row in rows:
        if row.get("p") is None:
            continue
        name = fold(row.get("player"))
        if not name or name in blocked_players:
            continue
        lid = leg_id(row, fold)
        if not lid or lid in taken:
            continue
        prev = best.get(lid)
        if prev is None or float(row["p"]) > float(prev["p"]):
            best[lid] = row
    return list(best.values())


def build_one_standard_slips(
    goblins: list[dict[str, Any]],
    standards: list[dict[str, Any]],
    *,
    taken: set[str],
    fold: Callable[[object], str],
    blocked_players: set[str] | None = None,
    max_slips: int = MAX_SLIPS_PER_TRACK,
    max_player_legs: int = 2,
) -> list[dict[str, Any]]:
    """Each slip is one qualifying Standard plus one or two Goblins."""
    blocked = blocked_players or set()
    g_rows = _open_rows(goblins, taken=taken, blocked_players=blocked, fold=fold)
    s_rows = _open_rows(standards, taken=taken, blocked_players=blocked, fold=fold)
    uses: dict[str, int] = {}
    out: list[dict[str, Any]] = []
    while len(out) < max_slips and s_rows and g_rows:
        best: dict[str, Any] | None = None
        for std in s_rows:
            for n_g in (1, 2):
                if len(g_rows) < n_g:
                    continue
                for gcombo in combinations(g_rows, n_g):
                    combo = [std, *gcombo]
                    seen: dict[str, int] = {}
                    ok = True
                    for leg in combo:
                        name = fold(leg.get("player"))
                        if not name:
                            ok = False
                            break
                        seen[name] = seen.get(name, 0) + 1
                        if uses.get(name, 0) + seen[name] > max_player_legs:
                            ok = False
                            break
                    if not ok:
                        continue
                    scored = score_slip(combo, require_dollar=False)
                    if scored is None:
                        continue
                    if best is None or float(scored["ev"]) > float(best["ev"]):
                        best = scored
        if best is None:
            break
        for leg in best["legs"]:
            lid = leg_id(leg, fold)
            if lid:
                taken.add(lid)
            name = fold(leg.get("player"))
            if name:
                uses[name] = uses.get(name, 0) + 1
                blocked.add(name)
        g_rows = _open_rows(g_rows, taken=taken, blocked_players=set(), fold=fold)
        s_rows = _open_rows(s_rows, taken=taken, blocked_players=blocked, fold=fold)
        # A player may hold two props on this slip. They are off later slips.
        g_rows = [row for row in g_rows if fold(row.get("player")) not in blocked]
        out.append(best)
    return out


def track_log_row(slip: dict[str, Any], *, date: str) -> dict[str, Any]:
    legs = list(slip.get("legs") or [])
    n_g = sum(1 for leg in legs if "goblin" in str(leg.get("pick_type") or "").lower())
    n_s = sum(1 for leg in legs if "standard" in str(leg.get("pick_type") or "").lower())
    standard_sports = sorted(
        {
            str(leg.get("sport") or "")
            for leg in legs
            if "standard" in str(leg.get("pick_type") or "").lower() and leg.get("sport")
        }
    )
    ev = slip.get("ev")
    if ev is None:
        ev = slip.get("ev_n_correct")
        if ev is not None:
            try:
                ev = float(ev) - 1.0
            except (TypeError, ValueError):
                ev = None
    dollar = slip.get("dollar_test")
    if dollar is None and ev is not None:
        try:
            dollar = float(ev) > 0
        except (TypeError, ValueError):
            dollar = None
    src = slip.get("payout_source")
    cap = slip.get("captured_at")
    quote = slip.get("all_hit_x") or slip.get("sweep_payout_x")
    # Never promote a pin/estimate into scraped_payout. Real scrapes only.
    scraped = None
    if is_real_scraped_quote(src, cap):
        scraped = slip.get("scraped_payout")
        if scraped in (None, ""):
            scraped = quote
    return {
        "date": date,
        "id": slip.get("id"),
        "track": slip.get("track"),
        "ticket_track": slip.get("ticket_track"),
        "postseason": int(slip.get("postseason") or 0),
        "composition": f"{n_s}S+{n_g}G",
        "n_legs": len(legs),
        "n_standard": n_s,
        "n_goblin": n_g,
        "p_win": slip.get("p_win"),
        "quote": quote,
        "payout_source": src,
        "captured_at": cap,
        "real_quote": is_real_scraped_quote(src, cap),
        "scraped_payout": scraped,
        "ev": ev,
        "dollar_test": dollar,
        "ev_label": slip.get("ev_label"),
        "stake": slip.get("stake") or STAKE,
        "result": slip.get("result"),
        "standard_sports": standard_sports,
        "sports": slip.get("sports") or sorted({str(leg.get("sport") or "") for leg in legs}),
        "standard_cell_flag": sorted(
            {
                str(leg.get("standard_cell_flag") or "")
                for leg in legs
                if leg.get("standard_cell_flag")
            }
        ),
    }


def append_track_log(slip: dict[str, Any], *, date: str, path: Path | None = None) -> None:
    row = track_log_row(slip, date=date)
    if not row.get("track"):
        return
    dest = path or _LOG
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def wilson_band(hits: int, n: int, z: float = 1.96) -> tuple[float, float] | None:
    if n <= 0:
        return None
    p = hits / n
    den = 1.0 + z * z / n
    center = (p + z * z / (2.0 * n)) / den
    margin = z * math.sqrt((p * (1.0 - p) / n) + (z * z / (4.0 * n * n))) / den
    return center - margin, center + margin


def summarize_tracks(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per track.

    Win rate uses all graded slips. Promotion bar + P&L shadow use only rows
    that clear ``is_real_scraped_quote`` (scraped source + non-null
    ``captured_at``). Blank / pin floors never enter scraped_n or roi_scraped.
    """
    by: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if str(row.get("kind") or "slip") == "leg":
            continue
        track = str(row.get("track") or "")
        if track:
            by.setdefault(track, []).append(row)
    out = []
    for track, group in by.items():
        graded = [r for r in group if str(r.get("result") or "").lower() in {"hit", "miss", "win", "loss"}]
        wins = sum(1 for r in graded if str(r.get("result") or "").lower() in {"hit", "win"})
        band = wilson_band(wins, len(graded))
        pred = [float(r["p_win"]) for r in group if r.get("p_win") is not None]
        # Promotion / P&L: decided slips that also carry a real scraped quote.
        real_graded = [r for r in graded if real_scraped_payout_x(r) is not None]
        real_wins = sum(
            1 for r in real_graded if str(r.get("result") or "").lower() in {"hit", "win"}
        )
        real_band = wilson_band(real_wins, len(real_graded))
        scraped: list[float] = []
        stake = 0.0
        returned = 0.0
        for r in real_graded:
            px = real_scraped_payout_x(r)
            if px is None:
                continue
            try:
                st = float(r.get("stake") or STAKE)
            except (TypeError, ValueError):
                continue
            stake += st
            scraped.append(px)
            if str(r.get("result") or "").lower() in {"hit", "win"}:
                returned += st * px
        roi = (returned / stake - 1.0) if stake else None
        sports: set[str] = set()
        for r in group:
            for sport in r.get("standard_sports") or []:
                sports.add(str(sport))
        out.append(
            {
                "track": track,
                "slips": len(group),
                "graded": len(graded),
                "wins": wins,
                "win_rate": (wins / len(graded)) if graded else None,
                "wilson_low": band[0] if band else None,
                "wilson_high": band[1] if band else None,
                "predicted_sweep": (sum(pred) / len(pred)) if pred else None,
                "actual_sweep": (wins / len(graded)) if graded else None,
                # Fail-closed scraped quote counters (promotion bar + P&L shadow).
                "real_quote_n": len(real_graded),
                "real_quote_wins": real_wins,
                "real_quote_win_rate": (real_wins / len(real_graded)) if real_graded else None,
                "real_quote_wilson_low": real_band[0] if real_band else None,
                "real_quote_wilson_high": real_band[1] if real_band else None,
                "scraped_n": len(scraped),
                "roi_scraped": roi,
                "standard_sports": sorted(sports),
            }
        )
    return out
