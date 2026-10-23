"""L5=5 ungated shadow. Logged only. Nothing here is placed.

A leg qualifies with directional L5=5, a regular-season event, a matched
event, and a real decided window. DNP and minutes-unknown stay out. The
sport keep gate is not applied. The price is the ungated cell: corrected
tennis Games Won 819/1224 and Total Games 665/1050, and the category
catalog for every other prop. Legs and disjoint 2-leg slips are written
to track ``l5_only_shadow`` with ``placed`` false. They stay off /tickets.

R7: do not wire this track into a placed gate. A prop becomes a candidate
only after 100 forward legs whose Wilson low is above break-even at the
median quote, or an out-of-sample n of at least 150 that passes
split-balance with the same Wilson bar.
"""
from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path
from typing import Any

from utils.category_hit_rate import lookup_category_hr
from utils.comparison_tracks import wilson_band
from utils.fantasy_prop_filter import is_fantasy_prop_label
from utils.postseason_slate import is_regular_season_row, season_class
from utils.prop_norm import canon_prop
from utils.ticket_70_pool import directional_l5, skip_combo_player
from utils.ticket_ev_build import power_multiplier, score_slip, slip_family

TRACK_L5_SHADOW = "l5_only_shadow"
# Median N-correct quotes. Goblin 2-leg 1.40x, Standard 2-leg 3x.
GOBLIN_2_MEDIAN = 1.40
STANDARD_2_MEDIAN = 3.0
_UNKNOWN_OPP = {"", "unknown", "unk", "unknown_opp", "unknown opp"}
# Team leagues whose cells are regular-season only. Tennis, soccer, and
# golf are not on that scoreboard gate.
_SEASON_FAMILY = {
    "MLB",
    "WNBA",
    "WNBA1H",
    "WNBA1Q",
    "NBA",
    "NBA1H",
    "NBA1Q",
    "NBAP",
    "NHL",
    "NFL",
    "NFLP",
    "CFB",
    "CFB1H",
    "CBB",
    "WCBB",
}
# Corrected board-opponent attaches. The category file still has the
# wrong-match tennis Goblin rates (Games Won 28/40, Total Games 37/47).
_CORRECTED_UNGATED: dict[tuple[str, str, str, str], tuple[int, int]] = {
    ("TENNIS", "games_won", "Goblin", "OVER"): (819, 1224),
    ("TENNIS", "match_total_games", "Goblin", "OVER"): (665, 1050),
}
_LOG = Path(__file__).resolve().parents[1] / "data" / "reports" / "track_comparison.jsonl"
_DNP = {"dnp", "no_actual", "minutes_unknown"}


def leg_breakeven(pick_type: object) -> tuple[float, float]:
    """Per-leg break-even at the median 2-leg quote, and that quote."""
    quote = GOBLIN_2_MEDIAN if "goblin" in str(pick_type or "").lower() else STANDARD_2_MEDIAN
    return quote ** -0.5, quote


def _sport(row: dict[str, Any]) -> str:
    return str(row.get("sport") or "").strip().upper()


def _pick(row: dict[str, Any]) -> str:
    raw = str(row.get("pick_type") or "").strip().lower()
    if "goblin" in raw:
        return "Goblin"
    if "standard" in raw:
        return "Standard"
    return ""


def _direction(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    return raw if raw in {"OVER", "UNDER"} else ""


def _prop(row: dict[str, Any]) -> str:
    return canon_prop(_sport(row), row.get("prop") or row.get("prop_type"))


def _opponent(row: dict[str, Any]) -> str:
    return str(row.get("opp_team") or row.get("opp") or row.get("opponent") or "").strip()


def _fold(name: object) -> str:
    return str(name or "").strip().lower()


def _actual_status(row: dict[str, Any]) -> str:
    return str(
        row.get("minutes_status")
        or row.get("actual_status")
        or row.get("result_reason")
        or ""
    ).strip().lower()


def data_checks_ok(row: dict[str, Any]) -> bool:
    """Regular season, matched event, real actuals. DNP stays out."""
    if not isinstance(row, dict):
        return False
    if skip_combo_player(row) or is_fantasy_prop_label(row.get("prop")):
        return False
    from utils.nhl_excluded_props import nhl_series_held

    if nhl_series_held(row):
        return False
    if _pick(row) not in {"Goblin", "Standard"} or not _direction(row):
        return False
    try:
        if int(directional_l5(row) or 0) != 5:
            return False
    except (TypeError, ValueError):
        return False
    if _actual_status(row) in _DNP:
        return False
    sport = _sport(row)
    kind = season_class(row)
    if sport in _SEASON_FAMILY:
        if not is_regular_season_row(row):
            return False
    elif kind in {"preseason", "postseason"}:
        return False
    if sport in {"TENNIS", "SOCCER"}:
        if _opponent(row).lower() in _UNKNOWN_OPP:
            return False
    elif sport in _SEASON_FAMILY and kind != "regular":
        return False
    return True


def ungated_cell(row: dict[str, Any], index: dict) -> dict[str, float] | None:
    """Honest ungated rate. No keep-gate price and no 0.763 floor."""
    sport = _sport(row)
    prop = _prop(row)
    pick = _pick(row)
    direction = _direction(row)
    if not (sport and prop and pick and direction):
        return None
    corrected = _CORRECTED_UNGATED.get((sport, prop, pick, direction))
    if corrected is not None:
        hit, n = corrected
        return {"hr": hit / n, "n": float(n), "hit": float(hit)}
    cell = lookup_category_hr(
        index,
        sport=sport,
        prop=row.get("prop") or prop,
        pick_type=pick,
        direction=direction,
    )
    if not cell or cell.get("hr") is None:
        return None
    try:
        n = float(cell.get("n") or 0)
        hr = float(cell["hr"])
    except (TypeError, ValueError):
        return None
    if n <= 0 or hr <= 0 or hr > 1:
        return None
    return {"hr": hr, "n": n, "hit": float(cell.get("hit") or 0)}


def price_shadow_leg(row: dict[str, Any], index: dict) -> dict[str, Any] | None:
    """Copy the row and stamp the ungated price. None when the data checks fail."""
    if not data_checks_ok(row):
        return None
    out = dict(row)
    cell = ungated_cell(out, index)
    out["track"] = TRACK_L5_SHADOW
    out["placed"] = False
    out["p_source"] = "ungated_cell"
    if cell is None:
        out["p"] = None
        out["p_n"] = None
        out["shadow_reason"] = "no_ungated_cell"
        return out
    out["p"] = cell["hr"]
    out["p_n"] = cell["n"]
    out["p_hit"] = cell["hit"]
    out["shadow_reason"] = ""
    return out


def _shadow_id(row: dict[str, Any]) -> str:
    line = row.get("line")
    return "|".join(
        [
            _fold(row.get("player")),
            _prop(row),
            _pick(row),
            _direction(row),
            "" if line is None else str(line),
        ]
    )


def build_shadow_slips(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Disjoint 2-leg slips, highest sweep chance first. One player per slip set."""
    priced = []
    seen: set[str] = set()
    for leg in legs:
        if leg.get("p") is None:
            continue
        sid = _shadow_id(leg)
        if not sid or sid in seen:
            continue
        seen.add(sid)
        priced.append(leg)
    ranked: list[dict[str, Any]] = []
    for a, b in combinations(priced, 2):
        if _fold(a.get("player")) and _fold(a.get("player")) == _fold(b.get("player")):
            if _prop(a) == _prop(b):
                continue
        scored = score_slip([a, b], require_dollar=False)
        if scored is None:
            fam = slip_family([a, b])
            n_g = sum(1 for leg in (a, b) if "goblin" in str(leg.get("pick_type") or "").lower())
            px, src = power_multiplier(2, family=fam, n_goblin=n_g)
            try:
                p_win = float(a["p"]) * float(b["p"])
            except (TypeError, ValueError):
                continue
            if px <= 0:
                continue
            scored = {
                "legs": [a, b],
                "product": "Power",
                "ev": p_win * px - 1.0,
                "expected_multiple": p_win * px,
                "all_hit_x": px,
                "payout_source": src,
                "p_win": p_win,
                "dollar_test": p_win * px > 1.0,
                "stake": 20,
                "payout_note": "L5=5 shadow. Logged only.",
            }
        ranked.append(scored)
    ranked.sort(key=lambda slip: float(slip["p_win"]), reverse=True)
    used_players: set[str] = set()
    used_ids: set[str] = set()
    out: list[dict[str, Any]] = []
    for scored in ranked:
        ids = [_shadow_id(leg) for leg in scored["legs"]]
        names = [_fold(leg.get("player")) for leg in scored["legs"]]
        if any(not name or name in used_players for name in names):
            continue
        if any(sid in used_ids for sid in ids):
            continue
        used_players.update(names)
        used_ids.update(ids)
        scored["track"] = TRACK_L5_SHADOW
        scored["placed"] = False
        scored["kind"] = "slip"
        scored["dollar_test"] = bool(scored.get("dollar_test"))
        scored["ev_label"] = "EV-positive" if scored["dollar_test"] else "EV-negative"
        out.append(scored)
    return out


def _leg_log(leg: dict[str, Any], *, date: str, index: int) -> dict[str, Any]:
    pick = _pick(leg)
    n_g = 1 if pick == "Goblin" else 0
    n_s = 1 if pick == "Standard" else 0
    return {
        "date": date,
        "id": f"U-leg-{index}",
        "track": TRACK_L5_SHADOW,
        "kind": "leg",
        "placed": False,
        "ticket_track": "",
        "postseason": 0,
        "composition": f"{n_s}S+{n_g}G",
        "n_legs": 1,
        "n_standard": n_s,
        "n_goblin": n_g,
        "player": leg.get("player"),
        "sport": _sport(leg),
        "prop": leg.get("prop"),
        "prop_canon": _prop(leg),
        "pick_type": pick,
        "direction": _direction(leg),
        "line": leg.get("line"),
        "opponent": _opponent(leg),
        "p": leg.get("p"),
        "p_n": leg.get("p_n"),
        "p_hit": leg.get("p_hit"),
        "p_source": leg.get("p_source"),
        "p_win": leg.get("p"),
        "quote": None,
        "payout_source": None,
        "captured_at": None,
        "real_quote": False,
        "scraped_payout": None,
        "ev": None,
        "dollar_test": False,
        "ev_label": "shadow",
        "stake": 20,
        "result": None,
        "exclude_from_book": True,
        "shadow_reason": leg.get("shadow_reason") or "",
    }


def _slip_log(slip: dict[str, Any], *, date: str, index: int) -> dict[str, Any]:
    legs = list(slip.get("legs") or [])
    n_g = sum(1 for leg in legs if _pick(leg) == "Goblin")
    n_s = sum(1 for leg in legs if _pick(leg) == "Standard")
    return {
        "date": date,
        "id": f"U2-{index}",
        "track": TRACK_L5_SHADOW,
        "kind": "slip",
        "placed": False,
        "ticket_track": "",
        "postseason": 0,
        "composition": f"{n_s}S+{n_g}G",
        "n_legs": len(legs),
        "n_standard": n_s,
        "n_goblin": n_g,
        "p_win": slip.get("p_win"),
        "quote": slip.get("all_hit_x"),
        "payout_source": slip.get("payout_source"),
        "captured_at": slip.get("captured_at"),
        "real_quote": False,
        "scraped_payout": None,
        "ev": slip.get("ev"),
        "dollar_test": bool(slip.get("dollar_test")),
        "ev_label": slip.get("ev_label"),
        "stake": slip.get("stake") or 20,
        "result": None,
        "sports": sorted({_sport(leg) for leg in legs}),
        "legs": [
            {
                "player": leg.get("player"),
                "sport": _sport(leg),
                "prop": leg.get("prop"),
                "pick_type": _pick(leg),
                "direction": _direction(leg),
                "line": leg.get("line"),
                "p": leg.get("p"),
            }
            for leg in legs
        ],
        "exclude_from_book": True,
        "standard_sports": sorted({_sport(leg) for leg in legs if _pick(leg) == "Standard"}),
    }


def _write_day(rows: list[dict[str, Any]], *, date: str, path: Path) -> None:
    kept: list[dict[str, Any]] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                isinstance(rec, dict)
                and rec.get("track") == TRACK_L5_SHADOW
                and rec.get("date") == date
            ):
                continue
            if isinstance(rec, dict):
                kept.append(rec)
    path.parent.mkdir(parents=True, exist_ok=True)
    body = kept + rows
    text = "".join(json.dumps(rec, ensure_ascii=False) + "\n" for rec in body)
    path.write_text(text, encoding="utf-8")


def record_l5_shadow(
    board: list[dict[str, Any]],
    *,
    date: str,
    index: dict | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Log today's L5=5 legs and disjoint 2-leg slips. Does not place them."""
    from utils.category_hit_rate import load_category_hr_index

    hr = index if index is not None else load_category_hr_index()
    legs = []
    for row in board:
        priced = price_shadow_leg(row, hr)
        if priced is not None:
            legs.append(priced)
    slips = build_shadow_slips(legs)
    logged = [_leg_log(leg, date=date, index=i) for i, leg in enumerate(legs, start=1)]
    logged.extend(_slip_log(slip, date=date, index=i) for i, slip in enumerate(slips, start=1))
    _write_day(logged, date=date, path=path or _LOG)
    return {
        "track": TRACK_L5_SHADOW,
        "placed": False,
        "legs": legs,
        "slips": slips,
        "n_legs": len(legs),
        "n_slips": len(slips),
    }


def summarize_shadow_props(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Forward hit rate by sport and prop. Flag when the Wilson low clears break-even."""
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("track") != TRACK_L5_SHADOW or row.get("kind") != "leg":
            continue
        key = (
            str(row.get("sport") or ""),
            str(row.get("prop") or ""),
            str(row.get("pick_type") or ""),
            str(row.get("direction") or ""),
        )
        groups.setdefault(key, []).append(row)
    out = []
    for (sport, prop, pick, direction), group in sorted(groups.items()):
        graded = [
            r
            for r in group
            if str(r.get("result") or "").lower() in {"hit", "miss", "win", "loss"}
        ]
        wins = sum(1 for r in graded if str(r.get("result") or "").lower() in {"hit", "win"})
        band = wilson_band(wins, len(graded))
        bar, quote = leg_breakeven(pick)
        clears = bool(band and band[0] > bar)
        priced = [r for r in group if r.get("p") is not None]
        cell_p = float(priced[-1]["p"]) if priced else None
        out.append(
            {
                "sport": sport,
                "prop": prop,
                "pick_type": pick,
                "direction": direction,
                "legs": len(group),
                "graded": len(graded),
                "wins": wins,
                "win_rate": (wins / len(graded)) if graded else None,
                "wilson_low": band[0] if band else None,
                "wilson_high": band[1] if band else None,
                "cell_p": cell_p,
                "breakeven": bar,
                "median_quote": quote,
                "clears_breakeven": clears,
            }
        )
    return out
