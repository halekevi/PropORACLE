#!/usr/bin/env python3
"""Shadow a month of $20 slips from scraped N-correct payouts and graded hit rates.

Hit rates come from graded_props, split by sport, pick type, and direction.
A Goblin-70 sleeve (L5=5, L10>=8, directional D) is a second rate. Each
scraped slip keeps its own multiplier. Legs are not all priced at 55%.

Playbook shapes are 3-leg Power and 3-leg Flex. 4-leg and 6-leg are omitted.
A shape whose all-hit pays under 2x fails the $20 -> $40 rule and is flagged.

  py -3.14 scripts/shadow_realistic_pnl.py
  py -3.14 scripts/shadow_realistic_pnl.py --stake 20 --bets 20
"""
from __future__ import annotations

import argparse
import json
import math
import random
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

LIVE_CDP = ROOT / "ui_runner" / "data" / "payout_ladder_live_cdp.json"
GRADED_DIR = ROOT / "ui_runner" / "templates"
OUT_PATH = ROOT / "data" / "reports" / "realistic_pnl_shadow_latest.json"

# All-standard N-correct. 6-leg Power is 40x. 37.5x is a 1st-place tier.
STANDARD_POWER: dict[int, dict[int, float]] = {
    2: {2: 3.0},
    3: {3: 6.0},
    4: {4: 10.0},
    5: {5: 20.0},
    6: {6: 40.0},
}
STANDARD_FLEX: dict[int, dict[int, float]] = {
    2: {2: 3.0},
    3: {3: 3.0, 2: 1.0},
    4: {4: 6.0, 3: 1.5},
    5: {5: 10.0, 4: 2.0, 3: 0.4},
    6: {6: 25.0, 5: 2.0, 4: 0.4},
}

RULE9_MIN_X = 2.0
MIN_CELL_N = 15


def _f(raw: object) -> float | None:
    try:
        v = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    return v


def _legs(raw: object) -> int | None:
    try:
        n = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if n < 2 or n > 6:
        return None
    return n


def _ladder(raw: object) -> dict[int, float]:
    out: dict[int, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            hits = int(k)
        except (TypeError, ValueError):
            continue
        fx = _f(v)
        if fx is not None:
            out[hits] = fx
    return out


def _is_first_place(value: float, n_legs: int) -> bool:
    """A quote above the all-standard all-hit rate is a 1st-place tier."""
    cap = STANDARD_POWER.get(n_legs, {}).get(n_legs)
    if cap is None:
        return False
    return value > cap + 1e-9


def _comp(raw: object) -> str:
    return str(raw or "").replace(" ", "")


def _parse_comp(comp: str) -> tuple[int, int, int]:
    def grab(label: str) -> int:
        m = re.search(rf"(\d+){label}", comp)
        return int(m.group(1)) if m else 0

    return grab("S"), grab("G"), grab("D")


def load_cdp_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


def scraped_rates(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Median N-correct ladders from the CDP log, grouped by slip shape."""
    power: dict[tuple[int, str], list[float]] = defaultdict(list)
    flex: dict[tuple[int, str], list[dict[int, float]]] = defaultdict(list)
    for row in rows:
        n = _legs(row.get("n_legs"))
        if n is None:
            continue
        comp = _comp(row.get("leg_composition"))
        if not comp:
            continue
        px = _f(row.get("power_payout_x"))
        if px is not None and not _is_first_place(px, n):
            power[(n, comp)].append(px)
        ladder = _ladder(row.get("flex_n_correct"))
        if ladder and not any(_is_first_place(v, n) for v in ladder.values()):
            flex[(n, comp)].append(ladder)
        elif not ladder:
            fx = _f(row.get("flex_payout_x"))
            if fx is not None and not _is_first_place(fx, n):
                flex[(n, comp)].append({n: fx})

    cells: list[dict[str, Any]] = []
    for (n, comp), vals in sorted(power.items()):
        cells.append(
            {
                "product": "Power",
                "n_legs": n,
                "composition": comp,
                "n_scrapes": len(vals),
                "n_correct": {n: round(statistics.median(vals), 4)},
                "min_x": round(min(vals), 4),
                "max_x": round(max(vals), 4),
                "source": "live_cdp",
            }
        )
    for (n, comp), ladders in sorted(flex.items()):
        keys = sorted({k for lad in ladders for k in lad})
        merged = {
            k: round(statistics.median([lad[k] for lad in ladders if k in lad]), 4) for k in keys
        }
        cells.append(
            {
                "product": "Flex",
                "n_legs": n,
                "composition": comp,
                "n_scrapes": len(ladders),
                "n_correct": merged,
                "source": "live_cdp",
            }
        )
    return cells


def composition_audit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Count 2-leg slips by composition, before the 1st-place filter."""
    two = [r for r in rows if _legs(r.get("n_legs")) == 2]
    counts = Counter(_comp(r.get("leg_composition")) or "(blank)" for r in two)
    all_g = "0S+2G+0D"
    labeled = [r for r in two if _comp(r.get("leg_composition")) == all_g]
    dropped = 0
    kept_pays: list[float] = []
    for r in labeled:
        px = _f(r.get("power_payout_x"))
        if px is None:
            continue
        if _is_first_place(px, 2):
            dropped += 1
        else:
            kept_pays.append(px)
    return {
        "n_2_leg": len(two),
        "by_composition": dict(counts),
        "all_goblin": counts.get(all_g, 0),
        "mixed_1s1g": counts.get("1S+1G+0D", 0),
        "all_standard": counts.get("2S+0G+0D", 0),
        "all_goblin_first_place_dropped": dropped,
        "all_goblin_n_correct_kept": len(kept_pays),
    }


def payout_histogram(rows: list[dict[str, Any]], n_legs: int, comp: str) -> list[dict[str, Any]]:
    pays: list[float] = []
    for r in rows:
        if _legs(r.get("n_legs")) != n_legs or _comp(r.get("leg_composition")) != comp:
            continue
        px = _f(r.get("power_payout_x"))
        if px is None or _is_first_place(px, n_legs):
            continue
        pays.append(round(px, 2))
    counts = Counter(pays)
    return [{"payout_x": k, "n": counts[k]} for k in sorted(counts)]


def expected_multiplier(n_legs: int, ladder: dict[int, float], p: float) -> float:
    table = {int(k): float(v) for k, v in ladder.items()}
    return sum(binom_pmf(n_legs, k, p) * table.get(k, 0.0) for k in range(n_legs + 1))


def wilson_lower(hits: int, n: int, z: float = 1.96) -> float | None:
    """95% Wilson lower bound as a fraction."""
    if n <= 0:
        return None
    p = hits / n
    z2 = z * z
    den = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / den
    half = (z / den) * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))
    return max(0.0, center - half)


def breakeven(n_legs: int, all_hit_x: float, ladder: dict[int, float] | None = None) -> dict[str, float]:
    """Per-leg hit rate that makes expected return 1.

    Power is all-or-nothing, so the slip must win 1/payout of the time.
    3-leg Flex at 3x for 3/3 and 1x for 2/3 returns 3p^2, so the leg bar is 1/sqrt(3).
    """
    table = {int(k): float(v) for k, v in (ladder or {n_legs: all_hit_x}).items()}
    if n_legs not in table:
        table[n_legs] = float(all_hit_x)
    partial = any(k != n_legs and v > 0 for k, v in table.items())
    if (
        n_legs == 3
        and abs(table.get(3, 0.0) - 3.0) < 1e-9
        and abs(table.get(2, 0.0) - 1.0) < 1e-9
        and table.get(1, 0.0) == 0
        and table.get(0, 0.0) == 0
    ):
        leg = 1.0 / math.sqrt(3.0)
        formula = "3p^2"
    elif not partial:
        leg = (1.0 / float(all_hit_x)) ** (1.0 / n_legs)
        formula = "power"
    else:
        lo, hi = 1e-6, 1.0 - 1e-6
        for _ in range(60):
            mid = (lo + hi) / 2.0
            if expected_multiplier(n_legs, table, mid) < 1.0:
                lo = mid
            else:
                hi = mid
        leg = (lo + hi) / 2.0
        formula = "flex"
    slip = leg ** n_legs
    return {
        "all_hit_x": float(all_hit_x),
        "slip_win_rate": slip if formula == "power" else 1.0 / float(all_hit_x),
        "per_leg_rate": leg,
        "formula": formula,
        "rule9_pass": float(all_hit_x) + 1e-9 >= RULE9_MIN_X,
    }


def _norm_side(raw: object) -> str:
    side = str(raw or "").strip().upper()
    if side in ("O", "OVER", "HIGHER"):
        return "OVER"
    if side in ("U", "UNDER", "LOWER"):
        return "UNDER"
    return ""


def _norm_pick(raw: object) -> str:
    s = str(raw or "").strip().lower()
    if "gob" in s:
        return "Goblin"
    if "demon" in s:
        return "Demon"
    if "std" in s or s == "standard":
        return "Standard"
    return ""


def _num(raw: object) -> float | None:
    if raw is None or raw == "":
        return None
    s = str(raw).strip()
    if s in ("", "—", "–", "-", "nan", "None", "null"):
        return None
    try:
        v = float(s)
    except ValueError:
        return None
    if not math.isfinite(v):
        return None
    return v


def _dir_count(row: dict, side: str, window: str) -> float | None:
    if side == "OVER":
        keys = (f"{window}_over", f"last{window[1:]}_over", f"line_hits_over_{window[1:]}")
    else:
        keys = (f"{window}_under", f"last{window[1:]}_under", f"line_hits_under_{window[1:]}")
    for k in keys:
        v = _num(row.get(k))
        if v is not None:
            return v
    return None


def _bump(cell: dict[str, int], hit: bool) -> None:
    cell["n"] += 1
    if hit:
        cell["hits"] += 1


def _fold(raw: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(raw or "").lower())


def _passes_count_gate(row: dict, side: str, sport: str, prop: str) -> bool:
    from utils.defense_tiers import d_aligned

    l5 = _dir_count(row, side, "l5")
    l10 = _dir_count(row, side, "l10")
    if l5 != 5 or l10 is None or l10 < 8:
        return False
    return bool(d_aligned(sport, side, row.get("def_tier"), prop))


def _grade_dirs(templates: Path) -> list[Path]:
    dirs = [templates]
    extra = Path(r"H:\PropORACLE") / "ui_runner" / "templates"
    try:
        same = extra.resolve() == templates.resolve()
    except OSError:
        same = False
    if extra.is_dir() and not same:
        dirs.append(extra)
    return dirs


def load_grade_bundle(templates: Path, join_dates: set[str] | None = None) -> dict[str, Any]:
    """Graded hit rates, push/void audit, and a leg index for scrape joins.

    HIT and MISS are the only decided results. PUSH and VOID stay out of the rate.
    Goblin-70 and the same L5=5, L10>=8, directional-D gate on Standard are separate sleeves.
    """
    cells: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: {"n": 0, "hits": 0})
    prop_cells: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: {"n": 0, "hits": 0})
    audit: dict[tuple[str, str], Counter] = defaultdict(Counter)
    legs: dict[tuple[str, str, str, str], list[dict]] = defaultdict(list)
    if not templates.is_dir():
        return {"cells": cells, "prop_cells": prop_cells, "audit": audit, "legs": legs, "extra_dates": []}
    seen: set[str] = set()
    extra_dates: list[str] = []
    for folder_i, folder in enumerate(_grade_dirs(templates)):
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            if date in seen:
                continue
            if folder_i > 0 and (not join_dates or date not in join_dates):
                continue
            seen.add(date)
            if folder_i > 0:
                extra_dates.append(date)
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            props = payload.get("props") or payload.get("rows") or []
            if not isinstance(props, list):
                continue
            for row in props:
                if not isinstance(row, dict):
                    continue
                res = str(row.get("result") or "").strip().upper()
                sport = str(row.get("sport") or "").strip().upper()
                pick = _norm_pick(row.get("pick_type"))
                side = _norm_side(row.get("direction") or row.get("over_under"))
                if not sport or not pick or not side:
                    continue
                if pick == "Standard" and folder_i == 0:
                    bucket = res if res in ("HIT", "MISS", "PUSH", "VOID") else "OTHER"
                    audit[(pick, side)][bucket] += 1
                if res not in ("HIT", "MISS"):
                    continue
                hit = res == "HIT"
                prop = str(row.get("prop") or row.get("prop_type") or "")
                if pick == "Goblin":
                    legs[(date, _fold(row.get("player")), _fold(prop), side)].append(row)
                if folder_i > 0 or pick not in ("Goblin", "Standard"):
                    continue
                _bump(cells[(sport, pick, side)], hit)
                _bump(cells[("*", pick, side)], hit)
                if not _passes_count_gate(row, side, sport, prop):
                    continue
                sleeve = "G70" if pick == "Goblin" else "S70"
                _bump(cells[(sleeve, sport, side)], hit)
                _bump(cells[(sleeve, "*", side)], hit)
                if pick == "Standard":
                    _bump(prop_cells[(sport, side, prop.strip() or "?")], hit)
    return {
        "cells": cells,
        "prop_cells": prop_cells,
        "audit": audit,
        "legs": legs,
        "extra_dates": extra_dates,
    }


def load_graded_rates(templates: Path) -> dict[tuple[str, str, str], dict[str, int]]:
    return load_grade_bundle(templates)["cells"]


def _rate(cells: dict[tuple[str, str, str], dict[str, int]], key: tuple[str, str, str]) -> tuple[float | None, int]:
    cell = cells.get(key)
    if not cell or cell["n"] < MIN_CELL_N:
        return None, 0 if not cell else cell["n"]
    return cell["hits"] / cell["n"], cell["n"]


def rate_table(cells: dict[tuple[str, str, str], dict[str, int]], *, min_n: int = 40) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (sport, pick, side), cell in sorted(cells.items()):
        if cell["n"] < min_n:
            continue
        if sport in ("*", "G70", "S70"):
            continue
        rows.append(
            {
                "sport": sport,
                "pick_type": pick,
                "direction": side,
                "hits": cell["hits"],
                "n": cell["n"],
                "rate": round(cell["hits"] / cell["n"], 4),
            }
        )
    return rows


def sleeve_table(cells: dict[tuple[str, str, str], dict[str, int]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (kind, sport, side), cell in sorted(cells.items()):
        if kind != "G70" or cell["n"] <= 0:
            continue
        lo = wilson_lower(cell["hits"], cell["n"])
        rows.append(
            {
                "sport": sport,
                "direction": side,
                "hits": cell["hits"],
                "n": cell["n"],
                "rate": round(cell["hits"] / cell["n"], 4),
                "wilson_lo": None if lo is None else round(lo, 4),
            }
        )
    return rows


def _sports_from_notes(notes: object) -> list[str]:
    m = re.search(r"sports=([^;]+)", str(notes or ""), flags=re.I)
    if not m:
        return []
    return [part.strip().upper() for part in m.group(1).split(",") if part.strip()]


def parse_dedupe_legs(key: object) -> list[dict[str, Any]]:
    text = str(key or "")
    if not text:
        return []
    chunks = text.split("||")
    legs: list[dict[str, Any]] = []
    for i, chunk in enumerate(chunks):
        bits = chunk.split("|")
        if i == 0 and bits and bits[0][:4].isdigit():
            bits = bits[1:]
        if i == len(chunks) - 1 and len(bits) >= 5 and _f(bits[-1]) is not None:
            bits = bits[:-1]
        if len(bits) < 4:
            continue
        legs.append(
            {
                "player": bits[0],
                "prop": bits[1],
                "side": _norm_side(bits[2]),
                "line": _f(bits[3]),
            }
        )
    return legs


def _directions_from_dedupe(key: object) -> list[str]:
    return [leg["side"] for leg in parse_dedupe_legs(key) if leg["side"]]


def _pick_list(comp: str, n_legs: int) -> list[str]:
    n_s, n_g, n_d = _parse_comp(comp)
    picks = (["Standard"] * n_s) + (["Goblin"] * n_g) + (["Demon"] * n_d)
    if len(picks) != n_legs:
        return []
    return picks


def leg_hit_rate(
    cells: dict[tuple[str, str, str], dict[str, int]],
    sport: str,
    pick: str,
    side: str,
    *,
    sleeve: bool,
) -> tuple[float | None, int]:
    if sleeve and pick == "Goblin":
        rate, n = _rate(cells, ("G70", sport, side))
        if rate is None:
            rate, n = _rate(cells, ("G70", "*", side))
        return rate, n
    rate, n = _rate(cells, (sport, pick, side))
    if rate is None:
        rate, n = _rate(cells, ("*", pick, side))
    return rate, n


def slip_leg_rates(
    row: dict[str, Any],
    cells: dict[tuple[str, str, str], dict[str, int]],
    *,
    sleeve: bool,
) -> list[float] | None:
    n = _legs(row.get("n_legs"))
    comp = _comp(row.get("leg_composition"))
    if n is None or not comp:
        return None
    picks = _pick_list(comp, n)
    if not picks:
        return None
    sports = _sports_from_notes(row.get("notes"))
    sport = sports[0] if len(sports) == 1 else ""
    dirs = _directions_from_dedupe(row.get("_dedupe_key"))
    if len(dirs) != n:
        dirs = ["OVER"] * n
    rates: list[float] = []
    # All-Goblin slips pair each scraped direction with a Goblin rate.
    # Mixed slips follow Standard, then Goblin, then Demon against dedupe order.
    for pick, side in zip(picks, dirs):
        use_sport = sport or "*"
        rate, _n = leg_hit_rate(cells, use_sport, pick, side, sleeve=sleeve)
        if rate is None:
            return None
        rates.append(rate)
    return rates


def power_outcomes(p_legs: list[float], mult: float, stake: float) -> list[tuple[float, float]]:
    p_win = 1.0
    for p in p_legs:
        p_win *= p
    return [(p_win, stake * (mult - 1.0)), (1.0 - p_win, -stake)]


def slip_outcomes(n_legs: int, ladder: dict[int, float], p: float, stake: float) -> list[tuple[float, float]]:
    """(probability, profit) for one slip. A missing hit-count pays 0x."""
    table = {int(k): float(v) for k, v in ladder.items()}
    out: list[tuple[float, float]] = []
    for k in range(n_legs + 1):
        mult = table.get(k, 0.0)
        out.append((binom_pmf(n_legs, k, p), stake * (mult - 1.0)))
    return out


def binom_pmf(n: int, k: int, p: float) -> float:
    if k < 0 or k > n:
        return 0.0
    return math.comb(n, k) * (p**k) * ((1.0 - p) ** (n - k))


def expected_profit(outcomes: list[tuple[float, float]], n_bets: int) -> float:
    one = sum(prob * profit for prob, profit in outcomes)
    return one * n_bets


def month_band(
    outcomes: list[tuple[float, float]],
    n_bets: int,
    *,
    trials: int = 4000,
    seed: int = 11,
) -> dict[str, float]:
    rng = random.Random(seed)
    totals: list[float] = []
    for _ in range(trials):
        total = 0.0
        for _bet in range(n_bets):
            u = rng.random()
            acc = 0.0
            for prob, profit in outcomes:
                acc += prob
                if u <= acc:
                    total += profit
                    break
        totals.append(total)
    totals.sort()

    def pct(q: float) -> float:
        i = min(len(totals) - 1, max(0, int(round(q * (len(totals) - 1)))))
        return totals[i]

    return {"cold": pct(0.20), "median": pct(0.50), "warm": pct(0.80)}


def pool_month(
    slip_outcomes_list: list[list[tuple[float, float]]],
    n_bets: int,
    *,
    trials: int = 4000,
    seed: int = 11,
) -> dict[str, float]:
    """Draw each of the month's slips from the scraped pool, with replacement."""
    if not slip_outcomes_list:
        return {"expected": 0.0, "cold": 0.0, "median": 0.0, "warm": 0.0}
    evs = [sum(p * profit for p, profit in dist) for dist in slip_outcomes_list]
    expected = (sum(evs) / len(evs)) * n_bets
    rng = random.Random(seed)
    totals: list[float] = []
    n_pool = len(slip_outcomes_list)
    for _ in range(trials):
        total = 0.0
        for _bet in range(n_bets):
            dist = slip_outcomes_list[rng.randrange(n_pool)]
            u = rng.random()
            acc = 0.0
            for prob, profit in dist:
                acc += prob
                if u <= acc:
                    total += profit
                    break
        totals.append(total)
    totals.sort()

    def pct(q: float) -> float:
        i = min(len(totals) - 1, max(0, int(round(q * (len(totals) - 1)))))
        return totals[i]

    return {"expected": expected, "cold": pct(0.20), "median": pct(0.50), "warm": pct(0.80)}


def _money(v: float) -> str:
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.0f}"


def _pct(v: float) -> str:
    return f"{100.0 * v:.1f}%"


def summarize_power_pool(
    rows: list[dict[str, Any]],
    cells: dict[tuple[str, str, str], dict[str, int]],
    *,
    n_legs: int,
    comp: str,
    stake: float,
    n_bets: int,
    sleeve: bool,
) -> dict[str, Any] | None:
    dists: list[list[tuple[float, float]]] = []
    matched = 0
    unmatched = 0
    pays: list[float] = []
    for row in rows:
        if _legs(row.get("n_legs")) != n_legs or _comp(row.get("leg_composition")) != comp:
            continue
        px = _f(row.get("power_payout_x"))
        if px is None or _is_first_place(px, n_legs):
            continue
        pays.append(px)
        rates = slip_leg_rates(row, cells, sleeve=sleeve)
        if not rates or len(rates) != n_legs:
            unmatched += 1
            continue
        matched += 1
        dists.append(power_outcomes(rates, px, stake))
    if not pays:
        return None
    med = statistics.median(pays)
    be = breakeven(n_legs, med)
    band = pool_month(dists, n_bets) if dists else None
    return {
        "n_legs": n_legs,
        "composition": comp,
        "product": "Power",
        "n_scrapes": len(pays),
        "matched_slips": matched,
        "unmatched_slips": unmatched,
        "median_x": round(med, 4),
        "histogram": payout_histogram(rows, n_legs, comp),
        "breakeven": be,
        "rule9_pass": be["rule9_pass"],
        "sleeve": sleeve,
        "month": None
        if band is None
        else {k: round(v, 2) for k, v in band.items()},
    }


def _closest_graded(cands: list[dict], line: float | None) -> dict | None:
    if not cands:
        return None
    if line is None:
        return cands[0]
    best: dict | None = None
    best_d = 10**9
    for row in cands:
        posted = _f(row.get("line"))
        if posted is None:
            continue
        dist = abs(posted - line)
        if dist < best_d:
            best, best_d = row, dist
    if best is not None and best_d <= 0.51:
        return best
    return cands[0]


def _gate_status(row: dict, side: str) -> str:
    sport = str(row.get("sport") or "").strip().upper()
    prop = str(row.get("prop") or row.get("prop_type") or "")
    l5 = _dir_count(row, side, "l5")
    l10 = _dir_count(row, side, "l10")
    tier = str(row.get("def_tier") or "").strip()
    if _passes_count_gate(row, side, sport, prop):
        return "g70"
    if l5 is None or l10 is None or not tier:
        return "unknown"
    return "fail"


def _quote_band(slips: list[dict[str, Any]]) -> dict[str, Any]:
    gaps = [g for slip in slips for g in slip["gaps"]]
    slips_with_gap = sum(1 for slip in slips if slip["gaps"])
    return {
        "n_slips": len(slips),
        "slips_with_gap": slips_with_gap,
        "legs_matched": sum(slip["matched"] for slip in slips),
        "g70_legs": sum(slip["g70"] for slip in slips),
        "gate_fail_legs": sum(slip["fail"] for slip in slips),
        "unknown_legs": sum(slip["unknown"] for slip in slips),
        "unmatched_legs": sum(slip["unmatched"] for slip in slips),
        "slips_with_any_g70": sum(1 for slip in slips if slip["g70"] > 0),
        "median_gap": None if not gaps else round(statistics.median(gaps), 3),
    }


def join_goblin_quotes(
    rows: list[dict[str, Any]],
    legs_index: dict[tuple[str, str, str, str], list[dict]],
) -> dict[str, Any]:
    """Pair each all-Goblin 2-leg N-correct quote with its line gap and Goblin-70 legs."""
    slips: list[dict[str, Any]] = []
    unique: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        if _legs(row.get("n_legs")) != 2 or _comp(row.get("leg_composition")) != "0S+2G+0D":
            continue
        px = _f(row.get("power_payout_x"))
        if px is None or _is_first_place(px, 2):
            continue
        date = str(row.get("date") or "")[:10]
        raw_gaps = row.get("goblin_deltas")
        gaps = [g for raw in (raw_gaps if isinstance(raw_gaps, list) else []) if (g := _f(raw)) is not None]
        counts = {"g70": 0, "fail": 0, "unknown": 0, "unmatched": 0}
        for leg in parse_dedupe_legs(row.get("_dedupe_key")):
            key = (date, _fold(leg["player"]), _fold(leg["prop"]), leg["side"])
            chosen = _closest_graded(legs_index.get(key) or [], leg.get("line"))
            if chosen is None:
                counts["unmatched"] += 1
                status = "unmatched"
            else:
                side = _norm_side(chosen.get("direction") or chosen.get("over_under")) or leg["side"]
                status = _gate_status(chosen, side)
                counts[status] += 1
            ident = (date, _fold(leg["player"]), _fold(leg["prop"]))
            slot = unique.setdefault(
                ident,
                {
                    "date": date,
                    "player": leg["player"],
                    "prop": leg["prop"],
                    "status": status,
                    "quotes": [],
                },
            )
            if status == "g70":
                slot["status"] = "g70"
            slot["quotes"].append(px)
        slips.append(
            {
                "x": px,
                "gaps": gaps,
                "matched": counts["g70"] + counts["fail"] + counts["unknown"],
                "g70": counts["g70"],
                "fail": counts["fail"],
                "unknown": counts["unknown"],
                "unmatched": counts["unmatched"],
            }
        )
    g70_legs = []
    for slot in unique.values():
        hi = [q for q in slot["quotes"] if q + 1e-9 >= 2.0]
        if slot["status"] != "g70" or not hi:
            continue
        g70_legs.append(
            {
                "date": slot["date"],
                "player": slot["player"],
                "prop": slot["prop"],
                "n_slips": len(hi),
                "min_x": round(min(hi), 2),
                "max_x": round(max(hi), 2),
            }
        )
    g70_legs.sort(key=lambda r: (-r["n_slips"], r["player"]))
    return {
        "n_slips": len(slips),
        "under_1_5": _quote_band([s for s in slips if s["x"] < 1.5]),
        "from_1_5_to_2": _quote_band([s for s in slips if 1.5 <= s["x"] < 2.0]),
        "at_least_2": _quote_band([s for s in slips if s["x"] + 1e-9 >= 2.0]),
        "g70_legs_on_2x": g70_legs,
        "quote_breakeven": [
            {"quote": q, "per_leg_rate": round((1.0 / q) ** 0.5, 4)}
            for q in (1.4, 1.7, 2.0, 2.5)
        ],
    }


def published_shape(
    *,
    label: str,
    product: str,
    n_legs: int,
    ladder: dict[int, float],
    p: float,
    p_n: int,
    stake: float,
    n_bets: int,
) -> dict[str, Any]:
    all_hit = float(ladder[n_legs])
    be = breakeven(n_legs, all_hit, ladder)
    outcomes = slip_outcomes(n_legs, ladder, p, stake)
    band = month_band(outcomes, n_bets)
    ev = expected_profit(outcomes, n_bets)
    return {
        "label": label,
        "product": product,
        "n_legs": n_legs,
        "n_correct": {str(k): v for k, v in sorted(ladder.items())},
        "source": "published_n_correct",
        "hit_rate": round(p, 4),
        "hit_rate_n": p_n,
        "breakeven": be,
        "rule9_pass": be["rule9_pass"],
        "expected_profit": round(ev, 2),
        "cold_profit": round(band["cold"], 2),
        "median_profit": round(band["median"], 2),
        "warm_profit": round(band["warm"], 2),
    }


def _audit_public(audit: dict) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for (pick, side), counts in audit.items():
        out[f"{pick}_{side}".lower()] = {k: int(v) for k, v in sorted(counts.items())}
    return out


def gated_standard_flex(
    cells: dict[tuple[str, str, str], dict[str, int]],
    *,
    stake: float,
    n_bets: int,
    min_n: int = 20,
) -> list[dict[str, Any]]:
    """3-leg Flex priced at each gated Standard cell, OVER and UNDER separate."""
    bar = 1.0 / math.sqrt(3.0)
    rows: list[dict[str, Any]] = []
    for (kind, sport, side), cell in sorted(cells.items()):
        if kind != "S70" or sport == "*" or cell["n"] < min_n:
            continue
        p = cell["hits"] / cell["n"]
        lo = wilson_lower(cell["hits"], cell["n"])
        shape = published_shape(
            label=f"3-leg Flex, gated Standard {sport} {side}",
            product="Flex",
            n_legs=3,
            ladder=STANDARD_FLEX[3],
            p=p,
            p_n=cell["n"],
            stake=stake,
            n_bets=n_bets,
        )
        shape["hits"] = cell["hits"]
        shape["wilson_lo"] = None if lo is None else round(lo, 4)
        shape["point_clears_58"] = p + 1e-12 >= bar
        shape["wilson_clears_58"] = lo is not None and lo + 1e-12 >= bar
        rows.append(shape)
    rows.sort(key=lambda r: (-r["hit_rate"], -r["hit_rate_n"]))
    return rows


def gated_standard_props(
    prop_cells: dict[tuple[str, str, str], dict[str, int]],
    *,
    min_n: int = 20,
) -> list[dict[str, Any]]:
    """Best gated Standard cells by sport, prop, and direction."""
    bar = 1.0 / math.sqrt(3.0)
    rows: list[dict[str, Any]] = []
    for (sport, side, prop), cell in prop_cells.items():
        if cell["n"] < min_n:
            continue
        p = cell["hits"] / cell["n"]
        lo = wilson_lower(cell["hits"], cell["n"])
        rows.append(
            {
                "sport": sport,
                "direction": side,
                "prop": prop,
                "hits": cell["hits"],
                "n": cell["n"],
                "rate": round(p, 4),
                "wilson_lo": None if lo is None else round(lo, 4),
                "point_clears_58": p + 1e-12 >= bar,
                "wilson_clears_58": lo is not None and lo + 1e-12 >= bar,
            }
        )
    rows.sort(key=lambda r: (-r["rate"], -r["n"]))
    return rows


def run_shadow(*, stake: float, n_bets: int, rows: list[dict[str, Any]] | None = None, cells: dict | None = None) -> dict[str, Any]:
    cdp = rows if rows is not None else load_cdp_rows(LIVE_CDP)
    if cells is not None:
        graded = cells
        bundle = {"cells": cells, "prop_cells": {}, "audit": {}, "legs": {}, "extra_dates": []}
    else:
        bundle = load_grade_bundle(
            GRADED_DIR,
            {str(r.get("date") or "")[:10] for r in cdp},
        )
        graded = bundle["cells"]
    audit = composition_audit(cdp)
    flex_ladders = sum(
        1
        for r in cdp
        if _legs(r.get("n_legs")) == 3 and _ladder(r.get("flex_n_correct"))
    )
    std_over, std_over_n = _rate(graded, ("*", "Standard", "OVER"))
    std_under, std_under_n = _rate(graded, ("*", "Standard", "UNDER"))
    gob_over, gob_over_n = _rate(graded, ("*", "Goblin", "OVER"))
    g70_over, g70_over_n = _rate(graded, ("G70", "*", "OVER"))

    published: list[dict[str, Any]] = []
    if std_over is not None:
        published.append(
            published_shape(
                label="All-Standard 3-leg Power",
                product="Power",
                n_legs=3,
                ladder=STANDARD_POWER[3],
                p=std_over,
                p_n=std_over_n,
                stake=stake,
                n_bets=n_bets,
            )
        )
        published.append(
            published_shape(
                label="All-Standard 3-leg Flex",
                product="Flex",
                n_legs=3,
                ladder=STANDARD_FLEX[3],
                p=std_over,
                p_n=std_over_n,
                stake=stake,
                n_bets=n_bets,
            )
        )

    goblin_shapes = []
    for n, comp, label in (
        (2, "0S+2G+0D", "All-Goblin 2-leg Power"),
        (3, "0S+3G+0D", "All-Goblin 3-leg Power"),
        (2, "1S+1G+0D", "Mixed 2-leg Power (1 Standard + 1 Goblin)"),
    ):
        for sleeve in (False, True):
            summary = summarize_power_pool(
                cdp, graded, n_legs=n, comp=comp, stake=stake, n_bets=n_bets, sleeve=sleeve
            )
            if summary is None:
                continue
            summary["label"] = label
            summary["rate_source"] = "goblin70_sleeve" if sleeve else "graded_sport_pick_direction"
            goblin_shapes.append(summary)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stake": stake,
        "bets": n_bets,
        "wagered": stake * n_bets,
        "rule9_min_x": RULE9_MIN_X,
        "note": (
            "Hit rates are graded HIT and MISS only. PUSH and VOID are excluded. "
            "Goblin-70 and gated Standard are L5=5, L10>=8, and directional D. "
            "3-leg Flex return is 3p^2, so the per-leg bar is 1/sqrt(3). "
            "Each scraped slip uses its own N-correct multiplier. "
            "All-hit under 2x fails the $20 to $40 rule. "
            "4-leg and 6-leg are omitted. Neither live path is a ticket rule yet."
        ),
        "composition_2leg": audit,
        "flex_3leg_ladders": flex_ladders,
        "pooled_rates": {
            "standard_over": {"rate": std_over, "n": std_over_n},
            "standard_under": {"rate": std_under, "n": std_under_n},
            "goblin_over": {"rate": gob_over, "n": gob_over_n},
            "goblin70_over": {"rate": g70_over, "n": g70_over_n},
        },
        "result_audit": _audit_public(bundle.get("audit") or {}),
        "hit_rates": rate_table(graded),
        "goblin70": sleeve_table(graded),
        "gated_standard_flex": gated_standard_flex(graded, stake=stake, n_bets=n_bets),
        "gated_standard_props": gated_standard_props(bundle.get("prop_cells") or {}),
        "grade_extra_dates": bundle.get("extra_dates") or [],
        "goblin_quote_join": join_goblin_quotes(cdp, bundle.get("legs") or {}),
        "published_3leg": published,
        "scraped_shapes": goblin_shapes,
        "histograms": {
            "all_goblin_2": payout_histogram(cdp, 2, "0S+2G+0D"),
            "mixed_2": payout_histogram(cdp, 2, "1S+1G+0D"),
            "all_goblin_3": payout_histogram(cdp, 3, "0S+3G+0D"),
            "all_standard_3": payout_histogram(cdp, 3, "3S+0G+0D"),
        },
    }


def _print_be(be: dict[str, Any]) -> str:
    flag = "rule 9 pass" if be.get("rule9_pass") else "RULE 9 FAIL (under 2x)"
    if be.get("formula") == "3p^2":
        return (
            f"breakeven per leg {_pct(be['per_leg_rate'])} "
            f"(return is 3p^2; a 2/3 slip returns the stake), {flag}"
        )
    return (
        f"breakeven slip {_pct(be['slip_win_rate'])}, "
        f"per leg {_pct(be['per_leg_rate'])}, {flag}"
    )


def _print_hist(hist: list[dict[str, Any]]) -> None:
    if not hist:
        print("    no N-correct scrapes")
        return
    bits = [f"{row['payout_x']:g}x x{row['n']}" for row in hist]
    print("    payout counts: " + ", ".join(bits))


def print_report(report: dict[str, Any]) -> None:
    stake = report["stake"]
    bets = report["bets"]
    print(
        f"Stake ${stake:.0f} x {bets} slips (${report['wagered']:,.0f} through the month). "
        "Payouts are N-correct."
    )
    print(report["note"])
    print()
    audit = report["composition_2leg"]
    print(
        f"2-leg scrapes: {audit['n_2_leg']}. "
        f"All-Goblin {audit['all_goblin']}. "
        f"Mixed 1 Standard + 1 Goblin {audit['mixed_1s1g']}. "
        f"All-Standard {audit['all_standard']}."
    )
    print(
        f"All-Goblin 2-leg kept after the 1st-place filter: "
        f"{audit['all_goblin_n_correct_kept']} "
        f"({audit['all_goblin_first_place_dropped']} dropped)."
    )
    print(f"3-leg rows with a Flex N-correct ladder: {report['flex_3leg_ladders']}.")
    print()
    print("Payout counts")
    print("  All-Goblin 2-leg Power")
    _print_hist(report["histograms"]["all_goblin_2"])
    print("  Mixed 2-leg Power")
    _print_hist(report["histograms"]["mixed_2"])
    print("  All-Goblin 3-leg Power")
    _print_hist(report["histograms"]["all_goblin_3"])
    print("  All-Standard 3-leg Power")
    _print_hist(report["histograms"]["all_standard_3"])
    print()
    pooled = report["pooled_rates"]
    print("Graded hit rates (pooled)")
    for key, label in (
        ("standard_over", "Standard OVER"),
        ("standard_under", "Standard UNDER"),
        ("goblin_over", "Goblin OVER"),
        ("goblin70_over", "Goblin-70 OVER"),
    ):
        cell = pooled.get(key) or {}
        rate = cell.get("rate")
        if rate is None:
            print(f"  {label}: no cell with n>={MIN_CELL_N}")
        else:
            print(f"  {label}: {_pct(rate)}  n={cell.get('n')}")
    audit = report.get("result_audit") or {}
    over_audit = audit.get("standard_over") or {}
    under_audit = audit.get("standard_under") or {}
    if over_audit or under_audit:
        print(
            "  Standard results counted as decided only when HIT or MISS. "
            f"OVER push={over_audit.get('PUSH', 0)} void={over_audit.get('VOID', 0)} "
            f"other={over_audit.get('OTHER', 0)}. "
            f"UNDER push={under_audit.get('PUSH', 0)} void={under_audit.get('VOID', 0)} "
            f"other={under_audit.get('OTHER', 0)}."
        )
    print()
    g70 = report.get("goblin70") or []
    wnba = next((r for r in g70 if r["sport"] == "WNBA" and r["direction"] == "OVER"), None)
    nfl = next((r for r in g70 if r["sport"] == "NFL" and r["direction"] == "OVER"), None)
    pooled_g70 = pooled.get("goblin70_over") or {}
    if wnba and pooled_g70.get("n"):
        print(
            f"Goblin-70 OVER is mostly WNBA: {wnba['hits']}/{wnba['n']} "
            f"of the pooled {pooled_g70.get('n')} legs."
        )
    if nfl:
        print(
            f"NFL Goblin-70 OVER {nfl['hits']}/{nfl['n']} "
            f"({_pct(nfl['rate'])}), Wilson 95% lower bound {_pct(nfl.get('wilson_lo'))}. "
            "Promising. The lower bound is under the 84.5% a 1.4x 2-leg needs, "
            "so this cell has not cleared that bar."
        )
    print()
    print("2-leg Power quote and the per-leg rate that quote needs")
    join = report.get("goblin_quote_join") or {}
    for row in join.get("quote_breakeven") or []:
        print(f"  {row['quote']:g}x needs {_pct(row['per_leg_rate'])} a leg")
    extra_dates = report.get("grade_extra_dates") or []
    if extra_dates:
        print(
            "  Quote join also read graded dates missing from this tree: "
            + ", ".join(extra_dates)
            + ". Those files are not in the hit rates above."
        )
    at2 = join.get("at_least_2") or {}
    low = join.get("under_1_5") or {}
    mid = join.get("from_1_5_to_2") or {}

    def _band(label: str, band: dict) -> None:
        print(
            f"  {label}: {band.get('n_slips', 0)} slips, "
            f"gap on {band.get('slips_with_gap', 0)} "
            f"(median {band.get('median_gap')}). "
            f"Goblin-70 legs {band.get('g70_legs', 0)}, "
            f"gate fail {band.get('gate_fail_legs', 0)}, "
            f"window incomplete {band.get('unknown_legs', 0)}, "
            f"no graded row {band.get('unmatched_legs', 0)}."
        )

    print(f"  All-Goblin 2-leg slips joined: {join.get('n_slips', 0)}.")
    _band("Under 1.5x", low)
    _band("1.5x to under 2x", mid)
    _band("2x or more", at2)
    names = join.get("g70_legs_on_2x") or []
    if names:
        print(
            f"  Unique Goblin-70 legs on a 2x-or-better slip: {len(names)}. "
            "Same leg on several tickets counts once."
        )
        for leg in names:
            print(
                f"    {leg['date']} {leg['player']} {leg['prop']}: "
                f"{leg['n_slips']} slip(s), {leg['min_x']:g}x to {leg['max_x']:g}x"
            )
        print("  Goblin-70 legs do show up on 2x-plus slips. That is a candidate rule, not a ticket rule yet.")
    elif at2.get("n_slips", 0):
        print("  No Goblin-70 leg landed on a 2x-or-better slip. Goblin Power stays closed.")
    print()
    print("Gated Standard on 3-leg Flex (L5=5, L10>=8, directional D). Bar is about 58% a leg.")
    gated = report.get("gated_standard_flex") or []
    if not gated:
        print("  No gated Standard cell with n>=20.")
    for row in gated:
        mark = "clears 58% on the point estimate" if row.get("point_clears_58") else "under 58%"
        if row.get("point_clears_58") and not row.get("wilson_clears_58"):
            mark = "point estimate clears 58%; Wilson lower bound does not"
        print(
            f"  {row['label']}: {_pct(row['hit_rate'])} "
            f"({row.get('hits')}/{row['hit_rate_n']}), "
            f"Wilson lower {_pct(row.get('wilson_lo'))}, {mark}"
        )
        print(
            f"    expected {_money(row['expected_profit'])}   "
            f"cold {_money(row['cold_profit'])}   "
            f"median {_money(row['median_profit'])}   "
            f"warm {_money(row['warm_profit'])}"
        )
    props = report.get("gated_standard_props") or []
    clears = [row for row in props if row.get("point_clears_58")]
    print()
    print(f"Gated Standard by prop, n>=20. {len(props)} cells. {len(clears)} reach about 58% on the point estimate.")
    for row in clears:
        mark = "Wilson lower bound also clears" if row.get("wilson_clears_58") else "Wilson lower bound does not"
        print(
            f"  {row['sport']} {row['direction']} {row['prop']}: "
            f"{_pct(row['rate'])} ({row['hits']}/{row['n']}), "
            f"Wilson lower {_pct(row.get('wilson_lo'))}, {mark}"
        )
    print()
    print("Playbook: 3-leg, published N-correct, ungated Standard OVER")
    for row in report["published_3leg"]:
        print(f"  {row['label']}: " + " / ".join(f"{k} correct {v:g}x" for k, v in row["n_correct"].items()))
        print(f"    leg hit rate {_pct(row['hit_rate'])} (n={row['hit_rate_n']})")
        print(f"    {_print_be(row['breakeven'])}")
        print(
            f"    expected {_money(row['expected_profit'])}   "
            f"cold {_money(row['cold_profit'])}   "
            f"median {_money(row['median_profit'])}   "
            f"warm {_money(row['warm_profit'])}"
        )
    print()
    print("Scraped slips, each multiplier paired with that slip's graded leg rates")
    for row in report["scraped_shapes"]:
        where = "Goblin-70 sleeve" if row["sleeve"] else "graded sport, pick, direction"
        print(
            f"  {row['label']} [{where}]: median {row['median_x']:g}x  "
            f"scrapes={row['n_scrapes']}  matched={row['matched_slips']}"
        )
        print(f"    {_print_be(row['breakeven'])}")
        month = row.get("month") or {}
        if month:
            print(
                f"    expected {_money(month['expected'])}   "
                f"cold {_money(month['cold'])}   "
                f"median {_money(month['median'])}   "
                f"warm {_money(month['warm'])}"
            )
    print()
    print("4-leg and 6-leg shapes are omitted.")
    print()


def maybe_scrape(date: str) -> None:
    script = ROOT / "scripts" / "run_live_payout_capture.ps1"
    cmd = ["pwsh", "-NoProfile", "-File", str(script), "-UpdateOnly"]
    if date:
        cmd.extend(["-Date", date])
    print("Refreshing N-correct payouts from the live capture...")
    subprocess.run(cmd, cwd=str(ROOT), check=False)


def main() -> int:
    ap = argparse.ArgumentParser(description="Shadow a month of N-correct profit and loss")
    ap.add_argument("--stake", type=float, default=20.0)
    ap.add_argument("--bets", type=int, default=20)
    ap.add_argument("--scrape", action="store_true", help="Run the live N-correct capture first")
    ap.add_argument("--date", default="")
    ap.add_argument("--out", default=str(OUT_PATH))
    args = ap.parse_args()
    if args.scrape:
        maybe_scrape(str(args.date or "").strip())
    report = run_shadow(stake=float(args.stake), n_bets=int(args.bets))
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print_report(report)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
