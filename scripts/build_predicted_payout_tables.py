#!/usr/bin/env python3
"""Fill Standard / mix / all-Goblin Power + Flex tables from PrizePicks scrapes.

N-correct only (Entry → To Win). First-place multipliers are ignored.

After enough successful live scrapes for a mix family, missing cells are
predicted by scaling the all-Standard N-correct ladder:

    scale = power_mix / POWER_STANDARD[n]
    flex_k  = FLEX_STANDARD[n][k] * scale

Writes:
  data/reports/predicted_payout_tables_<date>.json
  data/reports/predicted_payout_tables_latest.json
"""
from __future__ import annotations

import argparse
import json
import math
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from itertools import combinations_with_replacement
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
LIVE_CDP = ROOT / "ui_runner" / "data" / "payout_ladder_live_cdp.json"
RATE_CARD = ROOT / "data" / "reports" / "payout_rate_card.json"
OUT_DIR = ROOT / "data" / "reports"
OUT_LATEST = OUT_DIR / "predicted_payout_tables_latest.json"
OUT_CATALOG = OUT_DIR / "predicted_payout_mix_catalog_latest.json"

POWER_STANDARD = {2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 40.0}
FLEX_STANDARD: dict[int, dict[int, float]] = {
    2: {2: 3.0},
    3: {3: 2.25, 2: 1.25},
    4: {4: 5.0, 3: 1.5, 2: 0.4},
    5: {5: 10.0, 4: 2.0, 3: 0.4},
    6: {6: 25.0, 5: 2.0, 4: 0.4},
}

MIN_SCRAPES_OBSERVED = 3
MIN_SCRAPES_FAMILY = 5
MIN_SCRAPES_GLOBAL_FIT = 8
TYPICAL_GOBLIN_DELTA = 1.5
POWER_SCALE_FLOOR = 0.22
POWER_SCALE_CEIL = 1.0
DELTA_BINS = (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0, 5.5, 6.0)
MIN_SLIPS_PER_DELTA_BIN = 3


def _parse_comp(raw: str) -> tuple[int, int, int]:
    s = g = d = 0
    text = str(raw or "").upper().replace(" ", "")
    for part in text.replace("_", "+").split("+"):
        if not part:
            continue
        if part.endswith("S") and part[:-1].isdigit():
            s = int(part[:-1])
        elif part.endswith("G") and part[:-1].isdigit():
            g = int(part[:-1])
        elif part.endswith("D") and part[:-1].isdigit():
            d = int(part[:-1])
    if s + g + d == 0:
        m = re.match(r"(\d+)L_(\d+)G", text)
        if m:
            n_legs, n_g = int(m.group(1)), int(m.group(2))
            return max(0, n_legs - n_g), n_g, 0
    return s, g, d


def _parse_deltas(raw: object) -> list[float]:
    out: list[float] = []
    if isinstance(raw, (list, tuple)):
        seq = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return []
        seq = text.replace("|", "+").replace(",", "+").split("+")
    for part in seq:
        try:
            v = float(part)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v) and v > 0:
            out.append(v)
    return sorted(out)


def _f(raw: object) -> float | None:
    try:
        v = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    return v


def _round_x(v: float) -> float:
    return round(float(v), 4)


def _median(vals: list[float]) -> float:
    return float(statistics.median(vals))


def _parse_flex_ladder(raw: object) -> dict[int, float]:
    ladder: dict[int, float] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                hits = int(k)
                fx = float(v)
            except (TypeError, ValueError):
                continue
            if hits >= 2 and fx > 0:
                ladder[hits] = fx
        return ladder
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                try:
                    hits = int(item[0])
                    fx = float(item[1])
                except (TypeError, ValueError):
                    continue
                if hits >= 2 and fx > 0:
                    ladder[hits] = fx
    return ladder


def load_scrape_rows(path: Path | None = None) -> list[dict[str, Any]]:
    src = path or LIVE_CDP
    rows: list[dict[str, Any]] = []
    if not src.is_file():
        return rows
    try:
        payload = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return rows
    for rec in payload.get("rows") or []:
        if not isinstance(rec, dict):
            continue
        source = str(rec.get("source") or "").strip().lower()
        if source and source not in {"live_cdp", "cdp", "live", "ok"}:
            continue
        power = _f(rec.get("power_payout_x") or rec.get("power_min_x"))
        if power is None:
            continue
        s, g, d = _parse_comp(str(rec.get("leg_composition") or rec.get("composition") or ""))
        n_legs = int(rec.get("n_legs") or (s + g + d) or 0)
        if d > 0:
            continue
        if s + g:
            n_legs = s + g
        if n_legs < 2 or n_legs > 6:
            continue
        # Drop first-place / parse leaks: mix Power N-correct is below all-Standard.
        if g > 0 and n_legs in POWER_STANDARD and power > float(POWER_STANDARD[n_legs]) + 0.05:
            continue
        deltas = _parse_deltas(rec.get("goblin_deltas"))
        flex_all = _f(rec.get("flex_payout_x") or rec.get("flex_min"))
        ladder = _parse_flex_ladder(rec.get("flex_n_correct") or rec.get("flex_correct_pays"))
        if flex_all is not None and n_legs not in ladder:
            ladder[n_legs] = flex_all
        rows.append(
            {
                "n_legs": n_legs,
                "n_s": s if s + g else n_legs - g,
                "n_g": g,
                "power_x": power,
                "deltas": deltas,
                "flex_ladder": ladder,
                "date": str(rec.get("date") or ""),
            }
        )
    return rows


def load_composition_floors(path: Path | None = None) -> dict[tuple[int, int], dict[str, Any]]:
    src = path or RATE_CARD
    out: dict[tuple[int, int], dict[str, Any]] = {}
    if not src.is_file():
        return out
    try:
        card = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    floors = card.get("composition_floors") or {}
    if not isinstance(floors, dict):
        return out
    for key, meta in floors.items():
        if not isinstance(meta, dict):
            continue
        s, g, d = _parse_comp(str(key))
        n_legs = s + g + d
        if n_legs < 2 or n_legs > 6 or d > 0:
            continue
        avg = _f(meta.get("avg_min_x"))
        n = int(meta.get("n") or 0)
        if avg is None:
            continue
        out[(n_legs, g)] = {
            "power_x": avg,
            "n": n,
            "source": str(meta.get("source") or "composition_floor"),
        }
    return out


def delta_signature(deltas: list[float]) -> str:
    """Sorted per-Goblin Δ bins, e.g. [1.0, 2.0] -> '1+2'. Empty if unknown."""
    bins: list[float] = []
    for d in deltas or []:
        try:
            v = float(d)
        except (TypeError, ValueError):
            continue
        if math.isfinite(v) and v > 0:
            bins.append(nearest_delta_bin(v))
    if not bins:
        return ""
    bins.sort()
    return "+".join(f"{b:g}" for b in bins)


def aggregate_delta_cells(
    scrapes: list[dict[str, Any]],
    *,
    step_cuts: dict[str, float] | None = None,
    dpu: float | None = None,
) -> list[dict[str, Any]]:
    """One row per (composition, Goblin-Δ signature). Composition medians hide this."""
    buckets: dict[tuple[int, int, str], list[dict[str, Any]]] = defaultdict(list)
    for row in scrapes:
        n_g = int(row["n_g"])
        if n_g <= 0:
            continue
        sig = delta_signature(list(row.get("deltas") or []))
        buckets[(int(row["n_legs"]), n_g, sig)].append(row)

    out: list[dict[str, Any]] = []
    for (n_legs, n_g, sig), items in sorted(
        buckets.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2] == "", kv[0][2])
    ):
        n_s = n_legs - n_g
        powers = [float(x["power_x"]) for x in items]
        flex_partials: dict[int, list[float]] = defaultdict(list)
        flex_all: list[float] = []
        for x in items:
            ladder = x.get("flex_ladder") or {}
            if n_legs in ladder:
                flex_all.append(float(ladder[n_legs]))
            for hits, fx in (ladder or {}).items():
                flex_partials[int(hits)].append(float(fx))
        parsed = _parse_deltas(sig) if sig else []
        pred_x = None
        pred_scale = None
        if parsed and n_legs in POWER_STANDARD and (step_cuts or dpu is not None):
            pred_scale = scale_from_goblin_deltas(
                n_legs, parsed, step_cuts=step_cuts, dpu=dpu
            )
            pred_x = _round_x(float(POWER_STANDARD[n_legs]) * pred_scale)
        flex_ladder = {
            str(k): _round_x(_median(vs))
            for k, vs in sorted(flex_partials.items(), reverse=True)
        }
        out.append(
            {
                "n_legs": n_legs,
                "n_s": n_s,
                "n_g": n_g,
                "family": "all_goblin" if n_s == 0 else "mix",
                "composition": f"{n_s}S+{n_g}G",
                "goblin_delta_sig": sig or "unknown",
                "n_scrapes": len(items),
                "power_x": _round_x(_median(powers)),
                "range": [_round_x(min(powers)), _round_x(max(powers))],
                "predicted_power_x": pred_x,
                "predicted_scale": _round_x(pred_scale) if pred_scale is not None else None,
                "flex_n_scrapes": len(flex_all),
                "flex_ladder": flex_ladder,
                "flex_all_x": _round_x(_median(flex_all)) if flex_all else None,
            }
        )
    return out


def build_mix_catalog(
    *,
    observed_delta: list[dict[str, Any]],
    step_cuts: dict[str, float] | None,
    dpu: float | None,
) -> list[dict[str, Any]]:
    """Every S/G split × every Goblin-Δ multiset (all Δ bins). Predicted until scraped."""
    bins = list(DELTA_BINS)
    obs_map = {
        (str(r.get("composition")), str(r.get("goblin_delta_sig"))): r
        for r in observed_delta
        if str(r.get("goblin_delta_sig") or "") not in {"", "unknown"}
    }
    out: list[dict[str, Any]] = []
    for n in range(2, 7):
        for n_g in range(0, n + 1):
            n_s = n - n_g
            comp = f"{n_s}S+{n_g}G"
            family = "all_standard" if n_g == 0 else ("all_goblin" if n_s == 0 else "mix")
            if n_g == 0:
                ladder = {
                    str(k): float(v)
                    for k, v in sorted((FLEX_STANDARD.get(n) or {}).items(), reverse=True)
                }
                out.append(
                    {
                        "n_legs": n,
                        "n_s": n_s,
                        "n_g": 0,
                        "family": family,
                        "composition": comp,
                        "goblin_delta_sig": "",
                        "status": "canonical",
                        "n_scrapes": 0,
                        "power_x": float(POWER_STANDARD[n]),
                        "predicted_power_x": float(POWER_STANDARD[n]),
                        "flex_ladder": ladder,
                        "flex_all_x": ladder.get(str(n)),
                    }
                )
                continue
            for combo in combinations_with_replacement(bins, n_g):
                sig = "+".join(f"{b:g}" for b in combo)
                scale = scale_from_goblin_deltas(
                    n, list(combo), step_cuts=step_cuts, dpu=dpu
                )
                pred_x = _round_x(float(POWER_STANDARD[n]) * scale)
                flex_pred = scale_flex(n, scale)
                obs = obs_map.get((comp, sig))
                if obs:
                    out.append(
                        {
                            **obs,
                            "status": "observed",
                            "predicted_power_x": pred_x,
                            "predicted_scale": _round_x(scale),
                            "predicted_flex_ladder": flex_pred,
                        }
                    )
                else:
                    out.append(
                        {
                            "n_legs": n,
                            "n_s": n_s,
                            "n_g": n_g,
                            "family": family,
                            "composition": comp,
                            "goblin_delta_sig": sig,
                            "status": "predicted",
                            "n_scrapes": 0,
                            "power_x": pred_x,
                            "predicted_power_x": pred_x,
                            "predicted_scale": _round_x(scale),
                            "flex_ladder": flex_pred,
                            "flex_all_x": flex_pred.get(str(n)),
                        }
                    )
    for r in observed_delta:
        if str(r.get("goblin_delta_sig") or "") in {"", "unknown"}:
            out.append({**r, "status": "observed_unlabeled"})
    return out


def aggregate_cells(scrapes: list[dict[str, Any]]) -> dict[tuple[int, int], dict[str, Any]]:
    buckets: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in scrapes:
        buckets[(int(row["n_legs"]), int(row["n_g"]))].append(row)
    cells: dict[tuple[int, int], dict[str, Any]] = {}
    for key, items in buckets.items():
        powers = [float(x["power_x"]) for x in items]
        delta_means: list[float] = []
        flex_all: list[float] = []
        flex_partials: dict[int, list[float]] = defaultdict(list)
        for x in items:
            ds = x.get("deltas") or []
            if ds:
                delta_means.append(sum(ds) / len(ds))
            ladder = x.get("flex_ladder") or {}
            n = key[0]
            if n in ladder:
                flex_all.append(float(ladder[n]))
            for hits, fx in (ladder or {}).items():
                flex_partials[int(hits)].append(float(fx))
        n_legs, n_g = key
        cells[key] = {
            "n_legs": n_legs,
            "n_g": n_g,
            "n_scrapes": len(items),
            "power_x": _round_x(_median(powers)),
            "power_min": _round_x(min(powers)),
            "power_max": _round_x(max(powers)),
            "mean_delta": round(sum(delta_means) / len(delta_means), 3) if delta_means else None,
            "flex_all_x": _round_x(_median(flex_all)) if flex_all else None,
            "flex_n_scrapes": len(flex_all),
            "flex_partials": {
                str(k): _round_x(_median(vs))
                for k, vs in sorted(flex_partials.items(), reverse=True)
            },
        }
    return cells


def nearest_delta_bin(delta: float) -> float:
    d = abs(float(delta))
    return min(DELTA_BINS, key=lambda b: abs(b - d))


def fit_delta_step_cuts(
    scrapes: list[dict[str, Any]],
    min_bin: int = MIN_SLIPS_PER_DELTA_BIN,
) -> dict[str, Any]:
    """Each Goblin of delta D removes cut[D] of Standard Power (additive, not averaged).

    Fits (1 - power/standard) ~= sum_i cut[bin(delta_i)] with ridge least squares.
    Sparse bins (< min_bin slips) are omitted so they do not invent a cut.
    """
    y: list[float] = []
    xrows: list[list[float]] = []
    bins = list(DELTA_BINS)
    k = len(bins)
    bin_slips = [0] * k
    for row in scrapes:
        n_g = int(row["n_g"])
        n = int(row["n_legs"])
        if n_g <= 0 or n not in POWER_STANDARD:
            continue
        deltas = list(row.get("deltas") or [])
        if len(deltas) != n_g:
            continue
        base = float(POWER_STANDARD[n])
        power = float(row["power_x"])
        if power <= 0 or power > base * 1.05:
            continue
        counts = [0.0] * k
        for d in deltas:
            idx = bins.index(nearest_delta_bin(float(d)))
            counts[idx] += 1.0
            bin_slips[idx] += 1
        y.append(max(0.0, 1.0 - power / base))
        xrows.append(counts)
    cuts = [0.0] * k
    if y:
        xtx = [[0.0] * k for _ in range(k)]
        xty = [0.0] * k
        for i, yi in enumerate(y):
            for a in range(k):
                xty[a] += xrows[i][a] * yi
                for b in range(k):
                    xtx[a][b] += xrows[i][a] * xrows[i][b]
        for a in range(k):
            xtx[a][a] += 1e-6
        aug = [xtx[r][:] + [xty[r]] for r in range(k)]
        for col in range(k):
            piv = max(range(col, k), key=lambda r: abs(aug[r][col]))
            aug[col], aug[piv] = aug[piv], aug[col]
            if abs(aug[col][col]) < 1e-12:
                continue
            div = aug[col][col]
            for j in range(col, k + 1):
                aug[col][j] /= div
            for r in range(k):
                if r == col:
                    continue
                f = aug[r][col]
                for j in range(col, k + 1):
                    aug[r][j] -= f * aug[col][j]
        cuts = [max(0.0, aug[i][k]) for i in range(k)]
    step: dict[str, float] = {}
    for i, b in enumerate(bins):
        if bin_slips[i] < int(min_bin):
            continue
        step[f"{b:g}"] = round(float(cuts[i]), 4)
    return {
        "model": "per_goblin_delta_additive_cut",
        "n_slips": len(y),
        "min_slips_per_bin": int(min_bin),
        "cuts": step,
        "bin_slips": {f"{b:g}": bin_slips[i] for i, b in enumerate(bins) if bin_slips[i]},
        "note": "Ticket scale = 1 - sum(cut[delta_bin] for each Goblin). Not mean-delta * n_g.",
    }


def scale_from_goblin_deltas(
    n_legs: int,
    deltas: list[float],
    *,
    step_cuts: dict[str, float] | None,
    dpu: float | None,
) -> float:
    """Standard Power scale from per-Goblin delta cuts (fallback: dpu * that delta)."""
    base_scale = 1.0
    cuts = step_cuts or {}
    total_cut = 0.0
    for d in deltas:
        b = nearest_delta_bin(float(d))
        key = f"{b:g}"
        if key in cuts:
            total_cut += float(cuts[key])
        elif dpu is not None:
            total_cut += float(dpu) * b
    return max(POWER_SCALE_FLOOR, min(POWER_SCALE_CEIL, base_scale - total_cut))


def fit_discount_per_unit(
    scrapes: list[dict[str, Any]],
    min_fit: int = MIN_SCRAPES_GLOBAL_FIT,
) -> dict[str, Any]:
    samples: list[float] = []
    for row in scrapes:
        n_g = int(row["n_g"])
        n = int(row["n_legs"])
        if n_g <= 0 or n not in POWER_STANDARD:
            continue
        deltas = list(row.get("deltas") or [])
        if len(deltas) != n_g:
            continue
        total = sum(deltas)
        if total <= 0:
            continue
        base = float(POWER_STANDARD[n])
        power = float(row["power_x"])
        if power <= 0 or power > base * 1.05:
            continue
        samples.append(max(0.0, 1.0 - power / base) / total)
    n = len(samples)
    if n < int(min_fit):
        return {"ready": False, "n": n, "dpu": None, "need": int(min_fit) - n}
    return {
        "ready": True,
        "n": n,
        "dpu": round(float(statistics.median(samples)), 4),
        "need": 0,
    }


def scale_flex(n: int, scale: float) -> dict[str, float]:
    row = FLEX_STANDARD.get(n) or {}
    return {str(k): _round_x(float(v) * scale) for k, v in sorted(row.items(), reverse=True)}


def predict_power(
    n_legs: int,
    n_g: int,
    *,
    observed: dict[tuple[int, int], dict[str, Any]],
    floors: dict[tuple[int, int], dict[str, Any]],
    dpu: float | None,
    typical_delta: float,
    family_n: int,
    global_n: int,
    step_cuts: dict[str, float] | None = None,
    min_observed: int = MIN_SCRAPES_OBSERVED,
    min_family: int = MIN_SCRAPES_FAMILY,
    min_fit: int = MIN_SCRAPES_GLOBAL_FIT,
) -> dict[str, Any]:
    base = float(POWER_STANDARD[n_legs])
    if n_g == 0:
        return {
            "power_x": base,
            "status": "canonical",
            "n_scrapes": int((observed.get((n_legs, 0)) or {}).get("n_scrapes") or 0),
            "scale": 1.0,
        }

    obs = observed.get((n_legs, n_g))
    floor = floors.get((n_legs, n_g))
    n_scrapes = int((obs or {}).get("n_scrapes") or 0)
    floor_n = int((floor or {}).get("n") or 0)
    evidence_n = max(n_scrapes, floor_n)

    if obs and n_scrapes >= min_observed:
        power = float(obs["power_x"])
        return {
            "power_x": _round_x(power),
            "status": "observed",
            "n_scrapes": n_scrapes,
            "scale": _round_x(power / base),
            "range": [obs.get("power_min"), obs.get("power_max")],
        }
    if floor and floor_n >= min_observed:
        power = float(floor["power_x"])
        return {
            "power_x": _round_x(power),
            "status": "observed",
            "n_scrapes": floor_n,
            "scale": _round_x(power / base),
            "source": "composition_floor",
        }

    family_ready = family_n >= min_family or global_n >= min_fit
    can_formula = dpu is not None or bool(step_cuts)
    if not family_ready or not can_formula:
        need = max(0, min_family - family_n)
        if family_n >= min_family:
            need = max(0, min_observed - evidence_n)
        return {
            "power_x": None,
            "status": "need_scrapes",
            "n_scrapes": evidence_n,
            "need_scrapes": need,
            "scale": None,
        }

    mean_d = None
    if obs and obs.get("mean_delta"):
        mean_d = float(obs["mean_delta"])
    if mean_d is None:
        mean_d = float(typical_delta)
    # Each Goblin applies the cut for its own delta bin. Composition tables
    # without a signature treat every Goblin as sitting at this cell's mean delta.
    goblin_deltas = [mean_d] * int(n_g)
    scale = scale_from_goblin_deltas(
        n_legs, goblin_deltas, step_cuts=step_cuts, dpu=dpu
    )
    if obs and n_scrapes > 0:
        thin = float(obs["power_x"]) / base
        scale = 0.55 * scale + 0.45 * thin
    elif floor and floor_n > 0:
        thin = float(floor["power_x"]) / base
        scale = 0.4 * scale + 0.6 * thin
    power = _round_x(base * scale)
    return {
        "power_x": power,
        "status": "predicted",
        "n_scrapes": evidence_n,
        "scale": _round_x(scale),
        "mean_delta_used": round(mean_d, 3),
        "formula": "per_goblin_delta_cut",
    }


def build_tables(
    *,
    scrapes: list[dict[str, Any]] | None = None,
    floors: dict[tuple[int, int], dict[str, Any]] | None = None,
    min_observed: int = MIN_SCRAPES_OBSERVED,
    min_family: int = MIN_SCRAPES_FAMILY,
    min_fit: int = MIN_SCRAPES_GLOBAL_FIT,
) -> dict[str, Any]:
    scrape_rows = scrapes if scrapes is not None else load_scrape_rows()
    floor_map = floors if floors is not None else load_composition_floors()
    observed = aggregate_cells(scrape_rows)
    fit = fit_discount_per_unit(scrape_rows, min_fit=min_fit)
    step_fit = fit_delta_step_cuts(scrape_rows)
    dpu = fit.get("dpu") if fit.get("ready") else None
    step_cuts = step_fit.get("cuts") if isinstance(step_fit.get("cuts"), dict) else {}

    mix_n_by_legs: dict[int, int] = defaultdict(int)
    for row in scrape_rows:
        if int(row["n_g"]) > 0:
            mix_n_by_legs[int(row["n_legs"])] += 1
    global_mix = sum(mix_n_by_legs.values())

    typical_delta = TYPICAL_GOBLIN_DELTA
    delta_vals = [
        sum(r["deltas"]) / len(r["deltas"]) for r in scrape_rows if r.get("deltas")
    ]
    if len(delta_vals) >= 5:
        typical_delta = float(statistics.median(delta_vals))

    power_grid: list[dict[str, Any]] = []
    flex_grid: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []

    for n in range(2, 7):
        family_n = int(mix_n_by_legs.get(n, 0))
        for n_g in range(0, n + 1):
            pred = predict_power(
                n,
                n_g,
                observed=observed,
                floors=floor_map,
                dpu=float(dpu) if dpu is not None else None,
                step_cuts=step_cuts or None,
                typical_delta=typical_delta,
                family_n=family_n,
                global_n=global_mix,
                min_observed=min_observed,
                min_family=min_family,
                min_fit=min_fit,
            )
            n_s = n - n_g
            label = "all_standard" if n_g == 0 else ("all_goblin" if n_s == 0 else "mix")
            power_grid.append(
                {
                    "n_legs": n,
                    "n_s": n_s,
                    "n_g": n_g,
                    "family": label,
                    "composition": f"{n_s}S+{n_g}G",
                    **pred,
                }
            )

            scale = pred.get("scale")
            flex_obs = observed.get((n, n_g)) or {}
            flex_ladder = dict(flex_obs.get("flex_partials") or {})
            flex_status = pred["status"]
            if (
                flex_obs.get("flex_all_x")
                and int(flex_obs.get("flex_n_scrapes") or 0) >= min_observed
            ):
                flex_ladder[str(n)] = flex_obs["flex_all_x"]
                flex_status = "observed"
            elif pred["status"] in {"canonical", "observed", "predicted"} and scale:
                scaled = scale_flex(n, float(scale))
                if not flex_ladder:
                    flex_ladder = scaled
                else:
                    for k, v in scaled.items():
                        flex_ladder.setdefault(k, v)
                if pred["status"] == "predicted":
                    flex_status = "predicted"
                elif pred["status"] == "canonical":
                    flex_status = "canonical"
                else:
                    flex_status = "scaled_from_power"
            else:
                flex_ladder = {}
                flex_status = pred["status"]

            flex_grid.append(
                {
                    "n_legs": n,
                    "n_s": n_s,
                    "n_g": n_g,
                    "family": label,
                    "composition": f"{n_s}S+{n_g}G",
                    "status": flex_status,
                    "n_scrapes": int(flex_obs.get("flex_n_scrapes") or 0),
                    "ladder": flex_ladder,
                    "all_correct_x": flex_ladder.get(str(n)),
                }
            )
            coverage.append(
                {
                    "composition": f"{n_s}S+{n_g}G",
                    "n_legs": n,
                    "n_g": n_g,
                    "power_status": pred["status"],
                    "flex_status": flex_status,
                    "n_scrapes": int(pred.get("n_scrapes") or 0),
                    "need_scrapes": pred.get("need_scrapes"),
                }
            )

    mix_by_delta = aggregate_delta_cells(
        scrape_rows, step_cuts=step_cuts or None, dpu=float(dpu) if dpu is not None else None
    )
    mix_catalog = build_mix_catalog(
        observed_delta=mix_by_delta,
        step_cuts=step_cuts or None,
        dpu=float(dpu) if dpu is not None else None,
    )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "N-correct only (ignore 1st place). Standard + Goblin. Demons excluded.",
        "gates": {
            "min_scrapes_observed": min_observed,
            "min_scrapes_family_to_predict": min_family,
            "min_scrapes_global_dpu_fit": min_fit,
            "typical_goblin_delta": round(typical_delta, 3),
        },
        "standard": {
            "power": {str(k): v for k, v in POWER_STANDARD.items()},
            "flex": {
                str(n): {str(k): v for k, v in row.items()}
                for n, row in FLEX_STANDARD.items()
            },
        },
        "fit": {**fit, "delta_steps": step_fit},
        "scrape_counts": {
            "n_live_ok": len(scrape_rows),
            "n_mix": global_mix,
            "by_n_legs_mix": {str(k): v for k, v in sorted(mix_n_by_legs.items())},
        },
        "summary": {
            "n_power_cells": len(power_grid),
            "n_observed": sum(1 for r in power_grid if r["status"] == "observed"),
            "n_predicted": sum(1 for r in power_grid if r["status"] == "predicted"),
            "n_need_scrapes": sum(1 for r in power_grid if r["status"] == "need_scrapes"),
            "n_canonical": sum(1 for r in power_grid if r["status"] == "canonical"),
            "n_mix_by_delta": len(mix_by_delta),
            "n_mix_by_delta_unknown": sum(
                1 for r in mix_by_delta if r.get("goblin_delta_sig") == "unknown"
            ),
            "n_mix_catalog": len(mix_catalog),
            "n_mix_catalog_observed": sum(1 for r in mix_catalog if r.get("status") == "observed"),
            "n_mix_catalog_predicted": sum(1 for r in mix_catalog if r.get("status") == "predicted"),
        },
        "power_grid": power_grid,
        "flex_grid": flex_grid,
        "mix_by_delta": mix_by_delta,
        "mix_catalog": mix_catalog,
        "coverage": coverage,
        "notes": [
            "All-Standard cells are canonical N-correct, not scrape-fitted.",
            "power_grid / flex_grid are composition medians (3S+2G, 2S+3G, …). They hide Goblin-Δ mix.",
            "mix_by_delta is scraped (composition, Goblin-Δ) rows only.",
            "mix_catalog is every S/G split × every Goblin-Δ multiset from fitted bins.",
            "Observed = median live CDP Power N-correct (or composition floor) with n ≥ min_scrapes_observed.",
            "Predicted = Standard Power × (1 − sum of per-Goblin delta-bin cuts), blended with thin floors.",
            "Each Goblin applies cut[its Δ] from live scrapes. Not mean-Δ × n_g × one slope.",
            "Flex ladders: scraped N-correct rows when available; otherwise Standard Flex × Power scale.",
        ],
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Fill mix / all-Goblin Power+Flex tables from scrapes"
    )
    ap.add_argument("--min-observed", type=int, default=MIN_SCRAPES_OBSERVED)
    ap.add_argument("--min-family", type=int, default=MIN_SCRAPES_FAMILY)
    ap.add_argument("--min-fit", type=int, default=MIN_SCRAPES_GLOBAL_FIT)
    args = ap.parse_args()
    tables = build_tables(
        min_observed=args.min_observed,
        min_family=args.min_family,
        min_fit=args.min_fit,
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dated = OUT_DIR / f"predicted_payout_tables_{stamp}.json"
    slim = {k: v for k, v in tables.items() if k != "mix_catalog"}
    text = json.dumps(slim, indent=2)
    dated.write_text(text, encoding="utf-8")
    OUT_LATEST.write_text(text, encoding="utf-8")
    catalog = {
        "generated_at": tables["generated_at"],
        "scope": tables["scope"],
        "fit_cuts": ((tables.get("fit") or {}).get("delta_steps") or {}).get("cuts") or {},
        "n_rows": len(tables.get("mix_catalog") or []),
        "rows": tables.get("mix_catalog") or [],
    }
    OUT_CATALOG.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
    print(f"[predicted_payout] -> {dated}")
    print(f"[predicted_payout] -> {OUT_LATEST}")
    print(f"[predicted_payout] mix_catalog -> {OUT_CATALOG} n={catalog['n_rows']}")
    print(json.dumps(tables["summary"], indent=2))
    print(json.dumps(tables["scrape_counts"], indent=2))
    print(json.dumps(tables["fit"], indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
