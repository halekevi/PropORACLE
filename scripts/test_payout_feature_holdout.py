#!/usr/bin/env python3
"""Log scraped slips with per-leg features and test payout prediction.

Held-out by date. Compares three pre-scrape quotes against the scraped
Power N-correct:

  * composition low percentile (the long-slip rule)
  * composition median (the old high-biased quote)
  * a linear model on leg features (sport, prop, line, gap, side, pick)

If the feature model does not beat the low percentile on 4+ leg slips,
those slips are not predictable from the features we store. Price them
by scraping the slip at submit time.
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT)]

from utils.n_correct_payout import (  # noqa: E402
    MIN_SCRAPES_FOR_PERCENTILE,
    MIN_SCRAPES_FOR_P25,
    QUOTE_EXCLUDED_DATES,
    _percentile,
)

REPORTS = ROOT / "data" / "reports"
LOG_PATH = REPORTS / "payout_scrape_legs.jsonl"
SPORTS = ("MLB", "WNBA", "TENNIS", "SOCCER", "NBA", "NFL", "CFB", "NHL")


def _f(raw: object) -> float | None:
    try:
        v = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return v


def _gap(leg: dict) -> float | None:
    for key in ("line_distance", "delta", "gap"):
        v = _f(leg.get(key))
        if v is not None and v > 0:
            return v
    line = _f(leg.get("line"))
    std = _f(leg.get("standard_line"))
    if line is None or std is None:
        return None
    d = abs(std - line)
    return d if d > 0 else None


def _power(rec: dict) -> float | None:
    for key in ("power_min_x", "power_payout_x", "min_x"):
        v = _f(rec.get(key))
        if v is not None and v > 0:
            return v
    return None


def _leg_row(leg: dict) -> dict | None:
    if not isinstance(leg, dict):
        return None
    prop = str(leg.get("prop_type") or leg.get("prop") or "").strip()
    if not prop and not leg.get("player"):
        return None
    return {
        "sport": str(leg.get("sport") or "").strip().upper(),
        "prop": prop,
        "line": leg.get("line"),
        "side": str(leg.get("direction") or leg.get("side") or "").strip().upper(),
        "pick_type": str(leg.get("pick_type") or "").strip(),
        "gap": _gap(leg),
    }


def _slip_row(rec: dict, date: str) -> dict | None:
    if not isinstance(rec, dict):
        return None
    status = str(rec.get("status") or "ok").lower()
    if status not in {"ok", "partial", ""}:
        return None
    power = _power(rec)
    if power is None:
        return None
    legs_raw = rec.get("legs") if isinstance(rec.get("legs"), list) else rec.get("lines")
    legs = [x for x in (_leg_row(l) for l in legs_raw or []) if x]
    n = len(legs) or int(_f(rec.get("n_legs")) or 0)
    if n < 2:
        return None
    n_g = sum(1 for l in legs if "gob" in l["pick_type"].lower())
    n_s = sum(1 for l in legs if "std" in l["pick_type"].lower() or l["pick_type"].lower() == "standard")
    n_d = sum(1 for l in legs if "demon" in l["pick_type"].lower())
    if legs and n_s + n_g + n_d < n:
        n_g += n - (n_s + n_g + n_d)
    return {
        "date": str(rec.get("date") or date)[:10],
        "captured_at": str(rec.get("captured_at") or ""),
        "ticket_id": str(rec.get("ticket_id") or ""),
        "n_legs": n,
        "n_s": n_s,
        "n_g": n_g,
        "n_d": n_d,
        "power_min_x": power,
        "legs": legs,
    }


def load_slips() -> list[dict]:
    rows: list[dict] = []
    paths = sorted(REPORTS.glob("payout_capture*.json"))
    paths += sorted(REPORTS.glob("payout_pre_tip_capture*.json"))
    paths += sorted(REPORTS.glob("payout_ladder_validation_capture_*.json"))
    paths += sorted(REPORTS.glob("payout_scrape_log_*.jsonl"))
    for path in paths:
        if path.suffix == ".jsonl":
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                date = str(rec.get("captured_at") or path.stem)[:10]
                row = _slip_row(rec, date)
                if row:
                    rows.append(row)
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        date = str(payload.get("date") or "")[:10]
        slips = payload.get("slips") if isinstance(payload, dict) else None
        if not isinstance(slips, list):
            continue
        for rec in slips:
            row = _slip_row(rec, date)
            if row:
                rows.append(row)
    dedup: dict[tuple, dict] = {}
    for row in rows:
        if row["date"] in QUOTE_EXCLUDED_DATES:
            continue
        tid = str(row.get("ticket_id") or "").strip()
        # One slip once. Two reads of the same ticket (2.0x and 2.1x) were
        # both kept when the key included the payout.
        if tid:
            key = (row["date"], tid, row["n_legs"])
        else:
            key = (row["date"], "", round(row["power_min_x"], 4), row["n_legs"])
        dedup[key] = row
    return list(dedup.values())


def _cell(row: dict) -> tuple[int, int, int]:
    return (int(row["n_legs"]), int(row["n_s"]), int(row["n_g"]))


def _quote(samples: list[float], *, low: bool) -> float | None:
    if not samples:
        return None
    if low:
        if len(samples) < MIN_SCRAPES_FOR_PERCENTILE:
            return min(samples)
        return _percentile(samples, 10.0)
    xs = sorted(samples)
    mid = len(xs) // 2
    if len(xs) % 2:
        return xs[mid]
    return (xs[mid - 1] + xs[mid]) / 2.0


def _features(row: dict) -> list[float]:
    legs = row["legs"]
    gaps = [float(l["gap"]) for l in legs if l.get("gap") not in (None, "")]
    lines = []
    for l in legs:
        v = _f(l.get("line"))
        if v is not None:
            lines.append(v)
    sports = [str(l.get("sport") or "") for l in legs]
    sides = [str(l.get("side") or "") for l in legs]
    vec = [
        float(row["n_legs"]),
        float(row["n_g"]),
        float(row["n_s"]),
        sum(gaps),
        (sum(gaps) / len(gaps)) if gaps else 0.0,
        max(gaps) if gaps else 0.0,
        (sum(lines) / len(lines)) if lines else 0.0,
        float(len({s for s in sports if s})),
        float(sum(1 for s in sides if s == "OVER")),
        float(sum(1 for s in sides if s == "UNDER")),
    ]
    for sport in SPORTS:
        vec.append(float(sum(1 for s in sports if s == sport)))
    vec.append(float(sum(1 for s in sports if s and s not in SPORTS)))
    return vec


def _fit_linear(train: list[dict]) -> list[float] | None:
    usable = [r for r in train if r["legs"]]
    if len(usable) < 30:
        return None
    dim = len(_features(usable[0]))
    # Least squares on [1 | features] -> power. No sklearn dependency.
    a = []
    y = []
    for row in usable:
        a.append([1.0] + _features(row))
        y.append(row["power_min_x"])
    # Normal equations via a simple Gaussian elimination on A'A.
    cols = dim + 1
    ata = [[0.0] * cols for _ in range(cols)]
    aty = [0.0] * cols
    for row_a, yi in zip(a, y):
        for i in range(cols):
            aty[i] += row_a[i] * yi
            for j in range(cols):
                ata[i][j] += row_a[i] * row_a[j]
    for i in range(cols):
        ata[i][i] += 1.0
    # Solve ata * beta = aty
    m = [ata[i][:] + [aty[i]] for i in range(cols)]
    for c in range(cols):
        pivot = max(range(c, cols), key=lambda r: abs(m[r][c]))
        if abs(m[pivot][c]) < 1e-12:
            return None
        m[c], m[pivot] = m[pivot], m[c]
        div = m[c][c]
        for j in range(c, cols + 1):
            m[c][j] /= div
        for r in range(cols):
            if r == c:
                continue
            factor = m[r][c]
            for j in range(c, cols + 1):
                m[r][j] -= factor * m[c][j]
    return [m[i][cols] for i in range(cols)]


def _predict_linear(beta: list[float] | None, row: dict) -> float | None:
    if beta is None or not row["legs"]:
        return None
    x = [1.0] + _features(row)
    if len(x) != len(beta):
        return None
    return sum(b * v for b, v in zip(beta, x))


def _mae(pairs: list[tuple[float, float]]) -> dict[str, float]:
    if not pairs:
        return {"n": 0, "mae": float("nan"), "bias": float("nan"), "high_share": float("nan")}
    err = [p - a for p, a in pairs]
    high = sum(1 for p, a in pairs if p > a + 0.05)
    return {
        "n": float(len(pairs)),
        "mae": sum(abs(e) for e in err) / len(err),
        "bias": sum(err) / len(err),
        "high_share": high / len(err),
    }


def main() -> int:
    slips = [r for r in load_slips() if r["n_d"] == 0]
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("w", encoding="utf-8") as fh:
        for row in sorted(slips, key=lambda r: (r["date"], r["ticket_id"])):
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    dates = sorted({r["date"] for r in slips if r["date"]})
    if len(dates) < 4:
        print(f"too few dates ({len(dates)}) to hold out")
        return 1
    cut = dates[max(1, int(len(dates) * 0.7))]
    train = [r for r in slips if r["date"] < cut]
    test = [r for r in slips if r["date"] >= cut]
    by_cell: dict[tuple[int, int, int], list[float]] = defaultdict(list)
    for row in train:
        by_cell[_cell(row)].append(row["power_min_x"])
    beta = _fit_linear(train)

    def collect(rows: list[dict]) -> dict[str, list[tuple[float, float]]]:
        out = {"low": [], "median": [], "features": []}
        for row in rows:
            actual = row["power_min_x"]
            samples = by_cell.get(_cell(row)) or []
            low = _quote(samples, low=True)
            med = _quote(samples, low=False)
            feat = _predict_linear(beta, row)
            if low is not None:
                out["low"].append((low, actual))
            if med is not None:
                out["median"].append((med, actual))
            if feat is not None and feat > 0:
                out["features"].append((feat, actual))
        return out

    print(f"slips={len(slips)} train_dates<{cut} train={len(train)} test={len(test)}")
    print(f"log -> {LOG_PATH}")
    for label, rows in (("2-3 legs", [r for r in test if r["n_legs"] <= 3]), ("4+ legs", [r for r in test if r["n_legs"] >= 4])):
        print(f"\n{label}")
        stats = collect(rows)
        for name, pairs in stats.items():
            s = _mae(pairs)
            print(
                f"  {name:8} n={int(s['n']):4}  mae={s['mae']:.2f}  "
                f"bias={s['bias']:+.2f}  share_high={s['high_share']:.0%}"
            )
    long_train = [r for r in train if r["n_legs"] >= 4]
    print(f"\ntrain 4+ legs with per-leg rows: {sum(1 for r in long_train if r['legs'])} / {len(long_train)}")
    short = [r for r in test if r["n_legs"] <= 3]

    def flips(name: str, pick) -> None:
        high = floor = priced = 0
        for row in short:
            samples = by_cell.get(_cell(row)) or []
            quote = pick(samples)
            if quote is None:
                continue
            priced += 1
            actual = row["power_min_x"]
            if quote >= 1.4 and actual < 1.4:
                floor += 1
            elif quote > actual + 1e-9:
                high += 1
        print(
            f"  {name:8} priced={priced:3}  ev_flip={high:3}  "
            f"floor_flip={floor:3}  (quote clears, scrape does not)"
        )

    def drops(name: str, pick) -> None:
        """Slips the scrape would keep (payout >= 1.4x) that this quote drops."""
        priced = kept = floor_drop = ev_drop = 0
        g2 = g2_kept = g2_floor = g2_ev = 0
        for row in short:
            samples = by_cell.get(_cell(row)) or []
            quote = pick(samples)
            if quote is None:
                continue
            priced += 1
            actual = row["power_min_x"]
            is_g2 = row["n_legs"] == 2 and row["n_s"] == 0 and row["n_g"] == 2
            if is_g2:
                g2 += 1
            if actual < 1.4:
                continue
            kept += 1
            if is_g2:
                g2_kept += 1
            if quote < 1.4:
                floor_drop += 1
                if is_g2:
                    g2_floor += 1
            elif quote + 1e-9 < actual:
                ev_drop += 1
                if is_g2:
                    g2_ev += 1
        print(
            f"  {name:8} scrape_clears={kept:3}/{priced}  "
            f"floor_drop={floor_drop:3}  ev_band_drop={ev_drop:3}  "
            f"| 2-leg Goblin {g2_kept}/{g2} clear, "
            f"floor_drop={g2_floor} ev_band_drop={g2_ev}"
        )

    print("\n2-3 leg drops on the same held-out slips")
    print("  floor_drop: scrape >= 1.4x and the quote is under 1.4x.")
    print("  ev_band_drop: both clear 1.4x, but the quote is lower, so some win")
    print("  probabilities pass the scrape and fail the quote.")
    drops("p25", lambda s: (min(s) if len(s) < MIN_SCRAPES_FOR_P25 else _percentile(s, 25.0)) if s else None)
    drops("min", lambda s: min(s) if s else None)

    print("\n2-3 leg decision flips on the held-out slips")
    print("  A flip is a win probability that beats 1/quote and loses to 1/scrape.")
    flips("median", lambda s: _quote(s, low=False))
    flips("p25", lambda s: (min(s) if len(s) < MIN_SCRAPES_FOR_P25 else _percentile(s, 25.0)) if s else None)
    flips("min", lambda s: min(s) if s else None)
    print("\n6-leg composition range (all logged scrapes)")
    cells: dict[tuple, list[float]] = defaultdict(list)
    for row in slips:
        if row["n_legs"] != 6:
            continue
        cells[(row["n_s"], row["n_g"])].append(row["power_min_x"])
    for key in sorted(cells):
        xs = sorted(cells[key])
        print(
            f"  {key[0]}S+{key[1]}G n={len(xs)} "
            f"min={xs[0]:g} p10={_percentile(xs, 10):.2f} median={_quote(xs, low=False):.2f} max={xs[-1]:g}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
