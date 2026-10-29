#!/usr/bin/env python3
"""Payout vs Goblin line-discount → per-leg break-even (scraped quotes only).

Price the slip **before** you open it: look up n-legs + mean Goblin-Δ, then
compare your estimated hit rate to BE at **p25** (deep cushions) and median.

Reads ``sg_delta_payout_rate_card_latest.json`` (live_cdp / observed cells).
N-correct = power_min_x only (never 1st-place).

  py -3.14 scripts/payout_vs_discount_breakeven.py
"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
CARD = _REPO / "ui_runner" / "data" / "sg_delta_payout_rate_card_latest.json"
OUT_JSON = _REPO / "data" / "reports" / "payout_vs_discount_breakeven_latest.json"
OUT_MD = _REPO / "data" / "reports" / "payout_vs_discount_breakeven_latest.md"

# Catalog / live check-in bands — for EV illustration only, not proof of +EV.
HR_BANDS = {
    "goblin_optimistic": 0.77,
    "goblin_mid": 0.75,
    "goblin_live_checkin": 0.70,
}

# Observed deep-cushion all-Goblin prints (manual / CDP). Not medians.
OBSERVED_DEEP = [
    {"when": "2026-10-28", "n_legs": 6, "note": "deep Goblin Points stack", "power_n_correct": 2.1},
    {"when": "2026-10-28", "n_legs": 6, "note": "after reboot", "power_n_correct": 1.8},
    {"when": "2026-10-29", "n_legs": 4, "note": "LaVine/Camara/Mitchell/Johnson deep Δ", "power_n_correct": 1.6},
]


def _be(n: int, q: float) -> float | None:
    if n < 1 or q is None or q <= 0:
        return None
    return float(q ** (-1.0 / n))


def _ev(p: float, n: int, q: float) -> float:
    return (p**n) * q - 1.0


def _pct(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    return float(sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f))


def _sum_delta(sig: Any) -> float | None:
    text = str(sig or "").strip()
    if not text or text in ("—", "-", "nan", "None"):
        return None
    parts: list[float] = []
    for tok in text.replace("|", "+").replace(",", "+").split("+"):
        tok = tok.strip()
        if not tok:
            continue
        try:
            parts.append(float(tok))
        except ValueError:
            return None
    return sum(parts) if parts else None


def _mean_delta(sig: Any, n_g: int) -> float | None:
    s = _sum_delta(sig)
    if s is None or n_g < 1:
        return None
    return s / n_g


def _delta_bin(md: float | None) -> str:
    if md is None:
        return "unknown_delta"
    if md < 1.0:
        return "mean_Δ<1"
    if md < 2.0:
        return "mean_Δ_1–2"
    if md < 3.0:
        return "mean_Δ_2–3"
    return "mean_Δ≥3"


def load_all_goblin_cells(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    cells = raw.get("cells") or []
    out: list[dict[str, Any]] = []
    for c in cells:
        if not isinstance(c, dict):
            continue
        try:
            n_legs = int(c.get("n_legs") or 0)
            n_g = int(c.get("n_g") or 0)
            n_s = int(c.get("n_s") or 0)
        except (TypeError, ValueError):
            continue
        if n_legs < 2 or n_g != n_legs or n_s:
            continue
        src = str(c.get("source") or "").lower()
        status = str(c.get("status") or "").lower()
        n_live = int(c.get("n_live") or 0)
        n_samp = int(c.get("n_samples") or 0)
        if src != "live_cdp" and status != "observed" and n_live <= 0 and n_samp <= 0:
            continue
        try:
            q = float(c.get("power_min_x") or c.get("min_x") or 0)
        except (TypeError, ValueError):
            continue
        if q <= 1.0:
            continue
        sig = c.get("goblin_delta_sig")
        out.append(
            {
                "n_legs": n_legs,
                "goblin_delta_sig": sig,
                "sum_delta": _sum_delta(sig),
                "mean_delta": _mean_delta(sig, n_g),
                "power_min_x": q,
                "min_x": float(c.get("min_x") or q),
                "max_x": float(c.get("max_x") or q),
                "n_samples": n_samp,
                "n_live": n_live,
                "source": c.get("source"),
                "status": c.get("status"),
                "confidence": c.get("confidence"),
                "breakeven_per_leg": _be(n_legs, q),
                "ev_at": {k: _ev(p, n_legs, q) for k, p in HR_BANDS.items()},
            }
        )
    return out


def _quote_stats(n: int, vals: list[float]) -> dict[str, Any]:
    qs = sorted(float(v) for v in vals)
    med = float(median(qs))
    p25 = _pct(qs, 0.25)
    p10 = _pct(qs, 0.10)
    return {
        "n": len(qs),
        "median_power_min_x": med,
        "p25_power_min_x": p25,
        "p10_power_min_x": p10,
        "min_power_min_x": qs[0],
        "max_power_min_x": qs[-1],
        "breakeven_at_median": _be(n, med),
        "breakeven_at_p25": _be(n, p25) if p25 else None,
        "breakeven_at_p10": _be(n, p10) if p10 else None,
        "quotes": qs,
        "ev_at_median": {name: _ev(p, n, med) for name, p in HR_BANDS.items()},
        "ev_at_p25": (
            {name: _ev(p, n, p25) for name, p in HR_BANDS.items()} if p25 else {}
        ),
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_n: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_n[int(r["n_legs"])].append(r)

    by_n_out: dict[str, Any] = {}
    for n, rs in sorted(by_n.items()):
        qs = [float(r["power_min_x"]) for r in rs]
        top = _quote_stats(n, qs)
        bins: dict[str, list[float]] = defaultdict(list)
        for r in rs:
            bins[_delta_bin(r.get("mean_delta"))].append(float(r["power_min_x"]))
        bin_stats = {k: _quote_stats(n, vals) for k, vals in sorted(bins.items())}
        by_n_out[str(n)] = {
            **{f: top[f] for f in top if f != "quotes"},
            "n_cells": len(rs),
            "quotes": top["quotes"],
            "by_mean_delta": bin_stats,
        }
    return by_n_out


def render_md(payload: dict[str, Any]) -> str:
    lines = [
        "# Payout vs Goblin line-discount → break-even",
        "",
        f"generated_at={payload['generated_at']}",
        f"card=`{payload['card_path']}`",
        "",
        "N-correct = `power_min_x` only (never 1st-place).",
        "All-Goblin Power cells only (`n_g == n_legs`, `n_s == 0`).",
        "",
        "**Price before you open the slip.** Use **p25** (or the live",
        "`N correct pays` line) for deep-cushion Goblins — not the median.",
        "Tonight-style deep stacks have printed **1.6x–2.1x**.",
        "",
        "Hit-rate bands are **illustrative** (catalog / check-in). They are not",
        "a claim that any Goblin cell is +EV until a cell clears its own",
        "scraped-quote break-even with a Wilson band.",
        "",
        f"Illustrative p: mid={HR_BANDS['goblin_mid']:.0%}, "
        f"optimistic={HR_BANDS['goblin_optimistic']:.0%}, "
        f"live_checkin={HR_BANDS['goblin_live_checkin']:.0%}.",
        "",
        "## Pre-slip lookup (use p25 BE)",
        "",
        "| n | mean Δ bin | p25 Nx | BE@p25 | median Nx | BE@med | skip if live < |",
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for n, block in sorted(payload["by_n_legs"].items(), key=lambda x: int(x[0])):
        for k, b in block["by_mean_delta"].items():
            p25 = b.get("p25_power_min_x")
            be25 = b.get("breakeven_at_p25")
            med = b["median_power_min_x"]
            bem = b["breakeven_at_median"]
            # Floor guidance: deep Δ (≥3) → require near median of shallow or ≥4x for 4-leg
            skip = p25 if p25 is not None else med
            lines.append(
                f"| {n} | {k} | "
                f"{(p25 or 0):.2f}x | {(be25 or 0):.1%} | "
                f"{med:.2f}x | {(bem or 0):.1%} | "
                f"~{skip:.1f}x |"
            )
    lines += [
        "",
        "## Observed deep-cushion prints (not rate-card medians)",
        "",
        "| when | n | N-correct | BE/leg | note |",
        "|---|---:|---:|---:|---|",
    ]
    for o in OBSERVED_DEEP:
        q = float(o["power_n_correct"])
        n = int(o["n_legs"])
        lines.append(
            f"| {o['when']} | {n} | {q:.1f}x | {_be(n, q):.1%} | {o['note']} |"
        )
    lines += [
        "",
        "## By leg count",
        "",
        "| n | cells | p25 Nx | BE@p25 | median Nx | BE@med | min–max | EV@75% med | EV@70% p25 |",
        "|---:|---:|---:|---:|---:|---:|---|---:|---:|",
    ]
    for n, block in sorted(payload["by_n_legs"].items(), key=lambda x: int(x[0])):
        p25 = block.get("p25_power_min_x")
        be25 = block.get("breakeven_at_p25")
        ev_m = block["ev_at_median"]
        ev_p = block.get("ev_at_p25") or {}
        lines.append(
            f"| {n} | {block['n_cells']} | "
            f"{(p25 or 0):.2f}x | {(be25 or 0):.1%} | "
            f"{block['median_power_min_x']:.2f}x | "
            f"{(block.get('breakeven_at_median') or 0):.1%} | "
            f"{block['min_power_min_x']:.2f}–{block['max_power_min_x']:.2f} | "
            f"{ev_m['goblin_mid']:+.3f} | "
            f"{ev_p.get('goblin_live_checkin', float('nan')):+.3f} |"
        )
    lines += [
        "",
        "## Deeper discount → lower payout (same n)",
        "",
        "Mean Δ = average Goblin line distance vs Standard face on the slip.",
        "",
    ]
    for n, block in sorted(payload["by_n_legs"].items(), key=lambda x: int(x[0])):
        lines.append(f"### {n}-leg all-Goblin")
        lines.append("")
        lines.append(
            "| mean Δ bin | n | p25 Nx | BE@p25 | median Nx | BE@med | EV@75% med | EV@70% p25 |"
        )
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
        for k, b in block["by_mean_delta"].items():
            p25 = b.get("p25_power_min_x")
            be25 = b.get("breakeven_at_p25")
            ev_m = b["ev_at_median"]
            ev_p = b.get("ev_at_p25") or {}
            lines.append(
                f"| {k} | {b['n']} | "
                f"{(p25 or 0):.2f}x | {(be25 or 0):.1%} | "
                f"{b['median_power_min_x']:.2f}x | "
                f"{(b.get('breakeven_at_median') or 0):.1%} | "
                f"{ev_m['goblin_mid']:+.3f} | "
                f"{ev_p.get('goblin_live_checkin', float('nan')):+.3f} |"
            )
        lines.append("")
    low_quote_notes: list[str] = []
    for n, block in sorted(payload["by_n_legs"].items(), key=lambda x: int(x[0])):
        for k, b in block["by_mean_delta"].items():
            for label, be_key, q_key in (
                ("p25", "breakeven_at_p25", "p25_power_min_x"),
                ("median", "breakeven_at_median", "median_power_min_x"),
            ):
                be = b.get(be_key)
                q = b.get(q_key)
                if be is not None and q is not None and be >= 0.78:
                    low_quote_notes.append(
                        f"- **{n}-leg {k} ({label})**: {q:.2f}x → BE/leg **{be:.1%}**"
                    )
    lines += [
        "## Verdict",
        "",
        "- Hit rate ≠ return. A 70–77% Goblin catalog band is **not** +EV at the",
        "  quotes deep-cushion slips actually get.",
        "- Deeper mean-Δ bins often pay less and need a **higher** per-leg hit rate —",
        "  the cushion that lifts HR is the same discount that raises break-even.",
        "- **Pre-slip rule:** if the live `N correct pays` is under the bin’s p25",
        "  (or under ~4x on a 4-Goblin), **pass** — do not submit on median hope.",
        "- Production floor for all-Goblin 3 is the scrape **p25 (~1.65x → BE ~84.6%)**,",
        "  not the rate-card median.",
        "- Do **not** claim any Goblin keep cell clears break-even until that cell’s",
        "  forward HR Wilson-low ≥ BE at the **actual** N-correct for its Δ mix.",
        "",
        "### High break-even bins (BE/leg ≥ 78%)",
        "",
    ]
    if low_quote_notes:
        lines.extend(low_quote_notes)
    else:
        lines.append("- (none at p25/median; check min quotes / live slip N-correct)")
    lines += [
        "",
        f"n_all_goblin_cells={payload['n_rows']}",
        f"any_median_ev_positive_at_75pct={payload['any_median_ev_positive_at_75pct']}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    if not CARD.is_file():
        print(f"ERROR: missing {CARD}")
        return 1
    rows = load_all_goblin_cells(CARD)
    by_n = summarize(rows)
    any_pos = any(
        block["ev_at_median"]["goblin_mid"] > 0 for block in by_n.values()
    )
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "card_path": str(CARD),
        "n_rows": len(rows),
        "hr_bands_illustrative": HR_BANDS,
        "observed_deep_cushion": OBSERVED_DEEP,
        "any_median_ev_positive_at_75pct": any_pos,
        "by_n_legs": by_n,
        "rows": rows,
        "verdict": (
            "no_median_all_goblin_cell_is_plus_ev_at_illustrative_75pct"
            if not any_pos
            else "some_medians_positive_at_75pct_still_not_a_cell_claim"
        ),
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_md(payload), encoding="utf-8")
    print(OUT_MD.read_text(encoding="utf-8"))
    print(f"\nWROTE {OUT_MD}")
    print(f"WROTE {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
