#!/usr/bin/env python3
"""Mature-availability confound test for season_cushion_dir (same design as cover).

1. Monthly / daily populated rates → onset date
2. Restrict BOTH train and validate to post-onset only
3. Within that window: gated (season_cush>=1 / >=2) vs ungated same rows
4. Also restate Hits Allowed cover as control (expect null lift)

  py -3.14 scripts/audit_mlb_feature_onset_gates.py
"""
from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
EXPORT = _REPO / "data" / "reports" / "discover_gates_mlb_export.csv"
OUT = _REPO / "data" / "reports" / "discover_gates_mlb_season_cush_onset_check.json"

# Pitcher Goblin props that showed season_cush lifts in the ranked table
PITCHER = (
    "hits_allowed",
    "pitcher_ks",
    "walks_allowed",
    "earned_runs",
    "pitching_outs",
    "pitches_thrown",
)


def num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def wilson_low(h: int, n: int, z: float = 1.96) -> float | None:
    if n <= 0:
        return None
    p = h / n
    z2 = z * z
    denom = 1 + z2 / n
    center = p + z2 / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return round((center - spread) / denom, 4)


def hr_block(rows: list[dict], pred=None) -> dict:
    pool = rows if pred is None else [r for r in rows if pred(r)]
    n = len(pool)
    h = sum(1 for r in pool if str(r.get("hit")) in ("1", "1.0"))
    return {
        "n": n,
        "hits": h,
        "hr": round(h / n, 4) if n else None,
        "wilson_low": wilson_low(h, n),
    }


def onset_from_daily(daily: list[dict], thresh: float = 0.50, streak_need: int = 7) -> str | None:
    streak = 0
    onset = None
    for row in daily:
        if (row["rate"] or 0) >= thresh:
            streak += 1
            if streak >= streak_need and onset is None:
                onset = row["date"]
        else:
            streak = 0
    return onset


def chron_split(dates: list[str], oos_frac: float = 0.30) -> tuple[set[str], set[str]]:
    dates = sorted(dates)
    if len(dates) < 5:
        return set(dates), set(dates)
    cut = max(1, int(round(len(dates) * (1.0 - oos_frac))))
    return set(dates[:cut]), set(dates[cut:])


def feature_audit(rows: list[dict], field: str, label: str) -> dict:
    by_date = defaultdict(lambda: {"n": 0, "pop": 0})
    for r in rows:
        d = r["date"]
        by_date[d]["n"] += 1
        if num(r.get(field)) is not None:
            by_date[d]["pop"] += 1

    dates = sorted(by_date)
    daily = []
    for d in dates:
        n = by_date[d]["n"]
        p = by_date[d]["pop"]
        daily.append({"date": d, "n": n, "pop": p, "rate": round(p / n, 4) if n else None})

    monthly = defaultdict(lambda: {"n": 0, "pop": 0})
    for row in daily:
        m = row["date"][:7]
        monthly[m]["n"] += row["n"]
        monthly[m]["pop"] += row["pop"]
    monthly_out = [
        {
            "month": m,
            "n": v["n"],
            "pop": v["pop"],
            "rate": round(v["pop"] / v["n"], 4) if v["n"] else None,
        }
        for m, v in sorted(monthly.items())
    ]

    onset = onset_from_daily(daily)
    overall_n = sum(by_date[d]["n"] for d in dates)
    overall_p = sum(by_date[d]["pop"] for d in dates)

    early = dates[: len(dates) // 2]
    late = dates[len(dates) // 2 :]

    def win_rate(ds):
        n = sum(by_date[d]["n"] for d in ds)
        p = sum(by_date[d]["pop"] for d in ds)
        return {"n": n, "pop": p, "rate": round(p / n, 4) if n else None}

    return {
        "field": field,
        "label": label,
        "overall_rate": round(overall_p / overall_n, 4) if overall_n else None,
        "monthly": monthly_out,
        "onset_7d_ge50pct": onset,
        "early_half": win_rate(early),
        "late_half": win_rate(late),
        "daily": daily,
    }


def gate_post_onset(
    rows: list[dict],
    *,
    book: str,
    prop: str,
    onset: str,
    gate_name: str,
    pred,
    field: str,
) -> dict:
    """Restrict to post-onset, then chron-split train/validate inside that window."""
    post = [r for r in rows if r["book"] == book and r["prop"] == prop and r["date"] >= onset]
    dates = sorted({r["date"] for r in post})
    train_d, val_d = chron_split(dates)
    train = [r for r in post if r["date"] in train_d]
    val = [r for r in post if r["date"] in val_d]

    # Populated-only subset (fair: feature available by construction after onset,
    # but still drop any residual nulls so gate vs ungated share the same frame)
    def populated(rs):
        return [r for r in rs if num(r.get(field)) is not None]

    train_p = populated(train)
    val_p = populated(val)
    post_p = populated(post)

    out = {
        "book": book,
        "prop": prop,
        "gate": gate_name,
        "onset": onset,
        "post_dates": f"{dates[0]}..{dates[-1]}" if dates else None,
        "train_dates": f"{sorted(train_d)[0]}..{sorted(train_d)[-1]}" if train_d else None,
        "val_dates": f"{sorted(val_d)[0]}..{sorted(val_d)[-1]}" if val_d else None,
        "post_all": {
            "ungated": hr_block(post),
            "populated_ungated": hr_block(post_p),
            "gated": hr_block(post, pred),
        },
        "train_post": {
            "ungated": hr_block(train),
            "populated_ungated": hr_block(train_p),
            "gated": hr_block(train, pred),
        },
        "val_post": {
            "ungated": hr_block(val),
            "populated_ungated": hr_block(val_p),
            "gated": hr_block(val, pred),
        },
    }
    # Lift vs populated ungated on validate (the definitive number)
    g = out["val_post"]["gated"]
    u = out["val_post"]["populated_ungated"]
    if g["hr"] is not None and u["hr"] is not None:
        out["val_lift_vs_populated_ungated"] = round(g["hr"] - u["hr"], 4)
    else:
        out["val_lift_vs_populated_ungated"] = None
    # Verdict
    if g["n"] == 0 or (out["train_post"]["gated"]["n"] == 0):
        out["verdict"] = "UNTESTABLE_ZERO_ROWS"
    elif abs(out.get("val_lift_vs_populated_ungated") or 0) < 0.005 and g["n"] >= 40:
        out["verdict"] = "NULL_DATE_ARTIFACT"
    elif (out.get("val_lift_vs_populated_ungated") or 0) >= 0.03 and g["n"] >= 40 and (g.get("wilson_low") or 0) >= 0.70:
        out["verdict"] = "SIGNAL_HOLDS_POST_ONSET"
    elif (out.get("val_lift_vs_populated_ungated") or 0) >= 0.03 and g["n"] >= 40:
        out["verdict"] = "LIFT_BUT_BELOW_TICKET_BAR"
    else:
        out["verdict"] = "WEAK_OR_NULL"
    return out


def main() -> None:
    rows = list(csv.DictReader(EXPORT.open(encoding="utf-8")))
    print(f"loaded {len(rows):,} rows from {EXPORT.name}")

    # --- Cover control (expect null on Hits Allowed) ---
    cover_meta = feature_audit(rows, "cover_dir", "cover_dir")
    cover_onset = cover_meta["onset_7d_ge50pct"] or "2026-07-23"
    print("\n=== COVER (control) ===")
    print(f"overall={cover_meta['overall_rate']} onset={cover_onset}")
    for m in cover_meta["monthly"]:
        print(f"  {m['month']}: {m['rate']:.1%} n={m['n']}")

    cover_gates = []
    if cover_onset:
        cover_gates.append(
            gate_post_onset(
                rows,
                book="Goblin OVER",
                prop="hits_allowed",
                onset=cover_onset,
                gate_name="cover>=1",
                pred=lambda r: (num(r.get("cover_dir")) or -999) >= 1.0,
                field="cover_dir",
            )
        )
        cover_gates.append(
            gate_post_onset(
                rows,
                book="Goblin OVER",
                prop="pitcher_ks",
                onset=cover_onset,
                gate_name="L10>=8 + cover>=1",
                pred=lambda r: (num(r.get("l10")) or -1) >= 8
                and (num(r.get("cover_dir")) or -999) >= 1.0,
                field="cover_dir",
            )
        )

    # --- season_cushion_dir ---
    cush_meta = feature_audit(rows, "season_cushion_dir", "season_cushion_dir")
    cush_onset = cush_meta["onset_7d_ge50pct"]
    print("\n=== SEASON_CUSHION_DIR ===")
    print(f"overall={cush_meta['overall_rate']} onset={cush_onset}")
    print(f"early={cush_meta['early_half']['rate']} late={cush_meta['late_half']['rate']}")
    for m in cush_meta["monthly"]:
        print(f"  {m['month']}: {m['rate']:.1%} n={m['n']}")

    cush_gates = []
    # If no clear 50% onset, try 25% streak as softer onset, else treat full window
    onset = cush_onset
    if onset is None:
        onset = onset_from_daily(cush_meta["daily"], thresh=0.25, streak_need=7)
        print(f"fallback onset @25%: {onset}")
    if onset is None:
        # Fully available from day 1 — use first date; no date confound
        onset = min(r["date"] for r in rows)
        print(f"no onset skew — using full window from {onset}")

    thresholds = [
        ("season_cush>=1", lambda r: (num(r.get("season_cushion_dir")) or -999) >= 1.0),
        ("season_cush>=2", lambda r: (num(r.get("season_cushion_dir")) or -999) >= 2.0),
        (
            "L10>=8 + season_cush>=1",
            lambda r: (num(r.get("l10")) or -1) >= 8
            and (num(r.get("season_cushion_dir")) or -999) >= 1.0,
        ),
        (
            "L10>=8 + season_cush>=2",
            lambda r: (num(r.get("l10")) or -1) >= 8
            and (num(r.get("season_cushion_dir")) or -999) >= 2.0,
        ),
        (
            "L5=5 + season_cush>=1",
            lambda r: (num(r.get("l5")) or -1) >= 5
            and (num(r.get("season_cushion_dir")) or -999) >= 1.0,
        ),
    ]

    for prop in PITCHER:
        for gname, pred in thresholds:
            cush_gates.append(
                gate_post_onset(
                    rows,
                    book="Goblin OVER",
                    prop=prop,
                    onset=onset,
                    gate_name=gname,
                    pred=pred,
                    field="season_cushion_dir",
                )
            )

    print("\n=== COVER POST-ONSET VAL (control) ===")
    for g in cover_gates:
        vp = g["val_post"]
        print(
            f"  {g['prop']:16} {g['gate']:28} "
            f"gated={vp['gated']['hr']} n={vp['gated']['n']}  "
            f"ungated_pop={vp['populated_ungated']['hr']} n={vp['populated_ungated']['n']}  "
            f"lift={g['val_lift_vs_populated_ungated']}  {g['verdict']}"
        )

    print("\n=== SEASON_CUSH POST-ONSET VAL ===")
    # Sort by absolute lift then n
    show = sorted(
        cush_gates,
        key=lambda g: (
            0 if g["verdict"] == "UNTESTABLE_ZERO_ROWS" else 1,
            -(g.get("val_lift_vs_populated_ungated") or 0),
            -(g["val_post"]["gated"]["n"]),
        ),
    )
    for g in show:
        vp = g["val_post"]
        if vp["gated"]["n"] == 0 and g["train_post"]["gated"]["n"] == 0:
            if g["gate"] not in ("season_cush>=1", "L10>=8 + season_cush>=1"):
                continue  # skip empty noise for stricter gates
        print(
            f"  {g['prop']:16} {g['gate']:28} "
            f"val_gated={vp['gated']['hr']} n={vp['gated']['n']} wl={vp['gated']['wilson_low']}  "
            f"val_ungated_pop={vp['populated_ungated']['hr']} n={vp['populated_ungated']['n']}  "
            f"lift={g['val_lift_vs_populated_ungated']}  "
            f"train_gated_n={g['train_post']['gated']['n']}  {g['verdict']}"
        )

    # Summary counts
    verdicts = defaultdict(int)
    for g in cush_gates:
        verdicts[g["verdict"]] += 1

    payload = {
        "export": str(EXPORT),
        "n_rows": len(rows),
        "cover": {
            "availability": {k: cover_meta[k] for k in ("overall_rate", "monthly", "onset_7d_ge50pct", "early_half", "late_half")},
            "post_onset_gates": cover_gates,
        },
        "season_cushion_dir": {
            "availability": {
                k: cush_meta[k]
                for k in ("overall_rate", "monthly", "onset_7d_ge50pct", "early_half", "late_half")
            },
            "onset_used": onset,
            "post_onset_gates": cush_gates,
            "verdict_counts": dict(verdicts),
        },
        "recommendation": {
            "cover": "Do not wire. Hits Allowed null under post-onset control; Pitcher Ks+cover often untestable.",
            "season_cushion": (
                "Wire only cells with SIGNAL_HOLDS_POST_ONSET; "
                "treat NULL_DATE_ARTIFACT / UNTESTABLE as do-not-wire."
            ),
        },
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nseason_cush verdict counts: {dict(verdicts)}")
    print(f"-> {OUT}")


if __name__ == "__main__":
    main()
