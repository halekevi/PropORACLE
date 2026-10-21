#!/usr/bin/env python3
"""Row-level actual-value audit — NHL sog (display Shots On Target) + Tennis Std UNDER Aces/DF.

Mirrors scripts/all_ranked_prop_hit_rates.py graded_props discovery (main_cp first).
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO = Path(r"H:/PropORACLE")
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import prop_hit_tiers as T  # noqa: E402

MAIN = REPO.parent / "PropORACLE_main_cp"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    REPO / "ui_runner" / "templates",
    REPO / "mobile" / "www",
]


def graded_paths() -> list[Path]:
    by_date: dict[str, Path] = {}
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            if date not in by_date:
                by_date[date] = p
    return [by_date[k] for k in sorted(by_date)]


def actual_num(row: dict) -> float | None:
    for k in ("actual_value", "actual"):
        v = row.get(k)
        if v is None or v == "":
            continue
        s = str(v).strip()
        if s.lower() in ("nan", "none", "null", "—", "–", "-", "void"):
            return None
        try:
            return float(s)
        except ValueError:
            continue
    return None


def bucket(a: float) -> str:
    if a == 0:
        return "0"
    if a <= 1:
        return "(0,1]"
    if a <= 2:
        return "(1,2]"
    if a <= 3:
        return "(2,3]"
    if a <= 5:
        return "(3,5]"
    return ">5"


def summarize(label: str, rows: list[dict]) -> dict:
    hits = sum(1 for r in rows if r["_hit"])
    n = len(rows)
    known = [r["_actual"] for r in rows if r["_actual"] is not None]
    zeros = [r for r in rows if r["_actual"] == 0.0]
    nonzero = [r for r in rows if r["_actual"] is not None and r["_actual"] > 0]
    blank = [r for r in rows if r["_actual"] is None]
    z_hit = sum(1 for r in zeros if r["_hit"])
    z_miss = len(zeros) - z_hit
    nz_hit = sum(1 for r in nonzero if r["_hit"])
    buckets = Counter(bucket(a) for a in known)
    lines = Counter(str(r.get("line")) for r in rows)
    return {
        "label": label,
        "n": n,
        "hits": hits,
        "hr": (hits / n) if n else None,
        "actual_known": len(known),
        "actual_blank": len(blank),
        "actual_zero": len(zeros),
        "zero_share": (len(zeros) / len(known)) if known else None,
        "mean_actual": (sum(known) / len(known)) if known else None,
        "buckets": dict(buckets),
        "zero_actual_hits": z_hit,
        "zero_actual_misses": z_miss,
        "nonzero_n": len(nonzero),
        "nonzero_hits": nz_hit,
        "hr_excl_zero_actual": (nz_hit / len(nonzero)) if nonzero else None,
        "top_lines": lines.most_common(10),
        "sample_zero_hits": [
            {
                "date": r["_date"],
                "player": r.get("player"),
                "line": r.get("line"),
                "actual": r["_actual"],
                "result": r.get("result"),
                "prop_raw": r.get("prop"),
            }
            for r in zeros
            if r["_hit"]
        ][:12],
        "sample_nonzero": [
            {
                "date": r["_date"],
                "player": r.get("player"),
                "line": r.get("line"),
                "actual": r["_actual"],
                "result": r.get("result"),
                "prop_raw": r.get("prop"),
            }
            for r in nonzero
        ][:8],
    }


def main() -> None:
    paths = graded_paths()
    targets = {
        "nhl_std_under_sog": [],
        "nhl_std_over_sog": [],
        "nhl_goblin_over_sog": [],
        "nhl_std_under_hits": [],
        "tennis_std_under_aces": [],
        "tennis_std_under_df": [],
        "tennis_std_over_aces": [],
        "tennis_std_over_df": [],
    }
    raw_prop_nhl_sog = Counter()
    dates: list[str] = []

    for path in paths:
        date = path.stem.replace("graded_props_", "")[:10]
        dates.append(date)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            print("skip", path, exc)
            continue
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            elif side in ("U", "UNDER"):
                side = "UNDER"
            else:
                continue
            book = T.book_label(str(r.get("pick_type") or ""), side)
            prop = T.canon_prop(sport, str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            row = dict(r)
            row["_date"] = date
            row["_hit"] = res == "HIT"
            row["_actual"] = actual_num(r)
            row["_book"] = book
            row["_prop"] = prop

            if sport == "NHL" and prop == "sog":
                raw_prop_nhl_sog[str(r.get("prop"))] += 1
                if book == "Standard UNDER":
                    targets["nhl_std_under_sog"].append(row)
                elif book == "Standard OVER":
                    targets["nhl_std_over_sog"].append(row)
                elif book == "Goblin OVER":
                    targets["nhl_goblin_over_sog"].append(row)
            if sport == "NHL" and prop == "hits" and book == "Standard UNDER":
                targets["nhl_std_under_hits"].append(row)
            if sport == "Tennis":
                if prop == "aces" and book == "Standard UNDER":
                    targets["tennis_std_under_aces"].append(row)
                elif prop == "aces" and book == "Standard OVER":
                    targets["tennis_std_over_aces"].append(row)
                elif prop == "double_faults" and book == "Standard UNDER":
                    targets["tennis_std_under_df"].append(row)
                elif prop == "double_faults" and book == "Standard OVER":
                    targets["tennis_std_over_df"].append(row)

    reports = {
        "source": "graded_props_*.json (main_cp preferred, same as all_ranked_prop_hit_rates)",
        "window": {"from": dates[0] if dates else None, "to": dates[-1] if dates else None, "n_files": len(dates)},
        "nhl_sog_raw_prop_labels": raw_prop_nhl_sog.most_common(),
        "note": (
            "NHL canon prop sog displays as 'Shots On Target' in the master catalog "
            "(utils/prop_norm.py DISPLAY). Raw graded labels are usually Shots On Goal."
        ),
    }
    for k, rows in targets.items():
        reports[k] = summarize(k, rows)

    # Compare tennis to known post-backfill artifact
    bf_path = REPO / "data" / "reports" / "tennis_serve_backfill_under_inflation.json"
    if bf_path.exists():
        reports["prior_backfill_report"] = json.loads(bf_path.read_text(encoding="utf-8"))[
            "post_backfill_std_serve"
        ]

    out = REPO / "data" / "reports" / "audit_nhl_sot_tennis_serve_actuals.json"
    out.write_text(json.dumps(reports, indent=2, default=str), encoding="utf-8")

    def show(d: dict) -> None:
        hr = None if d["hr"] is None else round(100 * d["hr"], 1)
        zs = None if d["zero_share"] is None else round(100 * d["zero_share"], 1)
        hx = None if d["hr_excl_zero_actual"] is None else round(100 * d["hr_excl_zero_actual"], 1)
        mean = None if d["mean_actual"] is None else round(d["mean_actual"], 2)
        print(f"\n=== {d['label']} ===")
        print(f"n={d['n']} hits={d['hits']} hr={hr}%")
        print(
            f"actual known={d['actual_known']} blank={d['actual_blank']} "
            f"zero={d['actual_zero']} zero_share={zs}% mean={mean}"
        )
        print(f"buckets={d['buckets']}")
        print(
            f"zero→HIT={d['zero_actual_hits']} zero→MISS={d['zero_actual_misses']} "
            f"nonzero n={d['nonzero_n']} nonzero HIT={d['nonzero_hits']} HR excl zero={hx}%"
        )
        if d["sample_zero_hits"]:
            print("zero-HIT samples:", d["sample_zero_hits"][:4])
        if d["sample_nonzero"]:
            print("nonzero samples:", d["sample_nonzero"][:3])

    print(
        f"window {reports['window']['from']} -> {reports['window']['to']} "
        f"files={reports['window']['n_files']}"
    )
    print("NHL sog raw labels:", raw_prop_nhl_sog.most_common())
    for k in (
        "nhl_std_under_sog",
        "nhl_std_over_sog",
        "nhl_goblin_over_sog",
        "nhl_std_under_hits",
        "tennis_std_under_aces",
        "tennis_std_under_df",
        "tennis_std_over_aces",
        "tennis_std_over_df",
    ):
        show(reports[k])
    print("\nWrote", out)


if __name__ == "__main__":
    main()
