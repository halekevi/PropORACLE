#!/usr/bin/env python3
"""Row-level actual audit: Soccer Passes Attempted + Clearances (Std OVER/UNDER)."""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(r"H:/PropORACLE")
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import display_prop  # noqa: E402

MAIN = REPO.parent / "PropORACLE_main_cp"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    REPO / "ui_runner" / "templates",
    REPO / "mobile" / "www",
]
TARGETS = frozenset({"passes", "clearances", "tackles", "crosses", "attempted_dribbles", "shots_assisted"})


def graded_paths():
    by = {}
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            by.setdefault(date, p)
    return [by[k] for k in sorted(by)]


def actual_num(row):
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


def bucket(a):
    if a == 0:
        return "0"
    if a <= 1:
        return "(0,1]"
    if a <= 5:
        return "(1,5]"
    if a <= 20:
        return "(5,20]"
    if a <= 50:
        return "(20,50]"
    return ">50"


def main():
    cells = defaultdict(list)
    raw_labels = defaultdict(Counter)
    dates = []
    for path in graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        dates.append(date)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            if sport != "Soccer":
                continue
            side = str(r.get("direction") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            elif side in ("U", "UNDER"):
                side = "UNDER"
            else:
                continue
            book = T.book_label(str(r.get("pick_type") or ""), side)
            prop_raw = str(r.get("prop") or "")
            canon = T.canon_prop(sport, prop_raw)
            if canon not in TARGETS:
                continue
            raw_labels[canon][prop_raw] += 1
            row = dict(r)
            row["_date"] = date
            row["_hit"] = res == "HIT"
            row["_actual"] = actual_num(r)
            row["_line"] = r.get("line")
            cells[(book, canon)].append(row)

    reports = {
        "window": {"from": dates[0] if dates else None, "to": dates[-1] if dates else None, "n_files": len(dates)},
        "raw_labels": {k: v.most_common() for k, v in raw_labels.items()},
        "cells": {},
    }
    for (book, canon), rows in sorted(cells.items()):
        known = [r["_actual"] for r in rows if r["_actual"] is not None]
        zeros = [r for r in rows if r["_actual"] == 0.0]
        blank = [r for r in rows if r["_actual"] is None]
        nonzero = [r for r in rows if r["_actual"] is not None and r["_actual"] > 0]
        hits = sum(1 for r in rows if r["_hit"])
        z_hit = sum(1 for r in zeros if r["_hit"])
        z_miss = len(zeros) - z_hit
        nz_hit = sum(1 for r in nonzero if r["_hit"])
        lines = Counter(str(r.get("line")) for r in rows)
        reports["cells"][f"{book}::{canon}"] = {
            "book": book,
            "canon": canon,
            "display": display_prop(canon, sport="Soccer"),
            "n": len(rows),
            "hits": hits,
            "hr": hits / len(rows) if rows else None,
            "actual_known": len(known),
            "actual_blank": len(blank),
            "actual_zero": len(zeros),
            "zero_share": len(zeros) / len(known) if known else None,
            "mean_actual": sum(known) / len(known) if known else None,
            "median_hint": sorted(known)[len(known) // 2] if known else None,
            "buckets": dict(Counter(bucket(a) for a in known)),
            "zero_HIT": z_hit,
            "zero_MISS": z_miss,
            "nonzero_n": len(nonzero),
            "nonzero_hits": nz_hit,
            "hr_excl_zero": nz_hit / len(nonzero) if nonzero else None,
            "top_lines": lines.most_common(8),
            "sample_zero": [
                {
                    "date": r["_date"],
                    "player": r.get("player"),
                    "line": r.get("line"),
                    "actual": r["_actual"],
                    "result": r.get("result"),
                    "prop": r.get("prop"),
                }
                for r in zeros[:8]
            ],
            "sample_nonzero": [
                {
                    "date": r["_date"],
                    "player": r.get("player"),
                    "line": r.get("line"),
                    "actual": r["_actual"],
                    "result": r.get("result"),
                }
                for r in nonzero[:6]
            ],
        }

    out = REPO / "data" / "reports" / "audit_soccer_passes_clearances_actuals.json"
    out.write_text(json.dumps(reports, indent=2, default=str), encoding="utf-8")

    print(f"window {reports['window']}")
    print("raw labels:", reports["raw_labels"])
    for key, d in reports["cells"].items():
        if d["canon"] not in ("passes", "clearances") and d["n"] < 20:
            continue
        zs = None if d["zero_share"] is None else round(100 * d["zero_share"], 1)
        hr = None if d["hr"] is None else round(100 * d["hr"], 1)
        hx = None if d["hr_excl_zero"] is None else round(100 * d["hr_excl_zero"], 1)
        mean = None if d["mean_actual"] is None else round(d["mean_actual"], 2)
        print(
            f"\n{key} n={d['n']} hr={hr}% zero_share={zs}% mean={mean} "
            f"blank={d['actual_blank']} zero→HIT={d['zero_HIT']} zero→MISS={d['zero_MISS']} "
            f"HR excl zero={hx}% buckets={d['buckets']}"
        )
        if d["sample_zero"][:2]:
            print("  zero samples:", d["sample_zero"][:2])
        if d["sample_nonzero"][:2]:
            print("  nonzero:", d["sample_nonzero"][:2])
    print("Wrote", out)


if __name__ == "__main__":
    main()
