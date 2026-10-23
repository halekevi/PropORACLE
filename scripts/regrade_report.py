"""Read-only report: what the grade-cell exclusions would change in the stored graded JSONs.

Does not write any graded_props_*.json. Writes ``data/reports/regrade_report.json`` and prints
a markdown summary.

    py -3.14 scripts/regrade_report.py [--templates ui_runner/templates] [--out data/reports/regrade_report.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.grade_cell_exclusions import CELLS, exclusion_reasons, norm_prop  # noqa: E402

DECIDED = ("HIT", "MISS")


def load(templates: Path) -> pd.DataFrame:
    frames = []
    for p in sorted(templates.glob("graded_props_*.json")):
        try:
            blob = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        rows = blob.get("props") if isinstance(blob, dict) else None
        if not rows:
            continue
        f = pd.DataFrame(rows)
        f["_date"] = p.stem.replace("graded_props_", "")
        frames.append(f)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def rate(df: pd.DataFrame) -> dict:
    d = df[df["result"].isin(DECIDED)]
    n = int(len(d))
    h = int((d["result"] == "HIT").sum())
    return {"n": n, "hit": h, "hr": round(h / n, 4) if n else None}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--templates", default=str(ROOT / "ui_runner" / "templates"))
    ap.add_argument("--out", default=str(ROOT / "data" / "reports" / "regrade_report.json"))
    a = ap.parse_args()

    df = load(Path(a.templates))
    if df.empty:
        print("no graded rows", file=sys.stderr)
        return 1
    df["result"] = df["result"].astype(str).str.strip().str.upper()
    df["p"] = df["prop"].map(norm_prop)
    df["reason"] = exclusion_reasons(df)
    df["excluded"] = df["reason"].ne("")
    changed = df[df["excluded"] & df["result"].isin(("HIT", "MISS", "PUSH"))]

    per_cell = (
        changed.groupby(["reason", "sport", "p", "pick_type", "direction", "result"])
        .size()
        .rename("rows")
        .reset_index()
    )
    per_prop = (
        changed.groupby(["sport", "p", "reason"])
        .agg(rows_changing=("result", "size"), hit=("result", lambda s: int((s == "HIT").sum())),
             miss=("result", lambda s: int((s == "MISS").sum())),
             dates=("_date", "nunique"), first=("_date", "min"), last=("_date", "max"))
        .reset_index()
    )
    per_prop["hr_before"] = (per_prop["hit"] / (per_prop["hit"] + per_prop["miss"]).replace(0, np.nan)).round(4)

    before_after: dict[str, dict] = {}
    for sport in ("Tennis", "NFL", "CFB", "NHL"):
        s = df[df["sport"].astype(str).str.upper() == sport.upper()]
        if s.empty:
            continue
        kept = s[~s["excluded"]]
        entry = {"all": {"before": rate(s), "after": rate(kept)}, "by_prop": {}, "by_pick_type": {}}
        for p, g in s.groupby("p"):
            b, af = rate(g), rate(g[~g["excluded"]])
            if b != af:
                entry["by_prop"][p] = {"before": b, "after": af}
        for pt, g in s.groupby(s["pick_type"].astype(str)):
            b, af = rate(g), rate(g[~g["excluded"]])
            if b != af:
                entry["by_pick_type"][pt] = {"before": b, "after": af}
        before_after[sport] = entry

    # Tennis gate cells quoted in the rules: confirm they do not read excluded props.
    ten = df[df["sport"].astype(str).str.upper() == "TENNIS"]
    gate_props = {}
    for p, g in ten.groupby("p"):
        gate_props[p] = {
            "rows": int(len(g)),
            "decided": int(g["result"].isin(DECIDED).sum()),
            "excluded_rows": int(g["excluded"].sum()),
            "hr_decided": rate(g)["hr"],
        }
    tennis_gobo = ten[(ten["pick_type"].astype(str).str.title() == "Goblin") & (ten["direction"].astype(str).str.upper() == "OVER")]
    cells = {}
    for p, g in tennis_gobo.groupby("p"):
        cells[p] = {"before": rate(g), "after": rate(g[~g["excluded"]])}

    totals = {
        "graded_rows": int(len(df)),
        "excluded_rows_all_results": int(df["excluded"].sum()),
        "decided_rows_changing_to_void": int(len(changed)),
        "dates": int(df["_date"].nunique()),
        "first_date": str(df["_date"].min()),
        "last_date": str(df["_date"].max()),
    }
    out = {
        "totals": totals,
        "cells": list(CELLS),
        "per_prop": per_prop.to_dict(orient="records"),
        "per_cell_detail": per_cell.to_dict(orient="records"),
        "before_after_by_sport": before_after,
        "tennis_props_overview": gate_props,
        "tennis_goblin_over_by_prop": cells,
        "note": "Read-only. No graded_props_*.json was modified.",
    }
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")

    print(json.dumps(totals, indent=2))
    print("\n## Rows that change HIT/MISS/PUSH -> VOID")
    print(per_prop.to_string(index=False))
    print("\n## Before / after hit rate")
    for sport, e in before_after.items():
        print(f"{sport}: before {e['all']['before']}  after {e['all']['after']}")
        for p, v in e["by_prop"].items():
            print(f"   {p}: before {v['before']} after {v['after']}")
    print("\n## Tennis Goblin OVER by prop (before/after)")
    for p, v in cells.items():
        print(f"   {p}: {v['before']} -> {v['after']}")
    print(f"\nWrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
