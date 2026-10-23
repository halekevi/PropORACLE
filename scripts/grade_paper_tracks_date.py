#!/usr/bin/env python3
"""Grade logged paper-track legs for a date against on-disk actuals / graded workbooks."""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _nk(s: object) -> str:
    return re.sub(r"[^a-z0-9 ]", "", str(s or "").lower()).strip()


def _load_legs(jsonl: Path, date_str: str) -> list[dict]:
    out: list[dict] = []
    if not jsonl.is_file():
        return out
    for line in jsonl.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        o = json.loads(line)
        if o.get("date") != date_str:
            continue
        if o.get("track") not in ("nba1q_goblin_reb_ast", "nhl_std_under_pts_ast"):
            continue
        out.append(o)
    return out


def _grade_side(direction: str, line: float, actual: float) -> str:
    d = str(direction or "").upper()
    if d == "OVER":
        return "HIT" if actual > line else "MISS"
    return "HIT" if actual < line else "MISS"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--root", default=str(ROOT))
    args = ap.parse_args()
    root = Path(args.root)
    date_str = str(args.date).strip()[:10]
    jsonl = root / "data" / "reports" / "track_comparison.jsonl"
    legs = _load_legs(jsonl, date_str)
    print(f"paper legs {date_str}: {len(legs)}")

    nhl_path = root / "outputs" / date_str / f"graded_nhl_{date_str}.xlsx"
    nhl = pd.DataFrame()
    if nhl_path.exists():
        nhl = pd.read_excel(nhl_path)
        nhl.columns = [str(c).strip() for c in nhl.columns]

    nba1q_path = root / "outputs" / date_str / f"actuals_nba1q_{date_str}.csv"
    nba1q = pd.read_csv(nba1q_path) if nba1q_path.exists() else pd.DataFrame()

    results: list[dict] = []
    for leg in legs:
        track = leg["track"]
        player = leg["player"]
        prop = str(leg.get("prop_canon") or leg.get("prop") or "").lower()
        line = float(leg["line"])
        direction = str(leg["direction"]).upper()
        actual = None
        source = ""
        result = "VOID"

        if track.startswith("nhl") and len(nhl):
            m = nhl[
                (nhl["player"].map(_nk) == _nk(player))
                & (nhl["prop_type"].astype(str).str.lower().str.contains(prop[:5], na=False))
            ].copy()
            if len(m):
                m["_ld"] = (pd.to_numeric(m["line"], errors="coerce") - line).abs()
                row = m.sort_values("_ld").iloc[0]
                if pd.notna(row.get("actual")):
                    actual = float(row["actual"])
                    source = "graded_nhl"
                    result = str(row.get("result") or "").upper() or _grade_side(
                        direction, line, actual
                    )
        elif track.startswith("nba1q") and len(nba1q):
            if {"player", "prop_type", "actual"}.issubset(set(nba1q.columns)):
                sub = nba1q[
                    (nba1q["player"].map(_nk) == _nk(player))
                    & (
                        nba1q["prop_type"]
                        .astype(str)
                        .str.lower()
                        .str.contains(prop[:3], na=False)
                    )
                ]
                if len(sub) and pd.notna(sub.iloc[0]["actual"]):
                    actual = float(sub.iloc[0]["actual"])
                    source = "actuals_nba1q"
                    result = _grade_side(direction, line, actual)
            else:
                pc = next(
                    (
                        c
                        for c in nba1q.columns
                        if c.lower() in ("player", "athlete", "name")
                    ),
                    None,
                )
                col = None
                for c in nba1q.columns:
                    cl = c.lower()
                    if prop.startswith("reb") and "reb" in cl and "ast" not in cl:
                        col = c
                        break
                    if prop.startswith("ast") and ("ast" in cl or "assist" in cl):
                        col = c
                        break
                if pc and col is not None:
                    sub = nba1q[nba1q[pc].map(_nk) == _nk(player)]
                    if len(sub) and pd.notna(sub.iloc[0][col]):
                        actual = float(sub.iloc[0][col])
                        source = "actuals_nba1q_wide"
                        result = _grade_side(direction, line, actual)

        if actual is None and result == "VOID":
            source = source or "NO_ACTUAL"

        graded = {
            **leg,
            "actual": actual,
            "result": result,
            "grade_source": source,
            "graded_at": datetime.now(timezone.utc).isoformat(),
        }
        results.append(graded)
        print(
            f"  {track[:22]:22} {player:28} {prop:10} "
            f"{direction} {line} actual={actual} -> {result} ({source})"
        )

    print("summary", Counter((r["track"], r["result"]) for r in results))
    out = root / "data" / "reports" / f"paper_l5eq5_graded_{date_str}.json"
    out.write_text(
        json.dumps({"date": date_str, "legs": results}, indent=2), encoding="utf-8"
    )
    print(f"wrote {out}")

    reg_path = root / "data" / "reports" / "paper_tracks_registry.json"
    if reg_path.exists():
        reg = json.loads(reg_path.read_text(encoding="utf-8"))
        for track in ("nba1q_goblin_reb_ast", "nhl_std_under_pts_ast"):
            if track not in reg.get("tracks", {}):
                continue
            tr = reg["tracks"][track]
            decided = [
                r for r in results if r["track"] == track and r["result"] in ("HIT", "MISS")
            ]
            hits = sum(1 for r in decided if r["result"] == "HIT")
            tr["forward_hits"] = hits
            tr["forward_decided"] = len(decided)
            tr["forward_n"] = sum(1 for r in results if r["track"] == track)
            day = tr.setdefault("dates", {}).setdefault(date_str, {})
            day["hits"] = hits
            day["decided"] = len(decided)
            day["results"] = {
                f"{r['player']}:{r['prop']}": r["result"]
                for r in results
                if r["track"] == track
            }
            tr["updated_at"] = datetime.now(timezone.utc).isoformat()
        reg["updated_at"] = datetime.now(timezone.utc).isoformat()
        reg_path.write_text(json.dumps(reg, indent=2), encoding="utf-8")
        print(f"updated {reg_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
