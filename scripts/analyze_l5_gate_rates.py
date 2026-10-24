#!/usr/bin/env python3
"""
Hit rates by directional L5 gate + defensive (D) gate, sport, prop category, pick type.

Reads mobile/www/graded_props_*.json (same source as analyze_graded_history.py).
Directional L5 = l5_over when betting OVER, l5_under when betting UNDER.

D gates (same as rank_best_props_today / best-props rules):
  All sports: Over vs Weak|Below Avg; Under vs Elite|Above Avg
  MLB hitter_strikeouts invert (wide): Over Elite|Above; Under Weak|Below
  Avg / unknown / missing def tier does not pass D gate.

Usage:
  py -3.14 scripts/analyze_l5_gate_rates.py
  py -3.14 scripts/analyze_l5_gate_rates.py --days 30 --min-n 25
  py -3.14 scripts/analyze_l5_gate_rates.py --sport WNBA --min-n 10
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))

from analyze_graded_history import (  # noqa: E402
    _norm_dir,
    _norm_pick,
    _norm_prop_type,
    _norm_sport,
    _parse_hit,
)
from rank_best_props_today import _def, _over_d_ok, _under_d_ok  # noqa: E402

_WEAK = {"weak", "bottom", "easy"}
_ELITE = {"elite", "top", "hard", "stingy"}


def _norm_def_tier_raw(raw: str) -> str:
    s = str(raw or "").strip()
    if not s or s.lower() in {"n/a", "na", "none", "—", "-"}:
        return ""
    low = s.lower()
    if low in _WEAK or "easy" in low:
        return "Weak"
    if "below" in low:
        return "Below Avg"
    if low in _ELITE or "hard" in low or "elite" in low:
        return "Elite"
    if "above" in low:
        return "Above Avg"
    return s


def _def_tier_for_row(row: dict[str, Any], sport: str) -> str:
    tier = _def({**row, "sport": sport})
    if tier:
        return tier
    return _norm_def_tier_raw(str(row.get("def_tier") or row.get("DEF_TIER") or ""))

_GRADED_DIR = _REPO / "mobile" / "www"
_OUT_JSON = _REPO / "data" / "reports" / "l5_gate_rates_latest.json"


def _num(v: object) -> float | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if s in ("", "—", "–", "-", "NaN", "nan", "None"):
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _d_gate_pass(sport: str, direction: str, def_tier: str) -> bool:
    tier = str(def_tier or "").strip()
    if not tier or tier in ("—", "-", "Unknown", "UNKNOWN"):
        return False
    if direction == "OVER":
        return _over_d_ok(sport, tier)
    if direction == "UNDER":
        return _under_d_ok(sport, tier)
    return False


def _directional_l5(row: dict[str, Any]) -> int | None:
    direction = _norm_dir(row.get("direction") or row.get("over_under"))
    lo = _num(row.get("l5_over"))
    if lo is None:
        lo = _num(row.get("last5_over"))
    lu = _num(row.get("l5_under"))
    if lu is None:
        lu = _num(row.get("last5_under"))
    if direction == "OVER" and lo is not None:
        return int(round(lo))
    if direction == "UNDER" and lu is not None:
        return int(round(lu))
    return None


def load_graded_with_l5(
    *,
    sport: str | None = None,
    days: int | None = None,
    min_date: str | None = None,
    include_suspect: bool = False,
) -> pd.DataFrame:
    paths = sorted(p for p in _GRADED_DIR.glob("graded_props_*.json") if ".bak_" not in p.name)
    if days and days > 0:
        paths = paths[-days:]
    min_d = str(min_date or "").strip()[:10]
    rows: list[dict[str, Any]] = []
    for path in paths:
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        file_date = str(raw.get("date") or path.stem.replace("graded_props_", ""))[:10]
        if min_d and len(min_d) == 10 and file_date < min_d:
            continue
        chunk = raw.get("props", raw.get("rows", []))
        if not isinstance(chunk, list):
            continue
        for r in chunk:
            if not isinstance(r, dict):
                continue
            sp = _norm_sport(r.get("sport"))
            if sport and sp != _norm_sport(sport):
                continue
            if not include_suspect and r.get("grading_suspect"):
                continue
            hit = _parse_hit(r.get("result"))
            if hit is None:
                continue
            l5 = _directional_l5(r)
            if l5 is None or l5 < 0 or l5 > 5:
                continue
            direction = _norm_dir(r.get("direction") or r.get("over_under"))
            def_tier = _def_tier_for_row(r, sp) or "—"
            rows.append(
                {
                    "sport": sp,
                    "prop_type": _norm_prop_type(r.get("prop")),
                    "prop_raw": str(r.get("prop", "")).strip(),
                    "pick_type": _norm_pick(r.get("pick_type")),
                    "direction": direction,
                    "l5": int(l5),
                    "l5_label": f"{int(l5)}/5",
                    "l5_ge4": int(l5) >= 4,
                    "l5_eq5": int(l5) == 5,
                    "hit": int(hit),
                    "file_date": file_date,
                    "def_tier": def_tier,
                    "d_gate": _d_gate_pass(sp, direction, def_tier),
                }
            )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["l5_ge3"] = df["l5"] >= 3
    df["l5_d_ge4"] = df["l5_ge4"] & df["d_gate"]
    df["l5_d_eq5"] = df["l5_eq5"] & df["d_gate"]
    return df


def _agg(df: pd.DataFrame, group_cols: list[str], min_n: int) -> list[dict[str, Any]]:
    if df.empty:
        return []
    g = (
        df.groupby(group_cols, dropna=False)
        .agg(n=("hit", "size"), hits=("hit", "sum"))
        .reset_index()
    )
    g["hit_rate"] = (g["hits"] / g["n"]).round(4)
    g = g[g["n"] >= min_n].sort_values(group_cols)
    return g.to_dict(orient="records")


def _print_table(title: str, rows: list[dict[str, Any]], cols: list[str]) -> None:
    print(f"\n{title}")
    if not rows:
        print("  (no rows meeting min-n)")
        return
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    hdr = "  ".join(c.ljust(widths[c]) for c in cols)
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        line = []
        for c in cols:
            v = r.get(c, "")
            if c == "hit_rate" and isinstance(v, (int, float)):
                line.append(f"{100*v:5.1f}%".ljust(widths[c]))
            else:
                line.append(str(v).ljust(widths[c]))
        print("  ".join(line))


def main() -> int:
    ap = argparse.ArgumentParser(description="L5 gate hit rates by sport and prop category")
    ap.add_argument("--days", type=int, default=None, help="Only last N graded_props files")
    ap.add_argument("--min-date", default=None, help="Min slate date YYYY-MM-DD")
    ap.add_argument("--sport", default=None, help="Filter one sport (e.g. WNBA)")
    ap.add_argument("--min-n", type=int, default=20, help="Min sample for detail tables")
    ap.add_argument("--detail-min-n", type=int, default=15, help="Min sample for sport×prop×L5")
    args = ap.parse_args()

    df = load_graded_with_l5(
        sport=args.sport,
        days=args.days,
        min_date=args.min_date,
    )
    if df.empty:
        print("No graded props with directional L5 counts.")
        return 1

    date_min = df["file_date"].min()
    date_max = df["file_date"].max()
    overall_hr = df["hit"].mean()

    print("=" * 72)
    print("  L5 + D GATE HIT RATES — all sports & prop categories")
    print(f"  Dates: {date_min} → {date_max}  |  props with L5: {len(df):,}  |  overall HR: {100*overall_hr:.1f}%")
    print("  D rules: all sports O=Weak|Below U=Elite|Above | MLB hitter-K invert wide")
    print("=" * 72)

    # --- Sport × L5 ---
    sport_l5 = _agg(df, ["sport", "l5_label"], min_n=1)
    sport_l5.sort(key=lambda r: (r["sport"], -int(r["l5_label"][0])))
    _print_table("SPORT × L5 GATE", sport_l5, ["sport", "l5_label", "hits", "n", "hit_rate"])

    # --- Sport × pick_type × L5 ---
    sport_pick_l5 = _agg(df, ["sport", "pick_type", "l5_label"], min_n=args.min_n)
    sport_pick_l5.sort(key=lambda r: (r["sport"], r["pick_type"], -int(r["l5_label"][0])))
    _print_table(
        f"SPORT × PICK TYPE × L5 (min n={args.min_n})",
        sport_pick_l5,
        ["sport", "pick_type", "l5_label", "hits", "n", "hit_rate"],
    )

    # --- Sport × prop × L5 (>=4 only — actionable gate) ---
    ge4 = df[df["l5_ge4"]].copy()
    sport_prop_l5 = _agg(ge4, ["sport", "prop_type", "l5_label"], min_n=args.detail_min_n)
    sport_prop_l5.sort(key=lambda r: (-r["hit_rate"], -r["n"]))
    _print_table(
        f"SPORT × PROP × L5 (L5>=4 only, min n={args.detail_min_n}) — top slices",
        sport_prop_l5[:40],
        ["sport", "prop_type", "l5_label", "hits", "n", "hit_rate"],
    )

    # --- Full sport × prop × pick × direction × L5 ---
    full = _agg(
        df,
        ["sport", "prop_type", "pick_type", "direction", "l5_label"],
        min_n=args.detail_min_n,
    )
    full.sort(key=lambda r: (-r["hit_rate"], -r["n"]))

    # --- L5 × D gate ---
    sport_l5_d = _agg(df, ["sport", "l5_label", "d_gate"], min_n=1)
    sport_l5_d.sort(key=lambda r: (r["sport"], -int(r["l5_label"][0]), not r["d_gate"]))
    _print_table(
        "SPORT × L5 × D GATE (d_gate=True = directional D match)",
        sport_l5_d,
        ["sport", "l5_label", "d_gate", "hits", "n", "hit_rate"],
    )

    sport_dir_l5_d = _agg(df, ["sport", "direction", "l5_label", "d_gate"], min_n=args.min_n)
    sport_dir_l5_d.sort(key=lambda r: (r["sport"], r["direction"], -int(r["l5_label"][0]), not r["d_gate"]))
    _print_table(
        f"SPORT × DIR × L5 × D (min n={args.min_n})",
        sport_dir_l5_d,
        ["sport", "direction", "l5_label", "d_gate", "hits", "n", "hit_rate"],
    )

    ge4_d = df[df["l5_d_ge4"]].copy()
    sport_prop_l5_d = _agg(ge4_d, ["sport", "prop_type", "l5_label", "def_tier"], min_n=args.detail_min_n)
    sport_prop_l5_d.sort(key=lambda r: (-r["hit_rate"], -r["n"]))
    _print_table(
        f"SPORT × PROP × L5 × DEF TIER (L5>=4 & D pass, min n={args.detail_min_n})",
        sport_prop_l5_d[:35],
        ["sport", "prop_type", "l5_label", "def_tier", "hits", "n", "hit_rate"],
    )

    sport_pick_l5_d = _agg(df[df["d_gate"]], ["sport", "pick_type", "l5_label"], min_n=args.min_n)
    sport_pick_l5_d.sort(key=lambda r: (r["sport"], r["pick_type"], -int(r["l5_label"][0])))
    _print_table(
        f"SPORT × PICK TYPE × L5 (D gate pass only, min n={args.min_n})",
        sport_pick_l5_d,
        ["sport", "pick_type", "l5_label", "hits", "n", "hit_rate"],
    )

    d_cov = df.groupby("sport").agg(
        n=("hit", "size"),
        with_def=("def_tier", lambda s: int((s != "—").sum())),
        d_pass=("d_gate", "sum"),
    )
    print("\nD GATE COVERAGE")
    for sport, row in d_cov.iterrows():
        print(
            f"  {sport:8} def_tier on {int(row.with_def)}/{int(row.n)} props | "
            f"D-pass {int(row.d_pass)}/{int(row.n)}"
        )
    if int(d_cov.loc[d_cov.index == "TENNIS", "with_def"].sum() if "TENNIS" in d_cov.index else 0) == 0:
        print("  (Tennis graded_props lack def_tier/opponent_rank — D gates N/A until grader backfill)")

    # --- Threshold summary ---
    print("\nL5 + D THRESHOLD SUMMARY (all sports)")
    for sport, g in df.groupby("sport"):
        parts = []
        for label, col in [
            ("5/5", "l5_eq5"),
            ("5/5+D", "l5_d_eq5"),
            (">=4/5", "l5_ge4"),
            (">=4+D", "l5_d_ge4"),
            (">=3/5", "l5_ge3"),
            ("D only", "d_gate"),
        ]:
            s = g[g[col]]
            if len(s):
                parts.append(f"{label} {s['hit'].sum()}/{len(s)}={100*s['hit'].mean():.1f}%")
        print(f"  {sport:8} " + " | ".join(parts))

    def _threshold_block(g: pd.DataFrame) -> dict[str, dict[str, Any]]:
        block: dict[str, dict[str, Any]] = {}
        for label, col in [
            ("5/5", "l5_eq5"),
            ("5/5+D", "l5_d_eq5"),
            (">=4/5", "l5_ge4"),
            (">=4+D", "l5_d_ge4"),
            (">=3/5", "l5_ge3"),
            ("D only", "d_gate"),
        ]:
            s = g[g[col]]
            if len(s):
                block[label] = {
                    "hits": int(s["hit"].sum()),
                    "n": int(len(s)),
                    "hit_rate": round(float(s["hit"].mean()), 4),
                }
        return block

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "date_range": {"min": date_min, "max": date_max},
        "total_props_with_l5": int(len(df)),
        "overall_hit_rate": round(float(overall_hr), 4),
        "d_gate_rules": {
            "all_sports": {"over": "Weak|Below Avg", "under": "Elite|Above Avg"},
            "MLB_hitter_strikeouts": {"over": "Elite|Above Avg", "under": "Weak|Below Avg"},
        },
        "sport_l5": sport_l5,
        "sport_l5_d_gate": sport_l5_d,
        "sport_direction_l5_d": sport_dir_l5_d,
        "sport_pick_type_l5_d_pass": sport_pick_l5_d,
        "sport_pick_type_l5": sport_pick_l5,
        "sport_prop_l5_ge4": sport_prop_l5,
        "sport_prop_l5_d_ge4": sport_prop_l5_d,
        "sport_prop_pick_dir_l5": full,
        "threshold_by_sport": {},
    }
    for sport, g in df.groupby("sport"):
        out["threshold_by_sport"][sport] = _threshold_block(g)

    _OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    _OUT_JSON.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"\nWrote {_OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
