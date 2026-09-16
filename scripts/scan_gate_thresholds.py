#!/usr/bin/env python3
"""Scan feature thresholds for keep-gate lift with chronological OOS validation.

Finds candidate cutoffs on the first half of slate dates (by Wilson lower bound),
then reports performance only on the held-out second half. Prevents the
small-n tail / multiple-comparisons trap of picking the in-sample max.

Examples:
  py -3.14 scripts/scan_gate_thresholds.py \\
    --sport WNBA --pick-type Goblin --direction OVER \\
    --prop Points --prop "Pts+Asts" --prop "3-PT Made" \\
    --feature role_tier --feature def_tier --feature minutes_tier \\
    --feature l5_over --feature l10_over --feature edge \\
    --base-l5 5 --base-l10 8

  # Fade re-open (no base stack):
  py -3.14 scripts/scan_gate_thresholds.py --sport MLB --pick-type Goblin \\
    --direction OVER --prop Runs --feature l5_over --feature l10_over
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

_WILSON_Z = 1.96
_DEFAULT_GRADED = _REPO / "ui_runner" / "templates"

# Ordered encodings for categorical "threshold" scans (higher = more of the label).
_CAT_ORDER: dict[str, dict[str, float]] = {
    "def_tier": {
        "Elite": 0.0,
        "Above Avg": 1.0,
        "Avg": 2.0,
        "Below Avg": 3.0,
        "Weak": 4.0,
    },
    "role_tier": {
        "SUPPORT": 0.0,
        "SECONDARY": 1.0,
        "PRIMARY": 2.0,
    },
    "minutes_tier": {
        "LOW": 0.0,
        "MED": 1.0,
        "MEDIUM": 1.0,
        "HIGH": 2.0,
    },
}


def _wilson_lower(hits: int, n: int, z: float = _WILSON_Z) -> float:
    if n <= 0:
        return 0.0
    phat = hits / n
    z2 = z * z
    denom = 1.0 + z2 / n
    centre = phat + z2 / (2.0 * n)
    margin = z * math.sqrt((phat * (1.0 - phat) + z2 / (4.0 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def _num(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x):
        return None
    return x


def _load_graded_json(
    graded_dir: Path,
    *,
    sport: str,
    pick_type: str,
    direction: str,
    props: list[str] | None,
) -> pd.DataFrame:
    sport_u = sport.strip().upper()
    pick_l = pick_type.strip().lower()
    dir_u = direction.strip().upper()
    prop_want: set[str] | None = None
    if props:
        prop_want = {canon_prop(sport_u, p).lower() for p in props} | {
            p.strip().lower() for p in props
        }

    rows: list[dict[str, Any]] = []
    files = sorted(graded_dir.glob("graded_props_*.json"))
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        day = str(payload.get("date") or path.stem.replace("graded_props_", ""))[:10]
        for p in payload.get("props") or []:
            if str(p.get("sport") or "").strip().upper() != sport_u:
                continue
            if pick_l not in str(p.get("pick_type") or "").strip().lower():
                continue
            if str(p.get("direction") or "").strip().upper() != dir_u:
                continue
            result = str(p.get("result") or "").strip().upper()
            if result not in {"HIT", "MISS"}:
                continue
            prop_raw = str(p.get("prop") or p.get("prop_type") or "")
            prop_c = canon_prop(sport_u, prop_raw)
            if prop_want is not None:
                if prop_c.lower() not in prop_want and prop_raw.strip().lower() not in prop_want:
                    continue
            rows.append(
                {
                    "date": day,
                    "sport": sport_u,
                    "player": p.get("player"),
                    "prop": prop_c or prop_raw,
                    "prop_raw": prop_raw,
                    "pick_type": str(p.get("pick_type") or "").strip(),
                    "direction": dir_u,
                    "is_hit": 1 if result == "HIT" else 0,
                    "line": _num(p.get("line")),
                    "l5_over": _num(p.get("l5_over") if dir_u == "OVER" else p.get("l5_under")),
                    "l10_over": _num(p.get("l10_over") if dir_u == "OVER" else p.get("l10_under")),
                    "l5": _num(p.get("l5_over") if dir_u == "OVER" else p.get("l5_under")),
                    "l10": _num(p.get("l10_over") if dir_u == "OVER" else p.get("l10_under")),
                    "edge": _num(p.get("edge")),
                    "ml_prob": _num(p.get("ml_prob")),
                    "def_tier": normalize_def_tier_label(p.get("def_tier")) or "",
                    "role_tier": str(p.get("role_tier") or "").strip().upper(),
                    "minutes_tier": str(p.get("minutes_tier") or "").strip().upper(),
                    "usage_tier": str(p.get("usage_tier") or "").strip().upper(),
                    "consistency_grade": str(p.get("consistency_grade") or "").strip(),
                }
            )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df[df["date"].notna()].copy()
    return df.sort_values(["date", "prop", "player"]).reset_index(drop=True)


def _apply_base_stack(
    df: pd.DataFrame,
    *,
    base_l5: float | None,
    base_l10: float | None,
    base_d: bool,
    direction: str,
) -> pd.DataFrame:
    out = df
    if base_l5 is not None:
        out = out[out["l5"].notna() & (out["l5"] >= float(base_l5))]
    if base_l10 is not None:
        out = out[out["l10"].notna() & (out["l10"] >= float(base_l10))]
    if base_d:
        weak = {"Weak", "Below Avg"}
        elite = {"Elite", "Above Avg"}
        if direction.upper() == "OVER":
            out = out[out["def_tier"].isin(weak)]
        else:
            out = out[out["def_tier"].isin(elite)]
    return out.copy()


def _feature_series(df: pd.DataFrame, feature: str) -> pd.Series:
    if feature in _CAT_ORDER:
        mapping = _CAT_ORDER[feature]
        return df[feature].map(mapping)
    if feature in df.columns:
        return pd.to_numeric(df[feature], errors="coerce")
    raise KeyError(f"Unknown feature: {feature}")


def _candidate_thresholds(values: pd.Series, max_cuts: int = 40) -> list[float]:
    clean = values.dropna().astype(float)
    if clean.empty:
        return []
    uniq = np.unique(clean.to_numpy())
    if len(uniq) <= max_cuts:
        return [float(x) for x in uniq]
    qs = np.linspace(0.05, 0.95, max_cuts)
    return sorted({float(x) for x in np.quantile(uniq, qs)})


@dataclass
class CutResult:
    feature: str
    op: str
    threshold: float
    label: str
    find_n: int
    find_hits: int
    find_hr: float
    find_wilson: float
    find_lift_pp: float
    oos_n: int
    oos_hits: int
    oos_hr: float
    oos_lift_pp: float
    oos_wilson: float
    base_find_hr: float
    base_oos_hr: float


def _mask_for_cut(series: pd.Series, op: str, thr: float) -> pd.Series:
    if op == ">=":
        return series >= thr
    if op == "<=":
        return series <= thr
    if op == ">":
        return series > thr
    if op == "<":
        return series < thr
    raise ValueError(op)


def _hr(hits: int, n: int) -> float:
    return (hits / n) if n else float("nan")


def _scan_feature(
    find: pd.DataFrame,
    hold: pd.DataFrame,
    *,
    feature: str,
    min_n: int,
    ops: Iterable[str],
) -> list[CutResult]:
    try:
        s_find = _feature_series(find, feature)
        s_hold = _feature_series(hold, feature)
    except KeyError:
        return []

    base_find_hits = int(find["is_hit"].sum())
    base_find_n = len(find)
    base_find_hr = _hr(base_find_hits, base_find_n)
    base_oos_hits = int(hold["is_hit"].sum())
    base_oos_n = len(hold)
    base_oos_hr = _hr(base_oos_hits, base_oos_n)

    thr_list = _candidate_thresholds(s_find)
    out: list[CutResult] = []
    for op in ops:
        best: CutResult | None = None
        for thr in thr_list:
            m_find = _mask_for_cut(s_find, op, thr) & s_find.notna()
            n_f = int(m_find.sum())
            if n_f < min_n:
                continue
            hits_f = int(find.loc[m_find, "is_hit"].sum())
            hr_f = _hr(hits_f, n_f)
            w_f = _wilson_lower(hits_f, n_f)
            lift_f = (hr_f - base_find_hr) * 100.0

            m_hold = _mask_for_cut(s_hold, op, thr) & s_hold.notna()
            n_o = int(m_hold.sum())
            hits_o = int(hold.loc[m_hold, "is_hit"].sum()) if n_o else 0
            hr_o = _hr(hits_o, n_o) if n_o else float("nan")
            w_o = _wilson_lower(hits_o, n_o) if n_o else 0.0
            lift_o = (hr_o - base_oos_hr) * 100.0 if n_o else float("nan")

            label = f"{feature} {op} {thr:g}"
            if feature in _CAT_ORDER:
                # Human label for categorical encodings.
                rev = {v: k for k, v in _CAT_ORDER[feature].items()}
                # MED/MEDIUM collide at 1.0 — prefer MEDIUM in display.
                name = rev.get(thr, f"{thr:g}")
                if op == ">=":
                    label = f"{feature} >= {name} ({thr:g})"
                elif op == "<=":
                    label = f"{feature} <= {name} ({thr:g})"

            cand = CutResult(
                feature=feature,
                op=op,
                threshold=float(thr),
                label=label,
                find_n=n_f,
                find_hits=hits_f,
                find_hr=hr_f,
                find_wilson=w_f,
                find_lift_pp=lift_f,
                oos_n=n_o,
                oos_hits=hits_o,
                oos_hr=hr_o,
                oos_lift_pp=lift_o,
                oos_wilson=w_o,
                base_find_hr=base_find_hr,
                base_oos_hr=base_oos_hr,
            )
            # Skip tautologies / near-full-pool cuts (no real filter).
            if n_f >= int(0.98 * base_find_n) and abs(lift_f) < 0.25:
                continue
            # Select on find-half Wilson only (never look at OOS for selection).
            if best is None or cand.find_wilson > best.find_wilson:
                best = cand
            elif best is not None and cand.find_wilson == best.find_wilson:
                if cand.find_n > best.find_n:
                    best = cand
        if best is not None:
            out.append(best)
    return out


def _chronological_split(df: pd.DataFrame, find_frac: float) -> tuple[pd.DataFrame, pd.DataFrame, list]:
    dates = sorted(df["date"].dropna().unique())
    if len(dates) < 4:
        raise SystemExit(f"Need >=4 distinct slate dates for OOS split; got {len(dates)}")
    cut = max(1, min(len(dates) - 1, int(math.floor(len(dates) * find_frac))))
    find_dates = set(dates[:cut])
    hold_dates = set(dates[cut:])
    find = df[df["date"].isin(find_dates)].copy()
    hold = df[df["date"].isin(hold_dates)].copy()
    return find, hold, [str(pd.Timestamp(d).date()) for d in dates]


def _print_pool_header(df: pd.DataFrame, find: pd.DataFrame, hold: pd.DataFrame, dates: list[str]) -> None:
    n = len(df)
    hits = int(df["is_hit"].sum())
    print(
        f"pool n={n} hits={hits} hr={_hr(hits, n):.1%} | dates={len(dates)} "
        f"({dates[0]} -> {dates[-1]})"
    )
    print(
        f"find n={len(find)} hr={_hr(int(find['is_hit'].sum()), len(find)):.1%} | "
        f"oos  n={len(hold)} hr={_hr(int(hold['is_hit'].sum()), len(hold)):.1%}"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--graded-dir", type=Path, default=_DEFAULT_GRADED)
    ap.add_argument("--sport", required=True)
    ap.add_argument("--pick-type", default="Goblin")
    ap.add_argument("--direction", default="OVER")
    ap.add_argument("--prop", action="append", default=[], help="Repeatable. Default: all props.")
    ap.add_argument(
        "--feature",
        action="append",
        default=[],
        help="Repeatable. Numeric cols or def_tier/role_tier/minutes_tier.",
    )
    ap.add_argument("--base-l5", type=float, default=None, help="Require directional L5 >= this first.")
    ap.add_argument("--base-l10", type=float, default=None, help="Require directional L10 >= this first.")
    ap.add_argument("--base-d", action="store_true", help="Also require directional D before scanning.")
    ap.add_argument("--min-n", type=int, default=40, help="Min find-half n for a candidate cut.")
    ap.add_argument("--min-oos-n", type=int, default=20, help="Min OOS n to treat a cut as reportable.")
    ap.add_argument("--find-frac", type=float, default=0.5)
    ap.add_argument(
        "--ops",
        default=">=,<=",
        help="Comma ops for numeric/categorical ordinal scans (default: >=,<=).",
    )
    ap.add_argument("--out-csv", type=Path, default=None)
    args = ap.parse_args(argv)

    features = args.feature or [
        "l5",
        "l10",
        "edge",
        "ml_prob",
        "def_tier",
        "role_tier",
        "minutes_tier",
    ]
    # Don't re-scan floors already enforced by the base stack.
    if args.base_l5 is not None:
        features = [f for f in features if f not in {"l5", "l5_over", "l5_under"}]
    if args.base_l10 is not None:
        features = [f for f in features if f not in {"l10", "l10_over", "l10_under"}]
    if args.base_d:
        features = [f for f in features if f != "def_tier"]
    ops = [o.strip() for o in str(args.ops).split(",") if o.strip()]

    raw = _load_graded_json(
        args.graded_dir,
        sport=args.sport,
        pick_type=args.pick_type,
        direction=args.direction,
        props=args.prop or None,
    )
    if raw.empty:
        raise SystemExit("No graded rows matched filters.")

    props = sorted(raw["prop"].astype(str).unique())
    print(
        f"scan sport={args.sport} pick={args.pick_type} dir={args.direction} "
        f"props={props} features={features}"
    )
    if args.base_l5 is not None or args.base_l10 is not None or args.base_d:
        print(
            f"base stack: L5>={args.base_l5} L10>={args.base_l10} D={bool(args.base_d)}"
        )

    all_rows: list[dict[str, Any]] = []
    for prop in props:
        pool = raw[raw["prop"].astype(str) == prop].copy()
        pool = _apply_base_stack(
            pool,
            base_l5=args.base_l5,
            base_l10=args.base_l10,
            base_d=args.base_d,
            direction=args.direction,
        )
        print("\n" + "=" * 72)
        print(f"PROP {prop}")
        if len(pool) < args.min_n:
            print(f"  skip: pool n={len(pool)} < min_n={args.min_n}")
            continue
        try:
            find, hold, dates = _chronological_split(pool, args.find_frac)
        except SystemExit as e:
            print(f"  skip: {e}")
            continue
        _print_pool_header(pool, find, hold, dates)

        results: list[CutResult] = []
        for feat in features:
            results.extend(
                _scan_feature(find, hold, feature=feat, min_n=args.min_n, ops=ops)
            )
        # Rank by OOS wilson among cuts that clear min_oos_n (selection was find-only).
        reportable = [r for r in results if r.oos_n >= args.min_oos_n]
        reportable.sort(key=lambda r: (-r.oos_wilson, -r.oos_n))

        if not reportable:
            print("  no OOS-reportable cuts (try lowering --min-oos-n or widening pool)")
            continue

        print(
            f"  {'cut':<42} {'find':>14} {'oos':>14} {'oos_lift':>9} {'oos_w':>7}"
        )
        for r in reportable[:12]:
            find_s = f"{r.find_hits}/{r.find_n}={r.find_hr:.1%}"
            oos_s = f"{r.oos_hits}/{r.oos_n}={r.oos_hr:.1%}"
            print(
                f"  {r.label:<42} {find_s:>14} {oos_s:>14} "
                f"{r.oos_lift_pp:>+7.1f}pp {r.oos_wilson:>6.3f}"
            )
            all_rows.append(
                {
                    "sport": args.sport,
                    "pick_type": args.pick_type,
                    "direction": args.direction,
                    "prop": prop,
                    "base_l5": args.base_l5,
                    "base_l10": args.base_l10,
                    "base_d": bool(args.base_d),
                    "cut": r.label,
                    "feature": r.feature,
                    "op": r.op,
                    "threshold": r.threshold,
                    "find_n": r.find_n,
                    "find_hits": r.find_hits,
                    "find_hr": round(r.find_hr, 4),
                    "find_wilson": round(r.find_wilson, 4),
                    "find_lift_pp": round(r.find_lift_pp, 2),
                    "oos_n": r.oos_n,
                    "oos_hits": r.oos_hits,
                    "oos_hr": round(r.oos_hr, 4) if r.oos_n else None,
                    "oos_wilson": round(r.oos_wilson, 4),
                    "oos_lift_pp": round(r.oos_lift_pp, 2) if r.oos_n else None,
                    "base_find_hr": round(r.base_find_hr, 4),
                    "base_oos_hr": round(r.base_oos_hr, 4),
                }
            )

        best = reportable[0]
        print(
            f"  >> OOS-best: {best.label} | "
            f"oos {best.oos_hits}/{best.oos_n}={best.oos_hr:.1%} "
            f"({best.oos_lift_pp:+.1f}pp vs base {best.base_oos_hr:.1%})"
        )

    if args.out_csv and all_rows:
        args.out_csv.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(all_rows).to_csv(args.out_csv, index=False)
        print(f"\nWrote {args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
