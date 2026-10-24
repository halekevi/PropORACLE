#!/usr/bin/env python3
"""Keep-gate cells split by face verification (verified vs unverified sport-dates).

For each sport's keep props, prints Goblin OVER and Standard OVER/UNDER cells
(ungated, L5=5, L5=5 + L10>=8) on face-verified sport-dates and on
default-label / laddered dates, from the leg ledger (legs + features).

Default sports are the ones whose keep gates were measured in or near the
default-label windows: SOCCER (Mar-Jun), TENNIS (Apr-Jul), NHL (Feb-Jun).

  py -3.14 scripts/keep_gate_face_split.py
  py -3.14 scripts/keep_gate_face_split.py --sport SOCCER --props shots,sog,saves
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))

from proporacle.data.leg_ledger import current_legs, read_table_df  # noqa: E402

OUT = _REPO / "data" / "reports" / "keep_gate_face_split.json"

DEFAULT_PROPS = {
    "SOCCER": ["shots", "sog", "saves"],
    "TENNIS": ["games_won", "match_total_games", "aces", "double_faults"],
    "NHL": ["sog", "points", "assists", "goals", "saves", "blocked_shots", "hits"],
}
MIN_N = 10


def _cell(df: pd.DataFrame) -> tuple[int, int]:
    return int(df["hit"].sum()), int(len(df))


def _fmt(h: int, n: int) -> str:
    return f"{h}/{n} = {h / n:.1%}" if n else "0/0"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sport", default=None, help="SOCCER / TENNIS / NHL (default: all three)")
    ap.add_argument("--props", default=None, help="comma list of prop_canon values")
    args = ap.parse_args(argv)

    legs = current_legs()  # carries face_status / face_verified per sport-date
    feats = read_table_df("features")
    if not feats.empty:
        feats = feats.sort_values("ingested_at").drop_duplicates(subset=["leg_id"], keep="last")
        feats = feats[["leg_id", "l5_over", "l5_under", "l10_over", "l10_under"]]

    sports = [args.sport.upper()] if args.sport else list(DEFAULT_PROPS)
    report: dict[str, dict] = {}
    for sport in sports:
        props = args.props.split(",") if args.props else DEFAULT_PROPS.get(sport, [])
        s = legs[legs["sport"].astype(str).str.upper().eq(sport) & legs["result"].isin(["HIT", "MISS"])].copy()
        if s.empty:
            print(f"\n{sport}: no decided legs")
            continue
        s = s.merge(feats, on="leg_id", how="left")
        s["hit"] = (s["result"] == "HIT").astype(int)
        s["_face"] = s["face_status"].astype(str)
        s["_ver"] = s["face_verified"].astype(bool)
        s["_pick"] = s["pick_norm"].astype(str).str.lower()
        s["_side"] = s["direction"].astype(str).str.upper()
        over = s["_side"].eq("OVER")
        s["_l5"] = pd.to_numeric(s["l5_over"].where(over, s["l5_under"]), errors="coerce")
        s["_l10"] = pd.to_numeric(s["l10_over"].where(over, s["l10_under"]), errors="coerce")
        # one leg per player-prop-line-side-pick-date
        s = s.drop_duplicates(subset=["slate_date", "player_fold", "prop_canon", "line", "_side", "_pick"])
        avail = sorted(s["prop_canon"].astype(str).unique())
        print(f"\n===== {sport}  decided legs={len(s):,}  verified={int(s['_ver'].sum()):,}  "
              f"unverified={int((~s['_ver']).sum()):,}")
        print(f"  dates verified={s[s['_ver']]['slate_date'].nunique()} unverified={s[~s['_ver']]['slate_date'].nunique()}")
        missing = [p for p in props if p not in avail]
        if missing:
            print(f"  props not in ledger canon: {missing}  (available sample: {avail[:25]})")
        report[sport] = {}
        for prop in props:
            p = s[s["prop_canon"].astype(str) == prop]
            if p.empty:
                continue
            print(f"\n  -- {prop}")
            print(f"  {'book':<16} {'gate':<14} {'verified':>22} {'unverified':>22}")
            rows = []
            for pick, side in (("goblin", "OVER"), ("standard", "OVER"), ("standard", "UNDER"), ("demon", "OVER")):
                b = p[p["_pick"].eq(pick) & p["_side"].eq(side)]
                if len(b) < MIN_N:
                    continue
                for gate, mask in (
                    ("ungated", pd.Series(True, index=b.index)),
                    ("L5=5", b["_l5"] >= 5),
                    ("L5=5+L10>=8", (b["_l5"] >= 5) & (b["_l10"] >= 8)),
                    ("L5>=4", b["_l5"] >= 4),
                ):
                    v = b[mask & b["_ver"]]
                    u = b[mask & ~b["_ver"]]
                    hv, nv = _cell(v)
                    hu, nu = _cell(u)
                    if nv + nu < MIN_N:
                        continue
                    print(f"  {pick + ' ' + side:<16} {gate:<14} {_fmt(hv, nv):>22} {_fmt(hu, nu):>22}")
                    rows.append({"pick": pick, "side": side, "gate": gate,
                                 "verified": [hv, nv], "unverified": [hu, nu]})
            report[sport][prop] = rows
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
