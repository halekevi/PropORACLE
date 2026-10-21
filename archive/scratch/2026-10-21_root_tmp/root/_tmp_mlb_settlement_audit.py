#!/usr/bin/env python3
"""MLB settlement-path audit: zero-UNDER / undersized-actual / no-row VOID behavior.

Mirrors the checks that caught NHL SOG (zero→HIT UNDER) and Soccer Passes
(undersized actual). Reads graded_props + a sample actuals_mlb CSV; optionally
spot-checks Stats API gameLog for one date.
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent
MAIN = REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(REPO), str(REPO / "scripts")]

from discover_gates_nfl import graded_by_date  # same templates glob


def _props(data) -> list[dict]:
    props = data.get("props") or data.get("graded_props") or data
    if isinstance(props, dict):
        props = props.get("props") or []
    return [r for r in props if isinstance(r, dict)]


def is_mlb(r: dict) -> bool:
    return str(r.get("sport") or r.get("league") or "").strip().upper() == "MLB"


def main() -> None:
    by = graded_by_date()
    # Sample recent MLB-heavy dates + a mid-season date
    sample_dates = [
        "2026-09-16",
        "2026-09-10",
        "2026-08-15",
        "2026-07-04",
        "2026-05-15",
    ]

    zero_under_hit = Counter()  # prop -> count
    zero_under_hit_n = Counter()
    under_n = Counter()
    undersized = []  # actual > 0 but actual/line < 0.35 on OVER volume props
    result_mix = Counter()
    void_reasons = Counter()
    hit_miss = Counter()
    actual_missing_hit = 0
    actual_zero_under_hit_examples = []

    volume_props = {
        "pitchesthrown",
        "pitchesthrown",
        "pitchingouts",
        "hitsallowed",
        "strikeouts",
        "pitcherstrikeouts",
        "hits",
        "totalbases",
        "hitsrunsrbi",
        "plateappearances",
        "walks",
        "walksallowed",
    }

    dates_seen = []
    for d in sample_dates:
        p = by.get(d)
        if not p:
            print(f"{d}: no graded file")
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        rows = [r for r in _props(data) if is_mlb(r)]
        if not rows:
            print(f"{d}: 0 MLB rows")
            continue
        dates_seen.append(d)
        print(f"{d}: MLB rows={len(rows):,}")
        for r in rows:
            res = str(r.get("result") or r.get("grade") or "").strip().upper()
            result_mix[res] += 1
            if res in ("HIT", "MISS"):
                hit_miss[res] += 1
            if res == "VOID":
                void_reasons[str(r.get("void_reason_grade") or r.get("void_reason") or "?")] += 1

            prop = str(r.get("prop_type") or r.get("prop") or r.get("prop_type_norm") or "").lower()
            prop_n = "".join(c for c in prop if c.isalnum())
            side = str(r.get("bet_direction") or r.get("direction") or r.get("side") or "").upper()
            side2 = str(r.get("final_bet_direction") or "").upper()
            if "UNDER" in side2 and "UNDER" not in side:
                side = "UNDER"
            elif "OVER" in side2 and "OVER" not in side:
                side = "OVER"

            try:
                act = r.get("actual_value")
                if act is None:
                    act = r.get("actual")
                act_f = float(act) if act is not None and str(act).strip() not in ("", "nan", "None") else None
            except (TypeError, ValueError):
                act_f = None
            try:
                line = float(r.get("line") or r.get("Line") or 0)
            except (TypeError, ValueError):
                line = 0.0

            # Undersized (soccer-style): volume props with actual << line
            if (
                act_f is not None
                and act_f > 0
                and line >= 5
                and prop_n in volume_props
                and res in ("HIT", "MISS")
                and act_f / line < 0.35
            ):
                undersized.append(
                    {
                        "date": d,
                        "player": r.get("player") or r.get("player_name"),
                        "prop": prop,
                        "line": line,
                        "actual": act_f,
                        "ratio": round(act_f / line, 3),
                        "result": res,
                        "side": side,
                    }
                )

            if "UNDER" not in side and side != "U":
                continue
            under_n[prop_n] += 1

            if res == "HIT" and act_f is None:
                actual_missing_hit += 1
            if res == "HIT" and act_f == 0.0:
                zero_under_hit[prop_n] += 1
                if len(actual_zero_under_hit_examples) < 12:
                    actual_zero_under_hit_examples.append(
                        {
                            "date": d,
                            "player": r.get("player") or r.get("player_name"),
                            "prop": prop,
                            "line": line,
                            "pick": r.get("pick_type"),
                            "result": res,
                        }
                    )

    print("\n=== Result mix (sample dates) ===")
    for k, v in result_mix.most_common():
        print(f"  {k}: {v}")

    print("\n=== VOID reasons (top) ===")
    for k, v in void_reasons.most_common(10):
        print(f"  {k}: {v}")

    print(f"\nHIT with missing actual: {actual_missing_hit}")
    print("\n=== UNDER HIT with actual=0 (by prop) ===")
    total_zu = sum(zero_under_hit.values())
    print(f"  total={total_zu}")
    for prop, n in zero_under_hit.most_common(20):
        u = under_n[prop]
        print(f"  {prop}: {n} zero-HIT UNDER  (UNDER rows for prop in sample={u})")

    print("\n=== Example zero-UNDER HITs ===")
    for ex in actual_zero_under_hit_examples:
        print(f"  {ex}")

    print(f"\n=== Undersized actual/line <0.35 on volume OVER (n={len(undersized)}) ===")
    for ex in undersized[:15]:
        print(f"  {ex}")

    # Spot-check actuals CSV for a date vs graded: missing actuals should be VOID not HIT
    for d in dates_seen[:2]:
        for root in (MAIN, REPO):
            ap = root / "outputs" / d / f"actuals_mlb_{d}.csv"
            if not ap.exists():
                ap = root / "outputs" / d / "mlb" / f"actuals_mlb_{d}.csv"
            if ap.exists():
                adf = pd.read_csv(ap, low_memory=False)
                print(f"\nactuals file {ap}: rows={len(adf)} zero_actual={int((adf['actual']==0).sum())} "
                      f"nan={int(adf['actual'].isna().sum())}")
                print(f"  props sample: {adf['prop'].value_counts().head(8).to_dict()}")
                break

    # Live Stats API spot-check: one pitcher pitchesThrown vs CSV
    print("\n=== Spot-check: Stats API gameLog vs on-disk actuals (if available) ===")
    d = dates_seen[0] if dates_seen else None
    if d:
        ap = None
        for root in (MAIN, REPO):
            for cand in (
                root / "outputs" / d / f"actuals_mlb_{d}.csv",
                root / "outputs" / d / "mlb" / f"actuals_mlb_{d}.csv",
            ):
                if cand.exists():
                    ap = cand
                    break
            if ap:
                break
        if ap is None:
            print("  no actuals CSV found for spot-check")
            return
        adf = pd.read_csv(ap, low_memory=False)
        # pick a few non-zero pitcherstrikeouts / hits rows and re-fetch
        import importlib

        sys.path.insert(0, str(REPO / "Sports" / "MLB" / "scripts"))
        s4 = importlib.import_module("step4_attach_player_stats_mlb")
        season = d[:4]
        checks = 0
        mismatches = []
        for _, row in adf.iterrows():
            prop = str(row.get("prop") or "")
            pid = str(row.get("mlb_player_id") or "").strip()
            if "|" in pid or not pid or not pid.isdigit():
                continue
            if prop not in ("hits", "strikeouts", "pitcherstrikeouts", "totalbases", "hitsrunsrbi", "pitchesthrown"):
                continue
            act = float(row["actual"])
            group = "pitching" if prop in ("strikeouts", "pitcherstrikeouts", "pitchesthrown", "hitsallowed") else "hitting"
            # map grader prop key back to derive prop_norm
            prop_norm_map = {
                "hits": "hits",
                "totalbases": "total_bases",
                "hitsrunsrbi": "hits_runs_rbi",
                "strikeouts": "strikeouts",
                "pitcherstrikeouts": "strikeouts",
                "pitchesthrown": "pitches_thrown",
            }
            pn = prop_norm_map.get(prop)
            if not pn:
                continue
            splits = s4.fetch_game_log(pid, group, season)
            match = None
            for sp in splits:
                gd = str(sp.get("date") or "")[:10]
                if gd == d or gd == str(pd.Timestamp(d).date()):
                    match = sp
                    break
            if match is None:
                continue
            derive = s4.derive_pitcher_stat if group == "pitching" else s4.derive_hitter_stat
            live = float(derive(match, pn))
            checks += 1
            if abs(live - act) > 1e-6:
                mismatches.append(
                    {
                        "player": row.get("player"),
                        "prop": prop,
                        "csv": act,
                        "live": live,
                        "pid": pid,
                    }
                )
            if checks >= 25:
                break
        print(f"  re-fetched {checks} gameLog rows; mismatches={len(mismatches)}")
        for m in mismatches[:10]:
            print(f"    MISMATCH {m}")


if __name__ == "__main__":
    main()
