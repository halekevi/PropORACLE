#!/usr/bin/env python3
"""L5=5 board ranked by |L5 avg − line|, then |L10|, then |season|.

Tries PropORACLE_main_cp first, then this repo, so tennis/CFB still land when
the live tree is missing those step8 files.

  py -3.14 scripts/export_l5eq5_cover.py --date 2026-08-28
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

import rank_best_props_today as R  # noqa: E402
from proporacle.data.table_io import write_excel_sheets  # noqa: E402
from rank_l5eq5_secondary import l5eq5_buckets  # noqa: E402

ET = ZoneInfo("America/New_York")
GOLD = {"Diamond", "Platinum", "Gold"}
TENNIS_NOTES = {
    "Darja Semenistaja": "PP last-5 fails (2/5 at 11.5, avg 10.0)",
    "Elvina Kalieva": "PP last-5 fails (3/5 at 11.5, avg 10.8)",
    "Colton Smith": "Goblin 11.5 pulled; live Standard 12.5",
    "Tristan Schoolkate": "PP last-5 is 3/5 at 12.5 (avg 12.80)",
}


def _roots(preferred: Path | None) -> list[Path]:
    out: list[Path] = []
    if preferred is not None and preferred.is_dir():
        out.append(preferred)
    main_cp = _REPO.parent / "PropORACLE_main_cp"
    for p in (main_cp, _REPO):
        if p.is_dir() and p not in out:
            out.append(p)
    return out


def _first_nonempty(load_fn, roots: list[Path]) -> tuple[Path | None, pd.DataFrame]:
    for root in roots:
        df = load_fn(root)
        if df is not None and not df.empty:
            return root, df
    return None, pd.DataFrame()


def _xlsx_row(r: dict, category: str) -> dict:
    row = R._row_for_xlsx(r, category)
    player = str(row.get("Player") or "")
    extra = TENNIS_NOTES.get(player)
    if extra and not str(row.get("Note") or "").strip():
        row["Note"] = extra
    d5 = row.get("Dist_L5")
    row["Abs_Dist_L5"] = round(abs(float(d5)), 2) if isinstance(d5, (int, float)) else None
    return row


def _is_gold(r: dict) -> bool:
    return (r.get("Promo") or r.get("Badge") or "") in GOLD


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(ET).strftime("%Y-%m-%d"))
    ap.add_argument("--step8-root", default="")
    ap.add_argument(
        "--xlsx",
        default="",
        help="Output workbook (default: outputs/<date>/l5eq5_ranked_by_cover.xlsx)",
    )
    args = ap.parse_args()
    date = str(args.date).strip()[:10]
    preferred = Path(args.step8_root) if str(args.step8_root or "").strip() else None
    roots = _roots(preferred)
    used: dict[str, str] = {}
    all_rows: list[dict] = []

    for sport, folder, fname in R.SPORTS:
        root, df = _first_nonempty(
            lambda rt, s=sport, fo=folder, fn=fname: R.load_sport(rt, date, s, fo, fn),
            roots,
        )
        if df.empty:
            print(f"{sport}: no step8")
            continue
        used[sport] = str(root)
        recs = R.recs(df)
        so, su, gob = l5eq5_buckets(recs, sport)
        all_rows.extend(_xlsx_row(r, "Standard OVER") for r in so)
        all_rows.extend(_xlsx_row(r, "Standard UNDER") for r in su)
        all_rows.extend(_xlsx_row(r, "Goblin OVER") for r in gob)
        print(f"{sport}: {len(so)} std O / {len(su)} std U / {len(gob)} gob  root={root}")

    for sport, loader in (("NFL", R.load_nfl), ("CFB", R.load_cfb)):
        root, df = _first_nonempty(lambda rt, fn=loader: fn(rt, date), roots)
        if df.empty:
            print(f"{sport}: no step8")
            continue
        used[sport] = str(root)
        recs = R.recs(df)
        so, su, gob = l5eq5_buckets(recs, sport)
        all_rows.extend(_xlsx_row(r, "Standard OVER") for r in so)
        all_rows.extend(_xlsx_row(r, "Standard UNDER") for r in su)
        all_rows.extend(_xlsx_row(r, "Goblin OVER") for r in gob)
        print(f"{sport}: {len(so)} std O / {len(su)} std U / {len(gob)} gob  root={root}")

    ranked = sorted(all_rows, key=R.l5_window_sort_key)
    gold = [r for r in ranked if _is_gold(r)]
    out_xlsx = Path(str(args.xlsx).strip()) if str(args.xlsx or "").strip() else (
        _REPO / "outputs" / date / "l5eq5_ranked_by_cover.xlsx"
    )
    out_xlsx.parent.mkdir(parents=True, exist_ok=True)
    sheets = {
        "All L5=5 by L5 dist": pd.DataFrame(ranked) if ranked else pd.DataFrame({"note": ["none"]}),
        "Gold+": pd.DataFrame(gold) if gold else pd.DataFrame({"note": ["none"]}),
    }
    for cat in ("Standard OVER", "Standard UNDER", "Goblin OVER"):
        sub = [r for r in ranked if r.get("Category") == cat]
        sheets[cat[:31]] = pd.DataFrame(sub) if sub else pd.DataFrame({"note": [f"{cat}: none"]})
    write_excel_sheets(out_xlsx, sheets)

    payload = {
        "date": date,
        "sort": "|L5_avg-line| then |L10| then |season|",
        "n": len(ranked),
        "n_gold": len(gold),
        "roots": used,
        "rows": ranked,
    }
    out_json = out_xlsx.with_suffix(".json")
    out_json.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    report = _REPO / "data" / "reports" / "l5eq5_ranked_by_cover.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps({k: payload[k] for k in ("date", "sort", "n", "n_gold", "roots")}, indent=2), encoding="utf-8")
    print("wrote", out_xlsx)
    print("wrote", out_json)
    print("n", len(ranked), "gold+", len(gold))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
