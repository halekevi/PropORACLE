#!/usr/bin/env python3
"""One cushion screen for every sport. It does not wire a floor.

Cushion orders legs that already cleared the gate (utils.cushion_z.rank_tiebreak).
A floor waits on R7: holdout of at least 200 legs, top bucket n>=30, the hit
rate rises across terciles, the top Wilson low clears that book's break-even,
and the top bucket beats the shuffle. Props near 100% in every bucket are a
line effect and stay out of the claim.

NFL and CFB read Current G1..G10, the current-season list written at step5.
The face window (Stat G*) may include last season and is not the cushion source.
Until those Current G columns are on the dated boards, the sport stays OPEN.

Snap regrade is the dnp_zero_minutes rows in data/reports/grade_corrections.jsonl.
A corrected attach is a wrong_match_attach row in that same log. Both apply
when this script is run with --force. This default pass does not refit.

  py -3.14 scripts/cushion_analysis.py
  py -3.14 scripts/cushion_analysis.py --force

Sports: MLB, WNBA, NFL, CFB, NHL, Soccer, Tennis, Golf, NBA, CBB, WCBB.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
OUT = _REPO / "data" / "reports"
SPORTS = (
    "MLB",
    "WNBA",
    "NFL",
    "CFB",
    "NHL",
    "Soccer",
    "Tennis",
    "Golf",
    "NBA",
    "CBB",
    "WCBB",
)
# Goblin 2-leg at 1.40x, Standard 2-leg at 3x. Per leg.
BREAK_EVEN = {
    "Goblin OVER": 0.845,
    "Standard OVER": 0.577,
    "Standard UNDER": 0.577,
}
GAPS = {
    "NFL": "dated step8 has no Current G1..G10 yet; face Stat G1 is not a current-season L10",
    "CFB": "dated step8 has no Current G1..G10 yet; last-5 average is not the L10 window",
    "NHL": "actual-vs-box agreement is not on file at 98% per prop; SOG stays separate from shots",
    "CBB": "off-season fit waits on a current-season list and actual-vs-box at 98% per prop",
    "WCBB": "no current board; same current-season and actual-vs-box bar as CBB",
    "NBA": "combos stay out; actual-vs-box at 98% per prop is not on file",
    "Golf": "no opponent; holdout is under 200 legs",
}


def summary_from_discovery(sport: str) -> dict[str, str] | None:
    path = OUT / "cushion_z_discovery.json"
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    info = (payload.get("source") or {}).get("by_sport", {}).get(sport) or {}
    cells = [
        c
        for c in payload.get("cells") or []
        if c.get("sport") == sport and c.get("feature") == "cushion_z" and not c.get("ceiling")
    ]
    holdout_n = int(info.get("holdout_legs") or 0)
    cut = info.get("cut") or ""
    if not cells and holdout_n == 0:
        return None
    cells.sort(key=lambda c: (not c["buckets"][2]["thin"], c.get("holdout_n") or 0), reverse=True)
    top = cells[0] if cells else None
    high = (top or {}).get("buckets", [{}, {}, {}])[2] if top else {}
    status = "OPEN"
    if holdout_n >= 200 and top and not top.get("ceiling"):
        high_n = int(high.get("n") or 0)
        wilson = high.get("wilson_lo")
        be = float(top.get("breakeven_2leg") or 1)
        if (
            top.get("rising")
            and top.get("beats_shuffle") is True
            and high_n >= 30
            and wilson is not None
            and wilson > be
            and high_n >= 150
        ):
            status = "PASS"
        elif top.get("rising") or high_n >= 30:
            status = "FAIL"
    detail = GAPS.get(sport, "")
    if holdout_n:
        window = ""
        dates = info.get("holdout_dates") or []
        if dates:
            window = f" from {dates[0]} through {dates[-1]}"
        detail = f"holdout {holdout_n} legs{window}, cut {cut}"
        if top:
            detail += (
                f"; {top['prop']} {top['book']} high {high.get('hits')}/{high.get('n')}"
                f" ({high.get('rate')}) {top.get('blocked_reason')}"
            )
    elif detail == "" :
        detail = "no eligible legs in the archive or the templates board"
    return {
        "sport": sport,
        "id": f"cushion_{sport.lower()}",
        "status": status,
        "top_bucket": "" if not high else str(high.get("rate") or ""),
        "n": "" if not high else str(high.get("n") or ""),
        "wilson_lo": "" if not high else str(high.get("wilson_lo") or ""),
        "wilson_hi": "" if not high else str(high.get("wilson_hi") or ""),
        "break_even": "Goblin 0.845 / Standard 0.577",
        "wired": "0",
        "detail": detail,
    }


def summary_row(sport: str) -> dict[str, str]:
    found = summary_from_discovery(sport)
    if found:
        return found
    gap = GAPS.get(sport, "holdout is under 200 graded legs; rerun then, not every session")
    return {
        "sport": sport,
        "id": f"cushion_{sport.lower()}",
        "status": "OPEN",
        "top_bucket": "",
        "n": "",
        "wilson_lo": "",
        "wilson_hi": "",
        "break_even": "Goblin 0.845 / Standard 0.577",
        "wired": "0",
        "detail": gap,
    }


def write_sport(sport: str) -> Path:
    path = OUT / f"cushion_{sport.lower()}.csv"
    row = summary_row(sport)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(row))
        writer.writeheader()
        writer.writerow(row)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Shared cushion screen. Does not wire a floor.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Refit terciles. Default writes OPEN until a sport holdout reaches 200 legs.",
    )
    args = parser.parse_args()
    if args.force:
        import cushion_z_discovery as fit

        fit.main()
        return
    OUT.mkdir(parents=True, exist_ok=True)
    for sport in SPORTS:
        path = write_sport(sport)
        row = summary_row(sport)
        print(
            f"{row['id']} | {row['status']} | {row['detail']} | {row['break_even']}",
            flush=True,
        )
        print(f"  {path}", flush=True)


if __name__ == "__main__":
    main()
