#!/usr/bin/env python3
"""Backfill graded_* + graded_props JSON for a date range using on-disk actuals.

Skips fetch. Grades NBA (slate_grader), NHL/Soccer (advanced), CFB (slate_grader),
Tennis (tennis_grader), then rebuilds graded_props JSON + sanity alerts.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PY = sys.executable


def _dates(start: str, end: str) -> list[str]:
    s, e = date.fromisoformat(start), date.fromisoformat(end)
    out = []
    cur = s
    while cur <= e:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _run(cmd: list[str], label: str, timeout: int = 900) -> bool:
    print(f"  [{label}] {' '.join(str(c) for c in cmd[:8])}...")
    try:
        env = dict(**__import__("os").environ)
        env["PYTHONIOENCODING"] = "utf-8"
        proc = subprocess.run(
            cmd,
            cwd=str(REPO),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired:
        print(f"  [{label}] TIMEOUT")
        return False
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "")[-800:]
        print(f"  [{label}] FAIL exit={proc.returncode}\n{tail}")
        return False
    print(f"  [{label}] OK")
    return True


def _first_existing(paths: list[Path]) -> Path | None:
    for p in paths:
        if p.is_file() and p.stat().st_size > 100:
            return p
    return None


def grade_day(d: str) -> dict[str, bool]:
    out = REPO / "outputs" / d
    out.mkdir(parents=True, exist_ok=True)
    results: dict[str, bool] = {}

    # NBA
    nba_act = out / f"actuals_nba_{d}.csv"
    nba_slate = _first_existing(
        [
            out / f"step8_nba_direction_clean_{d}.xlsx",
            out / "nba" / "step8_nba_direction_clean.xlsx",
            out / "nba" / "step8_all_direction_clean.xlsx",
        ]
    )
    if nba_act.is_file() and nba_slate:
        results["NBA"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "grading" / "slate_grader.py"),
                "--sport",
                "NBA",
                "--slate",
                str(nba_slate),
                "--actuals",
                str(nba_act),
                "--output",
                str(out / f"graded_nba_{d}.xlsx"),
                "--date",
                d,
            ],
            f"NBA {d}",
            timeout=1200,
        )
    else:
        results["NBA"] = False
        print(f"  [NBA {d}] skip (actuals={nba_act.is_file()} slate={bool(nba_slate)})")

    # CFB
    cfb_act = out / f"actuals_cfb_{d}.csv"
    cfb_slate = _first_existing(
        [
            out / f"step8_cfb_direction_clean_{d}.xlsx",
            out / "cfb" / "step8_cfb_direction_clean.xlsx",
            REPO / "Sports" / "CFB" / "step8_cfb_direction_clean.xlsx",
        ]
    )
    if cfb_act.is_file() and cfb_act.stat().st_size > 100 and cfb_slate:
        results["CFB"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "grading" / "slate_grader.py"),
                "--sport",
                "NFL",  # CFB uses same loader path via sport label in run_grader as CFB
                "--slate",
                str(cfb_slate),
                "--actuals",
                str(cfb_act),
                "--output",
                str(out / f"graded_cfb_{d}.xlsx"),
                "--date",
                d,
            ],
            f"CFB {d}",
        )
    else:
        # Try CFB sport code if supported
        if cfb_act.is_file() and cfb_slate:
            results["CFB"] = _run(
                [
                    PY,
                    "-X",
                    "utf8",
                    str(REPO / "scripts" / "grading" / "slate_grader.py"),
                    "--sport",
                    "CFB",
                    "--slate",
                    str(cfb_slate),
                    "--actuals",
                    str(cfb_act),
                    "--output",
                    str(out / f"graded_cfb_{d}.xlsx"),
                    "--date",
                    d,
                ],
                f"CFB {d}",
            )
        else:
            results["CFB"] = False
            print(f"  [CFB {d}] skip")

    # NHL
    nhl_act = out / f"actuals_nhl_{d}.csv"
    nhl_slate = _first_existing(
        [
            out / f"step8_nhl_direction_clean_{d}.xlsx",
            out / "nhl" / "step8_nhl_direction_clean.xlsx",
            REPO / "Sports" / "NHL" / "step8_nhl_direction_clean.xlsx",
        ]
    )
    if nhl_act.is_file() and nhl_slate:
        results["NHL"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "nhl_grader_advanced.py"),
                "--date",
                d,
                "--slate",
                str(nhl_slate),
                "--actuals",
                str(nhl_act),
                "--output-dir",
                str(out),
            ],
            f"NHL {d}",
        )
    else:
        results["NHL"] = False
        print(f"  [NHL {d}] skip")

    # Soccer
    soc_act = out / f"actuals_soccer_{d}.csv"
    soc_slate = _first_existing(
        [
            out / f"step8_soccer_direction_clean_{d}.xlsx",
            out / "soccer" / "step8_soccer_direction_clean.xlsx",
            REPO / "Sports" / "Soccer" / "step8_soccer_direction_clean.xlsx",
        ]
    )
    if soc_act.is_file() and soc_slate:
        results["Soccer"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "soccer_grader_advanced.py"),
                "--date",
                d,
                "--slate",
                str(soc_slate),
                "--actuals",
                str(soc_act),
                "--output-dir",
                str(out),
            ],
            f"Soccer {d}",
        )
    else:
        results["Soccer"] = False
        print(f"  [Soccer {d}] skip")

    # WNBA
    wnba_act = out / f"actuals_wnba_{d}.csv"
    wnba_slate = _first_existing(
        [
            out / f"step8_wnba_direction_clean_{d}.xlsx",
            out / "wnba" / "step8_wnba_direction_clean.xlsx",
            REPO / "Sports" / "WNBA" / "step8_wnba_direction_clean.xlsx",
        ]
    )
    if wnba_act.is_file() and wnba_slate:
        results["WNBA"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "grading" / "slate_grader.py"),
                "--sport",
                "WNBA",
                "--slate",
                str(wnba_slate),
                "--actuals",
                str(wnba_act),
                "--output",
                str(out / f"graded_wnba_{d}.xlsx"),
                "--date",
                d,
            ],
            f"WNBA {d}",
        )
    else:
        results["WNBA"] = False
        print(f"  [WNBA {d}] skip")

    # NFL
    nfl_act = out / f"actuals_nfl_{d}.csv"
    nfl_slate = _first_existing(
        [
            out / f"step8_nfl_direction_clean_{d}.xlsx",
            out / "nfl" / "step8_nfl_direction_clean.xlsx",
            REPO / "Sports" / "NFL" / "step8_nfl_direction_clean.xlsx",
        ]
    )
    if nfl_act.is_file() and nfl_slate:
        results["NFL"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "grading" / "slate_grader.py"),
                "--sport",
                "NFL",
                "--slate",
                str(nfl_slate),
                "--actuals",
                str(nfl_act),
                "--output",
                str(out / f"graded_nfl_{d}.xlsx"),
                "--date",
                d,
            ],
            f"NFL {d}",
        )
    else:
        results["NFL"] = False
        print(f"  [NFL {d}] skip")

    # MLB (ticket-face union lives in nhl_soccer_grader)
    mlb_act = out / f"actuals_mlb_{d}.csv"
    mlb_slate = _first_existing(
        [
            out / f"step8_mlb_direction_clean_{d}.xlsx",
            out / "mlb" / "step8_mlb_direction_clean.xlsx",
            REPO / "Sports" / "MLB" / "step8_mlb_direction_clean.xlsx",
        ]
    )
    if mlb_act.is_file() and mlb_slate:
        results["MLB"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "scripts" / "nhl_soccer_grader.py"),
                "--sport",
                "MLB",
                "--date",
                d,
                "--slate",
                str(mlb_slate),
                "--actuals",
                str(mlb_act),
                "--output-dir",
                str(out),
            ],
            f"MLB {d}",
        )
    else:
        results["MLB"] = False
        print(f"  [MLB {d}] skip")

    # Tennis
    d1 = (date.fromisoformat(d) - timedelta(days=1)).isoformat()
    ten_slate = _first_existing(
        [
            out / f"step8_tennis_direction_clean_{d}.xlsx",
            out / "tennis" / f"step8_tennis_direction_clean_{d}.xlsx",
            out / "tennis" / "step8_tennis_direction_clean.xlsx",
            REPO / "outputs" / d1 / "tennis" / f"step8_tennis_direction_clean_{d}.xlsx",
            REPO / "outputs" / d1 / f"step8_tennis_direction_clean_{d}.xlsx",
        ]
    )
    ten_out = out / f"graded_tennis_{d}.xlsx"
    if ten_slate:
        results["Tennis"] = _run(
            [
                PY,
                "-X",
                "utf8",
                str(REPO / "Sports" / "Tennis" / "scripts" / "tennis_grader.py"),
                "--date",
                d,
                "--slate",
                str(ten_slate),
                "--output",
                str(ten_out),
                "--days-back",
                "2",
                "--days-forward",
                "1",
            ],
            f"Tennis {d}",
        )
    else:
        results["Tennis"] = False
        print(f"  [Tennis {d}] skip")

    # Rebuild graded_props JSON
    bf = REPO / "scripts" / "backfill_graded_props_json.py"
    if bf.is_file():
        _run(
            [PY, "-X", "utf8", str(bf), "--date", d],
            f"JSON {d}",
        )

    # Sanity alerts (log only during backfill — still notify)
    san = REPO / "scripts" / "grader_sanity_alerts.py"
    if san.is_file():
        _run(
            [PY, "-X", "utf8", str(san), "--date", d, "--root", str(REPO)],
            f"sanity {d}",
        )

    return results


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-01")
    ap.add_argument("--end", default="2026-10-22")
    ap.add_argument("--dates", nargs="*", default=None)
    args = ap.parse_args()
    days = args.dates if args.dates else _dates(args.start, args.end)
    print(f"Backfill grader fix: {len(days)} day(s) in {REPO}")
    summary = []
    for d in days:
        print(f"\n=== {d} ===")
        summary.append((d, grade_day(d)))
    print("\n=== SUMMARY ===")
    for d, res in summary:
        ok = ",".join(k for k, v in res.items() if v) or "(none)"
        print(f"  {d}: {ok}")
    return 0


if __name__ == "__main__":
    # Fix tennis launcher: use same interpreter
    raise SystemExit(main())
