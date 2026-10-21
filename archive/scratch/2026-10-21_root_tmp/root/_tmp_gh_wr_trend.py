"""Full-range grade_history WR trend — is May→June a one-off or a pattern?"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:/PropORACLE")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

MAIN_PREF = {
    "graded_main": 0,
    "main": 1,
    "combined": 2,
    "mixer": 3,
    "default": 4,
    "": 5,
}
SPECIAL = ("long", "high_leg", "strong", "goblin", "yolo", "winrate", "opt3", "recombo", "uniform")


def track_score(track: str) -> int:
    t = (track or "main").lower()
    score = MAIN_PREF.get(t, 50)
    if any(x in t for x in SPECIAL):
        score += 100
    return score


def main() -> None:
    from utils.proporacle_data_root import load_best_grade_history_runs

    runs = load_best_grade_history_runs(ROOT, templates_dir=ROOT / "ui_runner" / "templates") or []
    print(f"raw runs: {len(runs)}")

    by_date: dict[str, tuple[int, dict, str]] = {}
    tracks: dict[str, int] = defaultdict(int)
    for r in runs:
        if not isinstance(r, dict):
            continue
        d = str(r.get("date") or "")[:10]
        if len(d) < 10:
            continue
        track = str(r.get("track") or "main").lower()
        tracks[track] += 1
        score = track_score(track)
        prev = by_date.get(d)
        if prev is None or score < prev[0]:
            by_date[d] = (score, r, track)

    print(f"unique dates: {len(by_date)}")
    print("tracks (top):", dict(sorted(tracks.items(), key=lambda x: -x[1])[:15]))

    rows: list[tuple[str, int, int, int, float | None, str]] = []
    for d in sorted(by_date):
        _, r, track = by_date[d]
        n = int(r.get("n_tickets") or 0)
        w = int(r.get("wins") or 0)
        g = int(r.get("guarantees") or 0)
        wr = r.get("win_rate")
        if wr is None and n:
            wr = (w + g) / n
        else:
            try:
                wr = float(wr) if wr is not None else None
            except (TypeError, ValueError):
                wr = None
            if wr is not None and wr > 1.5:  # stored as percent
                wr = wr / 100.0
        rows.append((d, n, w, g, wr, track))

    # ── Monthly aggregates (ticket-weighted) ──
    month: dict[str, dict[str, float]] = defaultdict(lambda: {"n": 0, "w": 0, "g": 0, "days": 0})
    for d, n, w, g, wr, track in rows:
        m = d[:7]
        month[m]["n"] += n
        month[m]["w"] += w
        month[m]["g"] += g
        month[m]["days"] += 1

    print("\n=== WR by month (ticket-weighted, wins+guarantees) ===")
    print(f"{'month':8} {'days':>4} {'n':>6} {'wins':>5} {'guar':>4} {'WR%':>6}  Δpp vs prior")
    prev_wr = None
    month_wrs: list[tuple[str, float, int]] = []
    for m in sorted(month):
        s = month[m]
        n = int(s["n"])
        if n <= 0:
            continue
        wr = (s["w"] + s["g"]) / n
        month_wrs.append((m, wr, n))
        dpp = (wr - prev_wr) * 100 if prev_wr is not None else None
        dpp_s = f"{dpp:+.1f}" if dpp is not None else "  —"
        print(
            f"{m:8} {int(s['days']):4d} {n:6d} {int(s['w']):5d} {int(s['g']):4d} "
            f"{100*wr:6.1f}  {dpp_s}"
        )
        prev_wr = wr

    # ── Month-to-month transitions ranked by drop ──
    print("\n=== Month→month WR drops (largest first) ===")
    drops = []
    for i in range(1, len(month_wrs)):
        m0, wr0, n0 = month_wrs[i - 1]
        m1, wr1, n1 = month_wrs[i]
        drops.append((wr1 - wr0, m0, wr0, n0, m1, wr1, n1))
    for dpp, m0, wr0, n0, m1, wr1, n1 in sorted(drops, key=lambda x: x[0]):
        print(
            f"  {m0}→{m1}: {100*wr0:.1f}% → {100*wr1:.1f}%  ({100*dpp:+.1f} pp)  "
            f"n={n0}→{n1}"
        )

    # ── Rolling 30-day style: weekly buckets ──
    print("\n=== WR by ISO week (ticket-weighted) ===")
    week: dict[str, dict[str, float]] = defaultdict(lambda: {"n": 0, "w": 0, "g": 0, "days": 0})
    from datetime import date as _date

    for d, n, w, g, wr, track in rows:
        dt = _date.fromisoformat(d)
        iso = dt.isocalendar()
        key = f"{iso.year}-W{iso.week:02d}"
        week[key]["n"] += n
        week[key]["w"] += w
        week[key]["g"] += g
        week[key]["days"] += 1
        week[key].setdefault("_first", d)
        week[key]["_last"] = d

    print(f"{'week':10} {'span':23} {'days':>4} {'n':>5} {'WR%':>6}")
    for k in sorted(week):
        s = week[k]
        n = int(s["n"])
        if n <= 0:
            continue
        wr = (s["w"] + s["g"]) / n
        span = f"{s.get('_first','?')}→{s.get('_last','?')}"
        print(f"{k:10} {span:23} {int(s['days']):4d} {n:5d} {100*wr:6.1f}")

    # ── Flag similar dips: month transitions with ≤ −5 pp and both months n≥200 ──
    print("\n=== Material month dips (≤ −5 pp, both months n≥200) ===")
    material = [x for x in drops if x[0] <= -0.05 and x[3] >= 200 and x[6] >= 200]
    if not material:
        print("  (none)")
    else:
        for dpp, m0, wr0, n0, m1, wr1, n1 in material:
            print(f"  {m0}→{m1}: {100*wr0:.1f}% → {100*wr1:.1f}% ({100*dpp:+.1f} pp)")

    # ── Recent tail: last 45 days with data ──
    print("\n=== Last 45 dated days (daily WR) ===")
    tail = rows[-45:]
    print(f"{'date':10} {'n':>5} {'WR%':>6}")
    for d, n, w, g, wr, track in tail:
        wr_s = f"{100*wr:6.1f}" if wr is not None and n else "   n/a"
        print(f"{d:10} {n:5d} {wr_s}")

    # overall
    tot_n = sum(n for _, n, _, _, _, _ in rows)
    tot_wg = sum(w + g for _, _, w, g, _, _ in rows)
    print(f"\nOverall: {tot_n} tickets, WR={100*tot_wg/tot_n:.1f}%" if tot_n else "Overall: empty")
    if rows:
        print(f"Range: {rows[0][0]} → {rows[-1][0]}")


if __name__ == "__main__":
    main()
