"""Smoke-test ticket_eval HTML loader vs grade_history for a few days, then full backtest."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from scripts.build_leg_correlation_matrix import (  # noqa: E402
    backtest,
    build_lookup,
    load_graded_props,
    load_tickets_from_ticket_eval_html,
)
from utils.proporacle_data_root import load_best_grade_history_runs  # noqa: E402


def main() -> None:
    print("=== load graded props (prob join) ===")
    df = load_graded_props(str(ROOT / "ui_runner/templates"))
    print(f"graded rows={len(df)}")

    print("\n=== parse May–June ticket_eval HTML ===")
    tickets = load_tickets_from_ticket_eval_html(
        str(ROOT / "ui_runner/templates/ticket_eval_2026-0[56]-*.html"),
        graded_df=df,
    )
    print(f"normalized tickets={len(tickets)}")

    by = defaultdict(lambda: {"n": 0, "w": 0})
    for t in tickets:
        d = t["date"]
        by[d]["n"] += 1
        if t.get("ticket_hit"):
            by[d]["w"] += 1

    gh = {}
    for r in load_best_grade_history_runs(ROOT, templates_dir=ROOT / "ui_runner" / "templates") or []:
        d = str(r.get("date") or "")[:10]
        if d.startswith("2026-05") or d.startswith("2026-06"):
            track = str(r.get("track") or "main").lower()
            if track in ("main", "", "default", "combined", "graded_main", "graded-main", "mixer") or d not in gh:
                gh[d] = r

    print(f"\n{'date':10} {'te_n':>5} {'te_w':>5} {'gh_n':>5} {'gh_w':>5} {'dn':>5} {'dw':>5}")
    diffs = []
    for d in sorted(by):
        g = gh.get(d) or {}
        ghn = g.get("n_tickets")
        ghw = g.get("wins")
        n, w = by[d]["n"], by[d]["w"]
        dn = (n - int(ghn)) if ghn is not None else None
        dw = (w - int(ghw)) if ghw is not None else None
        print(f"{d:10} {n:5d} {w:5d} {str(ghn):>5} {str(ghw):>5} {str(dn):>5} {str(dw):>5}")
        if ghn is not None:
            diffs.append(abs(n - int(ghn)))
    if diffs:
        print(f"mean_|te_n-gh_n|={sum(diffs)/len(diffs):.2f}  overlap={len(diffs)}")

    # month WR check
    for month in ("2026-05", "2026-06"):
        sub = [t for t in tickets if (t.get("date") or "").startswith(month)]
        if not sub:
            continue
        wr = sum(1 for t in sub if t.get("ticket_hit")) / len(sub)
        print(f"{month}: n={len(sub)} WR={100*wr:.1f}%")

    print("\n=== backtest (reuse cached matrix) ===")
    cells = json.loads((ROOT / "data/cache/leg_correlation_matrix.json").read_text(encoding="utf-8"))
    lookup = build_lookup(cells)
    out = ROOT / "data/reports/corr_backtest_may_june_ticket_eval.md"
    backtest(
        tickets,
        lookup,
        str(out),
        population_note=(
            "**Population note:** `ticket_eval_YYYY-MM-DD.html` decided articles "
            "(grade_history-aligned May–June source)."
        ),
    )
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
